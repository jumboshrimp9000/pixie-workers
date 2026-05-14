<#
.SYNOPSIS
    Repairs restored replacement-domain Azure mailboxes after an old-domain cleanup.
.DESCRIPTION
    One-off repair script for the ProfitPath replacement batch. It only targets
    active Simple Inboxes rows on the protected new domain, resolves the restored
    Entra user by the new-domain alias, then repairs the matching Exchange
    mailbox by ExternalDirectoryObjectId.
#>

param(
    [Parameter(Mandatory=$true)][string]$OldDomain,
    [Parameter(Mandatory=$true)][string]$NewDomain,
    [switch]$Live,
    [string]$ConfirmText = "",
    [int]$ExpectedUserCount = 99,
    [string[]]$Emails = @(),
    [int]$Limit = 0,
    [switch]$QueueReuploadWhenReady,
    [string]$BatchId = "ffa54dd8-3fd1-4336-8e06-6f73872432f4",
    [string]$LogDir = (Join-Path $PSScriptRoot "logs")
)

. (Join-Path $PSScriptRoot "config.ps1")

$script:Summary = [ordered]@{
    oldDomain = $null
    newDomain = $null
    mode = if ($Live) { "live" } else { "dry_run" }
    tenantId = $null
    adminEmail = $null
    activeInboxRows = 0
    targetInboxRows = 0
    graphUsersWithNewAddress = 0
    exchangeMailboxes = 0
    softDeletedExchangeMailboxes = 0
    matched = 0
    matchedSoftDeleted = 0
    missingGraphUser = 0
    missingMailbox = 0
    alreadyPrimary = 0
    wouldRepairPrimary = 0
    wouldUndoSoftDeleted = 0
    softDeletedRestored = 0
    restorePropagationPending = 0
    primaryRepaired = 0
    oldAliasesRemoved = 0
    graphUsersPatched = 0
    finalActiveMailboxCount = 0
    finalOldAliasCount = 0
    finalGraphUpnCount = 0
    reuploadQueued = $false
    reuploadActionId = $null
    errors = @()
    samples = @()
}

function Normalize-Domain {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant().Replace("https://", "").Replace("http://", "").TrimEnd("/")
}

function Add-RepairError {
    param([string]$Message)
    $script:Summary.errors += $Message
    Write-Log $Message -Level Error
}

function Save-RepairLog {
    if (-not (Test-Path $LogDir)) {
        New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
    }
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
    $mode = if ($Live) { "live" } else { "dryrun" }
    $path = Join-Path $LogDir "profitpath-restored-mailbox-repair-$mode-$($script:Summary.newDomain)-$stamp.json"
    $script:Summary | ConvertTo-Json -Depth 20 | Set-Content -Path $path -Encoding UTF8
    Write-Log "Saved repair log: $path" -Level Info
}

function Get-DomainByName {
    param([string]$DomainName)
    $encoded = [uri]::EscapeDataString($DomainName)
    $query = "domain=eq.$encoded&order=created_at.desc&limit=1&select=*"
    $result = Invoke-SupabaseApi -Method GET -Table "domains" -Query $query
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
}

function Get-ActiveDomainInboxes {
    param([string]$DomainId)
    $result = Invoke-SupabaseApi -Method GET -Table "inboxes" -Query "domain_id=eq.$DomainId&status=eq.active&order=created_at.asc&select=*"
    if ($result.Success) { return @($result.Data) }
    return @()
}

function Get-TenantIdFromDomain {
    param([string]$Domain)
    try {
        $response = Invoke-RestMethod -Uri "https://login.microsoftonline.com/$Domain/v2.0/.well-known/openid-configuration" -TimeoutSec 30 -ErrorAction Stop
        if ($response.token_endpoint) {
            $parts = $response.token_endpoint -split '/'
            if ($parts.Length -ge 4 -and $parts[3] -and $parts[3].ToLower() -ne "organizations") { return $parts[3] }
        }
    } catch { }
    return $null
}

function Get-ROPCToken {
    param(
        [string]$TenantId,
        [string]$ClientId,
        [string]$Username,
        [string]$Password,
        [string]$ScopeString = "https://graph.microsoft.com/.default"
    )
    $body = @{
        grant_type = "password"
        client_id = $ClientId
        scope = $ScopeString
        username = $Username
        password = $Password
    }
    $response = Invoke-RestMethod -Method POST -Uri "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token" -Body $body -ContentType "application/x-www-form-urlencoded" -TimeoutSec 30 -ErrorAction Stop
    return $response.access_token
}

function Invoke-GraphRequest {
    param(
        [string]$Method,
        [string]$Url,
        [string]$Bearer,
        [object]$Body = $null,
        [switch]$AllowNotFound
    )
    $params = @{
        Method = $Method
        Uri = $Url
        Headers = @{ Authorization = "Bearer $Bearer" }
        ContentType = "application/json"
        TimeoutSec = 90
        ErrorAction = "Stop"
    }
    if ($Body) {
        $params.Body = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 15 }
    }
    try {
        return Invoke-RestMethod @params
    } catch {
        if ($AllowNotFound -and ($_.Exception.Message -match "404" -or $_.Exception.Message -match "ResourceNotFound" -or $_.Exception.Message -match "Request_ResourceNotFound")) {
            return $null
        }
        $message = $_.Exception.Message
        if ($_.ErrorDetails.Message) { $message = "$message :: $($_.ErrorDetails.Message)" }
        throw $message
    }
}

function Get-AllGraphItems {
    param([string]$Url, [string]$Bearer)
    $items = New-Object System.Collections.Generic.List[object]
    while ($Url) {
        $response = Invoke-GraphRequest -Method GET -Url $Url -Bearer $Bearer
        foreach ($item in @($response.value)) {
            if ($item) { $items.Add($item) | Out-Null }
        }
        $Url = [string]$response.'@odata.nextLink'
    }
    return $items.ToArray()
}

function Get-EmailAddressValue {
    param([string]$Address)
    $value = ([string]$Address).Trim()
    if ($value -match '^[^:]+:(.+)$') { $value = $matches[1] }
    return $value.Trim().Trim('"').ToLowerInvariant()
}

function Get-EmailAddressDomain {
    param([string]$Address)
    $value = Get-EmailAddressValue -Address $Address
    $at = $value.LastIndexOf("@")
    if ($at -lt 0 -or $at -eq ($value.Length - 1)) { return "" }
    return $value.Substring($at + 1)
}

function Test-ObjectHasEmail {
    param([object]$Object, [string]$Email)
    $target = ([string]$Email).Trim().ToLowerInvariant()
    if (-not $Object -or -not $target) { return $false }
    foreach ($candidate in @($Object.userPrincipalName, $Object.mail)) {
        if (([string]$candidate).Trim().ToLowerInvariant() -eq $target) { return $true }
    }
    foreach ($address in @($Object.proxyAddresses)) {
        if ((Get-EmailAddressValue -Address ([string]$address)) -eq $target) { return $true }
    }
    foreach ($address in @($Object.EmailAddresses)) {
        if ((Get-EmailAddressValue -Address ([string]$address)) -eq $target) { return $true }
    }
    $primary = [string]$Object.PrimarySmtpAddress
    if ($primary -and (Get-EmailAddressValue -Address $primary) -eq $target) { return $true }
    return $false
}

function Get-AddressesForDomain {
    param([object]$Object, [string]$Domain)
    $domainName = Normalize-Domain $Domain
    if (-not $Object -or -not $domainName) { return @() }
    $matches = @()
    foreach ($collection in @($Object.EmailAddresses, $Object.proxyAddresses)) {
        foreach ($address in @($collection)) {
            if (-not $address) { continue }
            $raw = [string]$address
            if ((Get-EmailAddressDomain -Address $raw) -eq $domainName) { $matches += $raw }
        }
    }
    return @($matches | Select-Object -Unique)
}

function Add-Sample {
    param([object]$Row)
    if (@($script:Summary.samples).Count -lt 12) {
        $script:Summary.samples += $Row
    }
}

function Get-SoftDeletedRestoreIdentity {
    param([object]$Mailbox)
    if (-not $Mailbox) { return "" }
    foreach ($propertyName in @("ExchangeGuid", "Guid", "PrimarySmtpAddress", "WindowsLiveID", "UserPrincipalName", "ExternalDirectoryObjectId", "Identity")) {
        $property = $Mailbox.PSObject.Properties[$propertyName]
        if ($property -and $property.Value) {
            $value = ([string]$property.Value).Trim()
            if ($value) { return $value }
        }
    }
    return ""
}

function Get-SendingToolSettingsPayload {
    return @{
        applyRequested = $true
        enableWarmup = $true
        dailyLimit = 5
        sendingGap = 30
        tag = "Mailboxpro 5/10"
        tags = @("Mailboxpro 5/10")
        instantlyWarmup = @{
            limit = 5
            reply_rate = 60
        }
    }
}

function Get-OpenReuploadAction {
    param([string]$DomainId)
    $result = Invoke-SupabaseApi -Method GET -Table "actions" -Query "domain_id=eq.$DomainId&type=eq.reupload_inboxes&status=in.(pending,in_progress)&order=created_at.desc&limit=1&select=*"
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
}

function New-RestoreReuploadAction {
    param(
        [object]$DomainRecord,
        [int]$ExpectedActiveInboxCount
    )

    $existing = Get-OpenReuploadAction -DomainId ([string]$DomainRecord.id)
    if ($existing) { return @{ Created = $false; Action = $existing } }

    $body = @{
        customer_id = $DomainRecord.customer_id
        domain_id = $DomainRecord.id
        type = "reupload_inboxes"
        status = "pending"
        attempts = 0
        max_attempts = 8
        payload = @{
            domain = $DomainRecord.domain
            source = "profitpath_restore"
            expected_active_inboxes = $ExpectedActiveInboxCount
            sending_tool_settings = Get-SendingToolSettingsPayload
        }
    }

    $result = Invoke-SupabaseApi -Method POST -Table "actions" -Body $body
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) {
        return @{ Created = $true; Action = $result.Data[0] }
    }
    throw "Failed to enqueue reupload_inboxes action for $($DomainRecord.domain): $($result.Error)"
}

try {
    $old = Normalize-Domain $OldDomain
    $new = Normalize-Domain $NewDomain
    $script:Summary.oldDomain = $old
    $script:Summary.newDomain = $new

    if (-not $old -or -not $new -or $old -eq $new) {
        throw "OldDomain and NewDomain must be non-empty and different."
    }

    $expectedConfirm = "REPAIR RESTORED MAILBOXES $new FROM OLD $old"
    if ($Live -and $ConfirmText -ne $expectedConfirm) {
        throw "Live repair requires ConfirmText exactly: $expectedConfirm"
    }

    $newDomainRecord = Get-DomainByName -DomainName $new
    if (-not $newDomainRecord) { throw "New Simple Inboxes domain not found: $new" }
    if ($BatchId -and ([string]$newDomainRecord.order_batch_id) -ne $BatchId) {
        throw "New domain $new belongs to batch $($newDomainRecord.order_batch_id), expected $BatchId"
    }

    $replacementOld = Normalize-Domain ([string]$newDomainRecord.fulfillment_settings.replacement_old_domain)
    if ($replacementOld -ne $old) {
        throw "Replacement mapping mismatch for $new`: expected old $old, found $replacementOld"
    }

    $activeInboxRows = @(Get-ActiveDomainInboxes -DomainId ([string]$newDomainRecord.id))
    $script:Summary.activeInboxRows = $activeInboxRows.Count
    if ($activeInboxRows.Count -lt $ExpectedUserCount) {
        throw "Simple Inboxes has $($activeInboxRows.Count) active inbox rows for $new, expected at least $ExpectedUserCount."
    }

    $adminRecord = Get-AssignedAdmin -DomainId ([string]$newDomainRecord.id)
    if (-not $adminRecord -or -not $adminRecord.email -or -not $adminRecord.password) {
        throw "No assigned Microsoft admin credentials found for $new"
    }
    $adminEmail = ([string]$adminRecord.email).Trim().ToLowerInvariant()
    $script:Summary.adminEmail = $adminEmail
    $tenantId = Get-TenantIdFromDomain -Domain (($adminEmail -split "@")[1])
    if (-not $tenantId) { throw "Could not resolve tenant id for admin $adminEmail" }
    $script:Summary.tenantId = $tenantId
    $bearer = Get-ROPCToken -TenantId $tenantId -ClientId $AzureCliPublicClientId -Username $adminEmail -Password ([string]$adminRecord.password)

    $graphUsers = @(Get-AllGraphItems -Url "https://graph.microsoft.com/v1.0/users?`$select=id,userPrincipalName,mail,displayName,proxyAddresses,accountEnabled&`$top=999" -Bearer $bearer | Where-Object {
        Test-ObjectHasEmail -Object $_ -Email "$(([string]$_.userPrincipalName).Trim().ToLowerInvariant())" -or
        (@($_.proxyAddresses) -join " ").ToLowerInvariant().Contains("@$new")
    })
    $graphUsersWithNew = @($graphUsers | Where-Object { @(Get-AddressesForDomain -Object $_ -Domain $new).Count -gt 0 -or ([string]$_.userPrincipalName).ToLowerInvariant().EndsWith("@$new") -or ([string]$_.mail).ToLowerInvariant().EndsWith("@$new") })
    $script:Summary.graphUsersWithNewAddress = $graphUsersWithNew.Count

    if ($graphUsersWithNew.Count -lt $ExpectedUserCount) {
        throw "Only $($graphUsersWithNew.Count) active Graph users reference $new; expected $ExpectedUserCount."
    }

    if (-not (Get-Module -ListAvailable -Name ExchangeOnlineManagement)) {
        Write-Log "Installing ExchangeOnlineManagement module..." -Level Warning
        Install-Module -Name ExchangeOnlineManagement -Force -AllowClobber -Scope CurrentUser
    }
    Import-Module ExchangeOnlineManagement -ErrorAction Stop
    $securePwd = ConvertTo-SecureString ([string]$adminRecord.password) -AsPlainText -Force
    $creds = New-Object System.Management.Automation.PSCredential($adminEmail, $securePwd)
    Connect-ExchangeOnline -Credential $creds -ShowBanner:$false -ErrorAction Stop

    $mailboxes = @(Get-Mailbox -ResultSize Unlimited -ErrorAction Stop)
    $script:Summary.exchangeMailboxes = $mailboxes.Count
    $softDeletedMailboxes = @(Get-Mailbox -SoftDeletedMailbox -ResultSize Unlimited -ErrorAction SilentlyContinue)
    $script:Summary.softDeletedExchangeMailboxes = $softDeletedMailboxes.Count
    $mailboxByExternalId = @{}
    foreach ($mailbox in $mailboxes) {
        if ($mailbox.ExternalDirectoryObjectId) {
            $mailboxByExternalId[[string]$mailbox.ExternalDirectoryObjectId] = $mailbox
        }
    }
    $softDeletedByExternalId = @{}
    foreach ($mailbox in $softDeletedMailboxes) {
        if ($mailbox.ExternalDirectoryObjectId) {
            $softDeletedByExternalId[[string]$mailbox.ExternalDirectoryObjectId] = $mailbox
        }
    }

    $targetEmails = @($Emails | ForEach-Object { ([string]$_).Trim().ToLowerInvariant() } | Where-Object { $_ } | Select-Object -Unique)
    $targetInboxRows = @($activeInboxRows | Where-Object {
        $email = ([string]$_.email).Trim().ToLowerInvariant()
        (-not $targetEmails -or $targetEmails -contains $email)
    })
    if ($Limit -gt 0) {
        $targetInboxRows = @($targetInboxRows | Select-Object -First $Limit)
    }
    $script:Summary.targetInboxRows = $targetInboxRows.Count
    if ($targetInboxRows.Count -eq 0) {
        throw "No matching active inbox rows selected for repair."
    }

    foreach ($row in $targetInboxRows) {
        $email = ([string]$row.email).Trim().ToLowerInvariant()
        if (-not $email -or -not $email.EndsWith("@$new")) { continue }

        $localPart = ($email -split "@")[0]
        $displayName = "$($row.first_name) $($row.last_name)".Trim()
        if (-not $displayName) { $displayName = $localPart }
        $password = [string]$row.password

        $graphUser = $graphUsersWithNew | Where-Object { Test-ObjectHasEmail -Object $_ -Email $email } | Select-Object -First 1
        if (-not $graphUser) {
            $script:Summary.missingGraphUser += 1
            Add-Sample ([pscustomobject]@{ email = $email; issue = "missing_graph_user" })
            continue
        }

        $mailbox = $null
        if ($graphUser.id -and $mailboxByExternalId.ContainsKey([string]$graphUser.id)) {
            $mailbox = $mailboxByExternalId[[string]$graphUser.id]
        }
        if (-not $mailbox) {
            $mailbox = $mailboxes | Where-Object { Test-ObjectHasEmail -Object $_ -Email $email } | Select-Object -First 1
        }
        if (-not $mailbox) {
            $softDeletedMailbox = $null
            if ($graphUser.id -and $softDeletedByExternalId.ContainsKey([string]$graphUser.id)) {
                $softDeletedMailbox = $softDeletedByExternalId[[string]$graphUser.id]
            }
            if (-not $softDeletedMailbox) {
                $softDeletedMailbox = $softDeletedMailboxes | Where-Object { Test-ObjectHasEmail -Object $_ -Email $email } | Select-Object -First 1
            }

            if ($softDeletedMailbox) {
                $script:Summary.matchedSoftDeleted += 1
                $script:Summary.wouldUndoSoftDeleted += 1
                Add-Sample ([pscustomobject]@{
                    email = $email
                    graphId = $graphUser.id
                    graphUPN = $graphUser.userPrincipalName
                    softDeletedIdentity = [string]$softDeletedMailbox.Identity
                    softDeletedPrimary = [string]$softDeletedMailbox.PrimarySmtpAddress
                    softDeletedExchangeGuid = [string]$softDeletedMailbox.ExchangeGuid
                    softDeletedExternalDirectoryObjectId = [string]$softDeletedMailbox.ExternalDirectoryObjectId
                    restoreIdentity = (Get-SoftDeletedRestoreIdentity -Mailbox $softDeletedMailbox)
                    softDeletedOldAliasCount = @(Get-AddressesForDomain -Object $softDeletedMailbox -Domain $old).Count
                    issue = "soft_deleted_exchange_mailbox"
                })

                if (-not $Live) { continue }

                try {
                    $rowSecurePwd = ConvertTo-SecureString $password -AsPlainText -Force
                    $restoreIdentity = Get-SoftDeletedRestoreIdentity -Mailbox $softDeletedMailbox
                    if (-not $restoreIdentity) { throw "Could not resolve a unique soft-deleted mailbox identity" }
                    try {
                        Undo-SoftDeletedMailbox -SoftDeletedObject $restoreIdentity -Confirm:$false -ErrorAction Stop | Out-Null
                    } catch {
                        $undoError = $_.Exception.Message
                        if ($_.ErrorDetails.Message) { $undoError = "$undoError :: $($_.ErrorDetails.Message)" }
                        if ($undoError -notmatch "WindowsLiveID|Microsoft account|Password") {
                            throw $undoError
                        }
                        Undo-SoftDeletedMailbox -SoftDeletedObject $restoreIdentity -WindowsLiveID $email -Password $rowSecurePwd -DisplayName $displayName -Name $displayName -Confirm:$false -ErrorAction Stop | Out-Null
                    }
                    $script:Summary.softDeletedRestored += 1
                    Start-Sleep -Seconds 5
                    $mailbox = Get-Mailbox -Identity $email -ErrorAction SilentlyContinue
                    if (-not $mailbox) {
                        $mailboxes = @(Get-Mailbox -ResultSize Unlimited -ErrorAction Stop)
                        $mailbox = $mailboxes | Where-Object { Test-ObjectHasEmail -Object $_ -Email $email } | Select-Object -First 1
                    }
                    if (-not $mailbox) {
                        $script:Summary.restorePropagationPending += 1
                        continue
                    }
                } catch {
                    Add-RepairError "Soft-deleted mailbox restore failed for ${email}: $($_.Exception.Message)"
                    continue
                }
            } else {
                $script:Summary.missingMailbox += 1
                Add-Sample ([pscustomobject]@{ email = $email; graphId = $graphUser.id; graphUPN = $graphUser.userPrincipalName; issue = "missing_exchange_mailbox" })
                continue
            }
        }

        $script:Summary.matched += 1
        $primary = ([string]$mailbox.PrimarySmtpAddress).Trim().ToLowerInvariant()
        $oldAliases = @(Get-AddressesForDomain -Object $mailbox -Domain $old)
        $needsPrimaryRepair = ($primary -ne $email)
        if ($needsPrimaryRepair) { $script:Summary.wouldRepairPrimary += 1 } else { $script:Summary.alreadyPrimary += 1 }

        Add-Sample ([pscustomobject]@{
            email = $email
            graphId = $graphUser.id
            graphUPN = $graphUser.userPrincipalName
            mailboxIdentity = [string]$mailbox.Identity
            mailboxPrimary = [string]$mailbox.PrimarySmtpAddress
            oldAliasCount = $oldAliases.Count
        })

        if (-not $Live) { continue }

        try {
            Set-User -Identity $mailbox.Identity -DisplayName $displayName -FirstName ([string]$row.first_name) -LastName ([string]$row.last_name) -Confirm:$false -ErrorAction SilentlyContinue
            Set-User -Identity $mailbox.Identity -WindowsEmailAddress $email -Confirm:$false -ErrorAction SilentlyContinue
            Set-Mailbox -Identity $mailbox.Identity -Type Shared -DisplayName $displayName -Name $displayName -Alias $localPart -Confirm:$false -ErrorAction Stop
            Set-Mailbox -Identity $mailbox.Identity -MicrosoftOnlineServicesID $email -WindowsEmailAddress $email -Confirm:$false -ErrorAction Stop

            if ($oldAliases.Count -gt 0) {
                Set-Mailbox -Identity $mailbox.Identity -EmailAddresses @{Remove=$oldAliases} -Confirm:$false -ErrorAction Stop
                $script:Summary.oldAliasesRemoved += $oldAliases.Count
            }

            if ($needsPrimaryRepair) { $script:Summary.primaryRepaired += 1 }

            $patchBody = @{
                accountEnabled = $true
                passwordProfile = @{ forceChangePasswordNextSignIn = $false; password = $password }
            }
            if (([string]$graphUser.userPrincipalName).Trim().ToLowerInvariant() -ne $email) {
                $patchBody.userPrincipalName = $email
            }
            Invoke-GraphRequest -Method PATCH -Url "https://graph.microsoft.com/v1.0/users/$($graphUser.id)" -Bearer $bearer -Body $patchBody | Out-Null
            $script:Summary.graphUsersPatched += 1
        } catch {
            Add-RepairError "Repair failed for ${email}: $($_.Exception.Message)"
        }
    }

    if ($Live -and @($script:Summary.errors).Count -gt 0) {
        throw "Repair finished with $(@($script:Summary.errors).Count) error(s)."
    }

    if ($QueueReuploadWhenReady -and $Live -and $targetInboxRows.Count -eq $activeInboxRows.Count -and $Limit -le 0 -and $targetEmails.Count -eq 0) {
        $freshMailboxes = @(Get-Mailbox -ResultSize Unlimited -ErrorAction Stop)
        $finalNewMailboxes = @($freshMailboxes | Where-Object {
            ([string]$_.PrimarySmtpAddress).Trim().ToLowerInvariant().EndsWith("@$new")
        })
        $finalOldAliasCount = 0
        foreach ($mailbox in $finalNewMailboxes) {
            $finalOldAliasCount += @(Get-AddressesForDomain -Object $mailbox -Domain $old).Count
        }

        $freshGraphUsers = @(Get-AllGraphItems -Url "https://graph.microsoft.com/v1.0/users?`$select=id,userPrincipalName,mail,displayName,proxyAddresses,accountEnabled&`$top=999" -Bearer $bearer)
        $finalGraphUpns = @($freshGraphUsers | Where-Object {
            ([string]$_.userPrincipalName).Trim().ToLowerInvariant().EndsWith("@$new")
        })

        $script:Summary.finalActiveMailboxCount = $finalNewMailboxes.Count
        $script:Summary.finalOldAliasCount = $finalOldAliasCount
        $script:Summary.finalGraphUpnCount = $finalGraphUpns.Count

        if ($finalNewMailboxes.Count -lt $ExpectedUserCount -or $finalGraphUpns.Count -lt $ExpectedUserCount -or $finalOldAliasCount -gt 0) {
            throw "Reupload gate blocked: mailboxes=$($finalNewMailboxes.Count), graphUpns=$($finalGraphUpns.Count), oldAliases=$finalOldAliasCount, expected=$ExpectedUserCount."
        }

        $reuploadResult = New-RestoreReuploadAction -DomainRecord $newDomainRecord -ExpectedActiveInboxCount $ExpectedUserCount
        $script:Summary.reuploadQueued = $true
        $script:Summary.reuploadActionId = [string]$reuploadResult.Action.id
        Write-Log "Queued reupload_inboxes action $($script:Summary.reuploadActionId) for $new with restore settings." -Level Success
    }

    Write-Log "Repair scan complete for ${new}: matched=$($script:Summary.matched), softDeleted=$($script:Summary.matchedSoftDeleted), missingGraph=$($script:Summary.missingGraphUser), missingMailbox=$($script:Summary.missingMailbox), wouldRepairPrimary=$($script:Summary.wouldRepairPrimary), wouldUndoSoftDeleted=$($script:Summary.wouldUndoSoftDeleted), oldAliasesRemoved=$($script:Summary.oldAliasesRemoved)" -Level Success
} catch {
    Add-RepairError -Message ([string]$_.Exception.Message)
    throw
} finally {
    try { Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue } catch { }
    Save-RepairLog
}
