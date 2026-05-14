<#
.SYNOPSIS
    Restores replacement-domain users that were soft-deleted during old-domain cleanup.
.DESCRIPTION
    One-off repair for Azure replacement rows. It restores deleted Entra users
    that still reference the protected new domain, then removes old-domain aliases
    from the restored new-domain mailboxes.
#>

param(
    [Parameter(Mandatory=$true)][string]$OldDomain,
    [Parameter(Mandatory=$true)][string]$NewDomain,
    [switch]$Live,
    [string]$ConfirmText = "",
    [int]$ExpectedUserCount = 99,
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
    activeNewUsersBefore = 0
    deletedNewUsersBefore = 0
    restoredUsers = 0
    restoreErrors = 0
    activeNewUsersAfterRestore = 0
    oldAliasesRemoved = 0
    aliasCleanupErrors = 0
    activeNewUsersAfter = 0
    oldAliasesRemainingAfter = 0
    errors = @()
}

function Normalize-Domain {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant().Replace("https://", "").Replace("http://", "").TrimEnd("/")
}

function Add-Error {
    param([string]$Message)
    $script:Summary.errors += $Message
    Write-Log $Message -Level Error
}

function Save-RestoreLog {
    if (-not (Test-Path $LogDir)) {
        New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
    }
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
    $mode = if ($Live) { "live" } else { "dryrun" }
    $path = Join-Path $LogDir "profitpath-replacement-restore-$mode-$($script:Summary.newDomain)-$stamp.json"
    $script:Summary | ConvertTo-Json -Depth 20 | Set-Content -Path $path -Encoding UTF8
    Write-Log "Saved restore log: $path" -Level Info
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

function Test-GraphObjectReferencesDomain {
    param([object]$Object, [string]$Domain)
    $suffix = "@$((Normalize-Domain $Domain))"
    if (-not $Object -or -not $suffix) { return $false }
    return (
        ([string]$Object.userPrincipalName).ToLowerInvariant().EndsWith($suffix) -or
        ([string]$Object.mail).ToLowerInvariant().EndsWith($suffix) -or
        (@($Object.proxyAddresses) -join " ").ToLowerInvariant().Contains($suffix)
    )
}

function Get-ActiveUsersReferencingDomain {
    param([string]$Bearer, [string]$Domain)
    $users = @(Get-AllGraphItems -Url "https://graph.microsoft.com/v1.0/users?`$select=id,userPrincipalName,mail,displayName,proxyAddresses&`$top=999" -Bearer $Bearer)
    return @($users | Where-Object { Test-GraphObjectReferencesDomain -Object $_ -Domain $Domain })
}

function Get-DeletedUsersReferencingDomain {
    param([string]$Bearer, [string]$Domain)
    $users = @(Get-AllGraphItems -Url "https://graph.microsoft.com/v1.0/directory/deletedItems/microsoft.graph.user?`$select=id,userPrincipalName,mail,displayName,proxyAddresses&`$top=999" -Bearer $Bearer)
    return @($users | Where-Object { Test-GraphObjectReferencesDomain -Object $_ -Domain $Domain })
}

function Get-EmailAddressDomain {
    param([string]$Address)
    $value = ([string]$Address).Trim()
    if ($value -match '^[^:]+:(.+)$') { $value = $matches[1] }
    $value = $value.Trim().Trim('"').ToLowerInvariant()
    $at = $value.LastIndexOf("@")
    if ($at -lt 0 -or $at -eq ($value.Length - 1)) { return "" }
    return $value.Substring($at + 1)
}

function Get-MailboxAddressesForDomain {
    param([object]$Mailbox, [string]$Domain)
    $domainName = Normalize-Domain $Domain
    if (-not $Mailbox -or -not $domainName) { return @() }
    $matches = @()
    foreach ($address in @($Mailbox.EmailAddresses)) {
        if (-not $address) { continue }
        $raw = [string]$address
        if ((Get-EmailAddressDomain -Address $raw) -eq $domainName) {
            $matches += $raw
        }
    }
    return @($matches | Select-Object -Unique)
}

function Get-MailboxesWithPrimaryDomain {
    param([string]$Domain)
    $domainName = Normalize-Domain $Domain
    return @(Get-Mailbox -ResultSize Unlimited -ErrorAction SilentlyContinue | Where-Object {
        $_.PrimarySmtpAddress -and (Get-EmailAddressDomain -Address ([string]$_.PrimarySmtpAddress)) -eq $domainName
    })
}

try {
    $old = Normalize-Domain $OldDomain
    $new = Normalize-Domain $NewDomain
    $script:Summary.oldDomain = $old
    $script:Summary.newDomain = $new

    if (-not $old -or -not $new -or $old -eq $new) {
        throw "OldDomain and NewDomain must be non-empty and different."
    }

    $expectedConfirm = "RESTORE NEW DOMAIN $new FROM OLD $old"
    if ($Live -and $ConfirmText -ne $expectedConfirm) {
        throw "Live restore requires ConfirmText exactly: $expectedConfirm"
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

    $activeNewBefore = @(Get-ActiveUsersReferencingDomain -Bearer $bearer -Domain $new)
    $deletedNewBefore = @(Get-DeletedUsersReferencingDomain -Bearer $bearer -Domain $new)
    $script:Summary.activeNewUsersBefore = $activeNewBefore.Count
    $script:Summary.deletedNewUsersBefore = $deletedNewBefore.Count

    if ($activeNewBefore.Count -ge $ExpectedUserCount) {
        Write-Log "$new already has $($activeNewBefore.Count) active Graph users; restore may already be complete." -Level Warning
    }
    if (($activeNewBefore.Count + $deletedNewBefore.Count) -lt $ExpectedUserCount) {
        throw "Could only account for $($activeNewBefore.Count) active + $($deletedNewBefore.Count) deleted users for $new; expected $ExpectedUserCount."
    }
    if ($deletedNewBefore.Count -eq 0 -and $activeNewBefore.Count -lt $ExpectedUserCount) {
        throw "No deleted users found to restore for $new."
    }

    Write-Log "Restore preflight for ${new}: active=$($activeNewBefore.Count), deleted=$($deletedNewBefore.Count), DB active rows=$($activeInboxRows.Count)" -Level Success
    if (-not $Live) {
        Write-Log "Dry run complete. No users were restored." -Level Success
        return
    }

    foreach ($user in $deletedNewBefore) {
        if (-not $user.id) { continue }
        try {
            Invoke-GraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/directory/deletedItems/$($user.id)/restore" -Bearer $bearer | Out-Null
            $script:Summary.restoredUsers += 1
            Write-Log "Restored deleted user $($user.id) ($($user.displayName))" -Level Success
        } catch {
            $script:Summary.restoreErrors += 1
            Write-Log "Restore failed for $($user.id): $($_.Exception.Message)" -Level Error
        }
    }
    if ($script:Summary.restoreErrors -gt 0) {
        throw "Failed to restore $($script:Summary.restoreErrors) deleted user(s) for $new."
    }

    Start-Sleep -Seconds 30
    $activeAfterRestore = @(Get-ActiveUsersReferencingDomain -Bearer $bearer -Domain $new)
    $script:Summary.activeNewUsersAfterRestore = $activeAfterRestore.Count
    if ($activeAfterRestore.Count -lt $ExpectedUserCount) {
        throw "Only $($activeAfterRestore.Count) active new-domain users visible after restore; expected $ExpectedUserCount."
    }

    if (-not (Get-Module -ListAvailable -Name ExchangeOnlineManagement)) {
        Write-Log "Installing ExchangeOnlineManagement module..." -Level Warning
        Install-Module -Name ExchangeOnlineManagement -Force -AllowClobber -Scope CurrentUser
    }
    Import-Module ExchangeOnlineManagement -ErrorAction Stop
    $securePwd = ConvertTo-SecureString ([string]$adminRecord.password) -AsPlainText -Force
    $creds = New-Object System.Management.Automation.PSCredential($adminEmail, $securePwd)
    Connect-ExchangeOnline -Credential $creds -ShowBanner:$false -ErrorAction Stop

    $inboxByEmail = @{}
    foreach ($row in $activeInboxRows) {
        $email = ([string]$row.email).Trim().ToLowerInvariant()
        if ($email) { $inboxByEmail[$email] = $row }
    }

    foreach ($row in $activeInboxRows) {
        $email = ([string]$row.email).Trim().ToLowerInvariant()
        if (-not $email) { continue }
        $localPart = ($email -split "@")[0]
        $displayName = "$($row.first_name) $($row.last_name)".Trim()
        if (-not $displayName) { $displayName = $localPart }
        $password = [string]$row.password

        try {
            $mailbox = $null
            for ($attempt = 1; $attempt -le 6; $attempt++) {
                $mailbox = Get-Mailbox -Identity $email -ErrorAction SilentlyContinue
                if ($mailbox) { break }
                Start-Sleep -Seconds 10
            }
            if (-not $mailbox) {
                throw "Mailbox not visible after restore for $email"
            }

            Set-Mailbox -Identity $mailbox.Identity -Type Shared -PrimarySmtpAddress $email -WindowsEmailAddress $email -Alias $localPart -DisplayName $displayName -Name $displayName -ErrorAction Stop
            $oldAliases = @(Get-MailboxAddressesForDomain -Mailbox (Get-Mailbox -Identity $email -ErrorAction Stop) -Domain $old)
            if ($oldAliases.Count -gt 0) {
                Set-Mailbox -Identity $email -EmailAddresses @{Remove=$oldAliases} -Confirm:$false -ErrorAction Stop
                $script:Summary.oldAliasesRemoved += $oldAliases.Count
                Write-Log "Removed old alias(es) from restored mailbox ${email}: $($oldAliases -join ', ')" -Level Warning
            }

            if ($password) {
                $mailbox = Get-Mailbox -Identity $email -ErrorAction Stop
                $userId = [string]$mailbox.ExternalDirectoryObjectId
                if ($userId) {
                    $body = @{
                        accountEnabled = $true
                        userPrincipalName = $email
                        mail = $email
                        passwordProfile = @{ forceChangePasswordNextSignIn = $false; password = $password }
                    }
                    Invoke-GraphRequest -Method PATCH -Url "https://graph.microsoft.com/v1.0/users/$userId" -Bearer $bearer -Body $body | Out-Null
                }
            }
        } catch {
            $script:Summary.aliasCleanupErrors += 1
            Write-Log "Restore cleanup failed for ${email}: $($_.Exception.Message)" -Level Error
        }
    }

    if ($script:Summary.aliasCleanupErrors -gt 0) {
        throw "Restore cleanup failed for $($script:Summary.aliasCleanupErrors) mailbox(es)."
    }

    $finalMailboxes = @(Get-MailboxesWithPrimaryDomain -Domain $new)
    $remainingOldAliases = 0
    foreach ($mailbox in $finalMailboxes) {
        $remainingOldAliases += @(Get-MailboxAddressesForDomain -Mailbox $mailbox -Domain $old).Count
    }
    $activeNewAfter = @(Get-ActiveUsersReferencingDomain -Bearer $bearer -Domain $new)
    $script:Summary.activeNewUsersAfter = $activeNewAfter.Count
    $script:Summary.oldAliasesRemainingAfter = $remainingOldAliases

    if ($finalMailboxes.Count -lt $ExpectedUserCount -or $activeNewAfter.Count -lt $ExpectedUserCount) {
        throw "Post-restore check failed: mailboxes=$($finalMailboxes.Count), active Graph users=$($activeNewAfter.Count), expected $ExpectedUserCount."
    }
    if ($remainingOldAliases -gt 0) {
        throw "Post-restore check failed: $remainingOldAliases old-domain alias(es) remain on $new mailboxes."
    }

    Write-Log "Restore complete for ${new}: active users=$($activeNewAfter.Count), mailboxes=$($finalMailboxes.Count), old aliases remaining=0" -Level Success
} catch {
    Add-Error -Message ([string]$_.Exception.Message)
    throw
} finally {
    try { Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue } catch { }
    Save-RestoreLog
}
