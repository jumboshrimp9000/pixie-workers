<#
.SYNOPSIS
    One-off ProfitPath old-domain tenant cleanup.
.DESCRIPTION
    Deletes an old Microsoft accepted domain and users only after validating the
    old->new replacement mapping stored on the new Simple Inboxes domain row.

    Default mode is read-only. Live mode requires:
      -Live
      -ConfirmText "DELETE OLD DOMAIN <old> KEEP NEW <new>"
#>

param(
    [Parameter(Mandatory=$true)][string]$OldDomain,
    [Parameter(Mandatory=$true)][string]$NewDomain,
    [switch]$Live,
    [string]$ConfirmText = "",
    [int]$ExpectedNewUserCount = 99,
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
    oldDomainExistsBefore = $false
    newDomainExistsBefore = $false
    oldUsersBefore = 0
    newUsersBefore = 0
    mailboxesRemoved = 0
    mailboxRemoveErrors = 0
    recipientsRemoved = 0
    recipientAliasesRemoved = 0
    protectedRecipientsSkipped = 0
    recipientRemoveErrors = 0
    graphUsersDeleted = 0
    graphUserDeleteErrors = 0
    oldDomainRemoved = $false
    oldDomainExistsAfter = $null
    newDomainExistsAfter = $null
    oldUsersAfter = $null
    newUsersAfter = $null
    newUserIdsPreserved = $null
    errors = @()
}

function Normalize-Domain {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant().Replace("https://", "").Replace("http://", "").TrimEnd("/")
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

function Get-RecipientAddressesForDomain {
    param([object]$Recipient, [string]$Domain)
    $domainName = Normalize-Domain $Domain
    if (-not $Recipient -or -not $domainName) { return @() }
    $matches = @()
    foreach ($address in @($Recipient.EmailAddresses)) {
        if (-not $address) { continue }
        $raw = [string]$address
        if ((Get-EmailAddressDomain -Address $raw) -eq $domainName) {
            $matches += $raw
        }
    }
    return @($matches | Select-Object -Unique)
}

function Test-RecipientHasDomain {
    param([object]$Recipient, [string]$Domain)
    $domainName = Normalize-Domain $Domain
    if (-not $Recipient -or -not $domainName) { return $false }
    $primary = [string]$Recipient.PrimarySmtpAddress
    if ($primary -and (Get-EmailAddressDomain -Address $primary) -eq $domainName) { return $true }
    return (@(Get-RecipientAddressesForDomain -Recipient $Recipient -Domain $domainName).Count -gt 0)
}

function Add-Error {
    param([string]$Message)
    $script:Summary.errors += $Message
    Write-Log $Message -Level Error
}

function Save-DeletionLog {
    if (-not (Test-Path $LogDir)) {
        New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
    }
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
    $mode = if ($Live) { "live" } else { "dryrun" }
    $path = Join-Path $LogDir "profitpath-old-delete-$mode-$($script:Summary.oldDomain)-$stamp.json"
    $script:Summary | ConvertTo-Json -Depth 20 | Set-Content -Path $path -Encoding UTF8
    Write-Log "Saved deletion log: $path" -Level Info
}

function Get-DomainByName {
    param([string]$DomainName)
    $encoded = [uri]::EscapeDataString($DomainName)
    $query = "domain=eq.$encoded&order=created_at.desc&limit=1&select=*"
    $result = Invoke-SupabaseApi -Method GET -Table "domains" -Query $query
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
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

    try {
        $response = Invoke-RestMethod -Method POST -Uri "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token" -Body $body -ContentType "application/x-www-form-urlencoded" -TimeoutSec 30 -ErrorAction Stop
        return $response.access_token
    } catch {
        throw "ROPC token failed for $Username`: $($_.Exception.Message)"
    }
}

function Invoke-GraphRequest {
    param(
        [string]$Method,
        [string]$Url,
        [string]$Bearer,
        [object]$Body = $null,
        [switch]$AllowNotFound
    )

    $headers = @{ Authorization = "Bearer $Bearer" }
    $params = @{
        Method = $Method
        Uri = $Url
        Headers = $headers
        ContentType = "application/json"
        TimeoutSec = 60
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
        throw
    }
}

function Get-GraphDomain {
    param([string]$Bearer, [string]$Domain)
    return Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer -AllowNotFound
}

function Get-GraphUsersByDomain {
    param([string]$Bearer, [string]$Domain)
    $suffix = "@$($Domain.ToLowerInvariant())"
    $url = "https://graph.microsoft.com/v1.0/users?`$select=id,userPrincipalName,mail,displayName,accountEnabled,userType&`$top=999"
    $users = New-Object System.Collections.Generic.List[object]

    while ($url) {
        $response = Invoke-GraphRequest -Method GET -Url $url -Bearer $Bearer
        foreach ($user in @($response.value)) {
            if (-not $user) { continue }
            $upn = ([string]$user.userPrincipalName).Trim().ToLowerInvariant()
            $mail = ([string]$user.mail).Trim().ToLowerInvariant()
            if (($upn -and $upn.EndsWith($suffix)) -or ($mail -and $mail.EndsWith($suffix))) {
                $users.Add($user) | Out-Null
            }
        }
        $url = [string]$response.'@odata.nextLink'
        if (-not $url) { break }
    }

    return $users.ToArray()
}

function Remove-MailboxesByUpn {
    param([object[]]$Users)

    $removed = 0
    $errors = 0
    foreach ($user in @($Users)) {
        $upn = [string]$user.userPrincipalName
        if (-not $upn) { continue }
        try {
            $mailbox = Get-Mailbox -Identity $upn -ErrorAction Stop
            Remove-Mailbox -Identity $mailbox.Identity -Confirm:$false -ErrorAction Stop
            $removed += 1
            Write-Log "Removed mailbox $upn" -Level Info
        } catch {
            $message = $_.Exception.Message
            if ($message -match "couldn't be found" -or $message -match "Cannot find object" -or $message -match "doesn't exist") {
                Write-Log "Mailbox already absent: $upn" -Level Info
            } else {
                $errors += 1
                Write-Log "Mailbox removal error for $upn`: $message" -Level Warning
            }
        }
    }
    return @{ Removed = $removed; Errors = $errors }
}

function Remove-ExchangeRecipientsByDomain {
    param([string]$Domain, [string]$ProtectedDomain = "")

    $domainName = Normalize-Domain $Domain
    $protectedDomainName = Normalize-Domain $ProtectedDomain
    $removed = 0
    $aliasesRemoved = 0
    $errors = 0
    $protectedSkipped = 0
    $recipientList = @()
    try {
        $recipientList = @(Get-Recipient -ResultSize Unlimited -ErrorAction SilentlyContinue)
    } catch {
        return @{ Removed = 0; Errors = 1; Error = $_.Exception.Message }
    }

    foreach ($recipient in $recipientList) {
        if (-not $recipient) { continue }
        $primary = [string]$recipient.PrimarySmtpAddress
        $primaryBelongsToDomain = ($primary -and (Get-EmailAddressDomain -Address $primary) -eq $domainName)
        $oldAliases = @(Get-RecipientAddressesForDomain -Recipient $recipient -Domain $domainName)
        if (-not $primaryBelongsToDomain -and $oldAliases.Count -eq 0) { continue }

        if ($protectedDomainName -and (Test-RecipientHasDomain -Recipient $recipient -Domain $protectedDomainName)) {
            $protectedSkipped += 1
            if ($oldAliases.Count -gt 0) {
                try {
                    Set-Mailbox -Identity $recipient.Identity -EmailAddresses @{Remove=$oldAliases} -Confirm:$false -ErrorAction Stop
                    $aliasesRemoved += $oldAliases.Count
                    Write-Log "Removed old-domain alias(es) from protected replacement recipient $($recipient.Identity): $($oldAliases -join ', ')" -Level Warning
                } catch {
                    $errors += 1
                    Write-Log "Protected alias removal failed for $($recipient.Identity): $($_.Exception.Message)" -Level Warning
                }
            }
            continue
        }

        if (-not $primaryBelongsToDomain) {
            if ($oldAliases.Count -gt 0) {
                try {
                    Set-Mailbox -Identity $recipient.Identity -EmailAddresses @{Remove=$oldAliases} -Confirm:$false -ErrorAction Stop
                    $aliasesRemoved += $oldAliases.Count
                    Write-Log "Removed old-domain alias(es) from non-domain-primary recipient $($recipient.Identity): $($oldAliases -join ', ')" -Level Warning
                } catch {
                    $errors += 1
                    Write-Log "Alias removal failed for $($recipient.Identity): $($_.Exception.Message)" -Level Warning
                }
            }
            continue
        }

        try {
            $recipientType = [string]$recipient.RecipientTypeDetails
            switch ($recipientType) {
                "MailUniversalDistributionGroup" { Remove-DistributionGroup -Identity $recipient.Identity -BypassSecurityGroupManagerCheck -Confirm:$false -ErrorAction Stop; $removed += 1 }
                "MailUniversalSecurityGroup" { Remove-DistributionGroup -Identity $recipient.Identity -BypassSecurityGroupManagerCheck -Confirm:$false -ErrorAction Stop; $removed += 1 }
                "GroupMailbox" { Remove-UnifiedGroup -Identity $recipient.Identity -Confirm:$false -ErrorAction Stop; $removed += 1 }
                "MailContact" { Remove-MailContact -Identity $recipient.Identity -Confirm:$false -ErrorAction Stop; $removed += 1 }
                "MailUser" { Remove-MailUser -Identity $recipient.Identity -Confirm:$false -ErrorAction Stop; $removed += 1 }
                "RoomMailbox" { Remove-Mailbox -Identity $recipient.Identity -Confirm:$false -ErrorAction Stop; $removed += 1 }
                "UserMailbox" { Remove-Mailbox -Identity $recipient.Identity -Confirm:$false -ErrorAction Stop; $removed += 1 }
                "SharedMailbox" { Remove-Mailbox -Identity $recipient.Identity -Confirm:$false -ErrorAction Stop; $removed += 1 }
                default {
                    Write-Log "Skipped unsupported recipient type $recipientType for $($recipient.Identity)" -Level Warning
                }
            }
        } catch {
            $errors += 1
            Write-Log "Recipient removal failed for $($recipient.Identity): $($_.Exception.Message)" -Level Warning
        }
    }

    return @{ Removed = $removed; AliasesRemoved = $aliasesRemoved; ProtectedSkipped = $protectedSkipped; Errors = $errors }
}

function Remove-GraphUsers {
    param([string]$Bearer, [object[]]$Users, [string]$AdminEmail)

    $deleted = 0
    $errors = 0
    foreach ($user in @($Users)) {
        if (-not $user -or -not $user.id) { continue }
        $upn = [string]$user.userPrincipalName
        if ($upn -and $AdminEmail -and $upn.Trim().ToLowerInvariant() -eq $AdminEmail.Trim().ToLowerInvariant()) {
            continue
        }
        try {
            Invoke-GraphRequest -Method DELETE -Url "https://graph.microsoft.com/v1.0/users/$($user.id)" -Bearer $Bearer | Out-Null
            $deleted += 1
            Write-Log "Deleted Graph user $upn" -Level Info
        } catch {
            $errors += 1
            Write-Log "Graph user delete failed for $upn`: $($_.Exception.Message)" -Level Warning
        }
    }
    return @{ Deleted = $deleted; Errors = $errors }
}

function Remove-OldAcceptedDomain {
    param(
        [string]$Bearer,
        [string]$Domain,
        [object[]]$OldUsers,
        [string]$AdminEmail,
        [int]$MaxAttempts = 6
    )

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt += 1) {
        try {
            $domainRecord = Get-GraphDomain -Bearer $Bearer -Domain $Domain
            if (-not $domainRecord) {
                Write-Log "Accepted domain already removed: $Domain" -Level Success
                return @{ Success = $true; AlreadyRemoved = $true; Attempts = $attempt }
            }
            Invoke-GraphRequest -Method DELETE -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer | Out-Null
            Write-Log "Accepted domain removed: $Domain" -Level Success
            return @{ Success = $true; Attempts = $attempt }
        } catch {
            $message = $_.Exception.Message
            Write-Log "Remove accepted domain failed (attempt $attempt/$MaxAttempts): $message" -Level Warning
            if ($attempt -lt $MaxAttempts) {
                Remove-ExchangeRecipientsByDomain -Domain $Domain -ProtectedDomain $script:Summary.newDomain | Out-Null
                $remaining = @(Get-GraphUsersByDomain -Bearer $Bearer -Domain $Domain)
                Remove-GraphUsers -Bearer $Bearer -Users $remaining -AdminEmail $AdminEmail | Out-Null
                Start-Sleep -Seconds ([Math]::Min(60, 10 * $attempt))
                continue
            }
            return @{ Success = $false; Error = $message; Attempts = $attempt }
        }
    }
    return @{ Success = $false; Error = "Unknown accepted-domain removal failure"; Attempts = $MaxAttempts }
}

try {
    $old = Normalize-Domain $OldDomain
    $new = Normalize-Domain $NewDomain
    $script:Summary.oldDomain = $old
    $script:Summary.newDomain = $new

    if (-not $old -or -not $new -or $old -eq $new) {
        throw "OldDomain and NewDomain must be non-empty and different."
    }

    $expectedConfirm = "DELETE OLD DOMAIN $old KEEP NEW $new"
    if ($Live -and $ConfirmText -ne $expectedConfirm) {
        throw "Live deletion requires ConfirmText exactly: $expectedConfirm"
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

    $oldDomainRecord = Get-DomainByName -DomainName $old
    if ($oldDomainRecord) {
        throw "Safety stop: old domain $old exists as a Simple Inboxes domain row. Use the normal cancellation path instead."
    }

    $adminRecord = Get-AssignedAdmin -DomainId ([string]$newDomainRecord.id)
    if (-not $adminRecord -or -not $adminRecord.email -or -not $adminRecord.password) {
        throw "No assigned Microsoft admin credentials found for $new"
    }

    $adminEmail = ([string]$adminRecord.email).Trim().ToLowerInvariant()
    $adminPassword = [string]$adminRecord.password
    $script:Summary.adminEmail = $adminEmail
    $adminDomain = ($adminEmail -split "@")[1]
    $tenantId = Get-TenantIdFromDomain -Domain $adminDomain
    if (-not $tenantId) { throw "Could not resolve tenant id for admin $adminEmail" }
    $script:Summary.tenantId = $tenantId

    $bearer = Get-ROPCToken -TenantId $tenantId -ClientId $AzureCliPublicClientId -Username $adminEmail -Password $adminPassword
    if (-not $bearer) { throw "Failed to obtain Graph token for admin $adminEmail" }

    $oldGraphDomain = Get-GraphDomain -Bearer $bearer -Domain $old
    $newGraphDomain = Get-GraphDomain -Bearer $bearer -Domain $new
    $oldUsersBefore = @(Get-GraphUsersByDomain -Bearer $bearer -Domain $old)
    $newUsersBefore = @(Get-GraphUsersByDomain -Bearer $bearer -Domain $new)
    $newUserIdsBefore = New-Object System.Collections.Generic.HashSet[string]
    foreach ($user in $newUsersBefore) {
        if ($user.id) { $newUserIdsBefore.Add([string]$user.id) | Out-Null }
    }

    $script:Summary.oldDomainExistsBefore = [bool]$oldGraphDomain
    $script:Summary.newDomainExistsBefore = [bool]$newGraphDomain
    $script:Summary.oldUsersBefore = $oldUsersBefore.Count
    $script:Summary.newUsersBefore = $newUsersBefore.Count

    if (-not $oldGraphDomain) { throw "Old domain $old is not present in tenant $tenantId." }
    if (-not $newGraphDomain) { throw "New domain $new is not present in tenant $tenantId." }
    if ($newUsersBefore.Count -lt $ExpectedNewUserCount) {
        throw "New domain $new has $($newUsersBefore.Count) users, expected at least $ExpectedNewUserCount. Refusing deletion."
    }
    if ($oldUsersBefore.Count -lt 1) {
        throw "Old domain $old has no matching users. Refusing deletion until reviewed."
    }

    Write-Log "Validated mapping $old -> $new in tenant $tenantId" -Level Success
    Write-Log "Old users before: $($oldUsersBefore.Count); new users before: $($newUsersBefore.Count)" -Level Info

    if (-not $Live) {
        Write-Log "Dry run complete. No tenant changes were made." -Level Success
        return
    }

    if (-not (Get-Module -ListAvailable -Name ExchangeOnlineManagement)) {
        Write-Log "Installing ExchangeOnlineManagement module..." -Level Warning
        Install-Module -Name ExchangeOnlineManagement -Force -AllowClobber -Scope CurrentUser
    }
    Import-Module ExchangeOnlineManagement -ErrorAction Stop

    $securePwd = ConvertTo-SecureString $adminPassword -AsPlainText -Force
    $creds = New-Object System.Management.Automation.PSCredential($adminEmail, $securePwd)
    Connect-ExchangeOnline -Credential $creds -ShowBanner:$false -ErrorAction Stop

    $mailboxResult = Remove-MailboxesByUpn -Users $oldUsersBefore
    $script:Summary.mailboxesRemoved = [int]$mailboxResult.Removed
    $script:Summary.mailboxRemoveErrors = [int]$mailboxResult.Errors

    $recipientResult = Remove-ExchangeRecipientsByDomain -Domain $old -ProtectedDomain $new
    $script:Summary.recipientsRemoved = [int]$recipientResult.Removed
    $script:Summary.recipientAliasesRemoved = [int]$recipientResult.AliasesRemoved
    $script:Summary.protectedRecipientsSkipped = [int]$recipientResult.ProtectedSkipped
    $script:Summary.recipientRemoveErrors = [int]$recipientResult.Errors

    $remainingOldUsers = @(Get-GraphUsersByDomain -Bearer $bearer -Domain $old)
    $graphResult = Remove-GraphUsers -Bearer $bearer -Users $remainingOldUsers -AdminEmail $adminEmail
    $script:Summary.graphUsersDeleted = [int]$graphResult.Deleted
    $script:Summary.graphUserDeleteErrors = [int]$graphResult.Errors

    $removeResult = Remove-OldAcceptedDomain -Bearer $bearer -Domain $old -OldUsers $remainingOldUsers -AdminEmail $adminEmail
    if (-not $removeResult.Success) {
        throw "Failed to remove old accepted domain $old`: $($removeResult.Error)"
    }
    $script:Summary.oldDomainRemoved = $true

    $oldAfter = Get-GraphDomain -Bearer $bearer -Domain $old
    $newAfter = Get-GraphDomain -Bearer $bearer -Domain $new
    $oldUsersAfter = @(Get-GraphUsersByDomain -Bearer $bearer -Domain $old)
    $newUsersAfter = @(Get-GraphUsersByDomain -Bearer $bearer -Domain $new)
    $newIdsAfter = New-Object System.Collections.Generic.HashSet[string]
    foreach ($user in $newUsersAfter) {
        if ($user.id) { $newIdsAfter.Add([string]$user.id) | Out-Null }
    }
    $preserved = $true
    foreach ($id in $newUserIdsBefore) {
        if (-not $newIdsAfter.Contains($id)) {
            $preserved = $false
            break
        }
    }

    $script:Summary.oldDomainExistsAfter = [bool]$oldAfter
    $script:Summary.newDomainExistsAfter = [bool]$newAfter
    $script:Summary.oldUsersAfter = $oldUsersAfter.Count
    $script:Summary.newUsersAfter = $newUsersAfter.Count
    $script:Summary.newUserIdsPreserved = $preserved

    if ($oldAfter) { throw "Post-check failed: old domain $old still exists." }
    if (-not $newAfter) { throw "Post-check failed: new domain $new is missing." }
    if (-not $preserved -or $newUsersAfter.Count -lt $ExpectedNewUserCount) {
        throw "Post-check failed: new domain user set was not preserved."
    }

    Write-Log "Live deletion complete for old domain $old; new domain $new preserved." -Level Success
} catch {
    Add-Error -Message ([string]$_.Exception.Message)
    throw
} finally {
    if ($Live) {
        try { Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue } catch { }
    }
    Save-DeletionLog
}
