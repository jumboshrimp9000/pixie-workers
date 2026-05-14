<#
.SYNOPSIS
    Finalizes a partially completed ProfitPath old-domain cleanup.
.DESCRIPTION
    Use only after the full deleter has removed active old-domain mailboxes/users
    but Microsoft still blocks accepted-domain removal because deleted users retain
    old-domain proxy references.
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
    activeDomainReferencesBefore = 0
    deletedUserReferencesBefore = 0
    protectedDeletedUserReferencesBefore = 0
    deletedUsersPurged = 0
    deletedUserPurgeErrors = 0
    oldDomainRemoved = $false
    oldDomainExistsAfter = $null
    newDomainExistsAfter = $null
    oldUsersAfter = $null
    newUsersAfter = $null
    activeDomainReferencesAfter = $null
    deletedUserReferencesAfter = $null
    protectedDeletedUserReferencesAfter = $null
    newUserIdsPreserved = $null
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

function Save-FinalizeLog {
    if (-not (Test-Path $LogDir)) {
        New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
    }
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
    $mode = if ($Live) { "live" } else { "dryrun" }
    $path = Join-Path $LogDir "profitpath-old-delete-finalize-$mode-$($script:Summary.oldDomain)-$stamp.json"
    $script:Summary | ConvertTo-Json -Depth 20 | Set-Content -Path $path -Encoding UTF8
    Write-Log "Saved finalize log: $path" -Level Info
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

function Get-GraphDomain {
    param([string]$Bearer, [string]$Domain)
    return Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer -AllowNotFound
}

function Get-GraphUsersByDomain {
    param([string]$Bearer, [string]$Domain)
    $suffix = "@$($Domain.ToLowerInvariant())"
    $users = @(Get-AllGraphItems -Url "https://graph.microsoft.com/v1.0/users?`$select=id,userPrincipalName,mail,displayName,accountEnabled,userType&`$top=999" -Bearer $Bearer)
    return @($users | Where-Object {
        ([string]$_.userPrincipalName).Trim().ToLowerInvariant().EndsWith($suffix) -or
        ([string]$_.mail).Trim().ToLowerInvariant().EndsWith($suffix)
    })
}

function Get-DomainNameReferences {
    param([string]$Bearer, [string]$Domain)
    return @(Get-AllGraphItems -Url "https://graph.microsoft.com/v1.0/domains/$Domain/domainNameReferences?`$top=999" -Bearer $Bearer)
}

function Get-DeletedUsersByDomain {
    param([string]$Bearer, [string]$Domain)
    $suffix = "@$($Domain.ToLowerInvariant())"
    $deletedUsers = @(Get-AllGraphItems -Url "https://graph.microsoft.com/v1.0/directory/deletedItems/microsoft.graph.user?`$select=id,userPrincipalName,mail,displayName,proxyAddresses&`$top=999" -Bearer $Bearer)
    return @($deletedUsers | Where-Object {
        ([string]$_.userPrincipalName).ToLowerInvariant().EndsWith($suffix) -or
        ([string]$_.mail).ToLowerInvariant().EndsWith($suffix) -or
        (@($_.proxyAddresses) -join " ").ToLowerInvariant().Contains($suffix)
    })
}

function Test-DeletedUserReferencesDomain {
    param([object]$User, [string]$Domain)
    $suffix = "@$((Normalize-Domain $Domain))"
    if (-not $User -or -not $suffix) { return $false }
    return (
        ([string]$User.userPrincipalName).ToLowerInvariant().EndsWith($suffix) -or
        ([string]$User.mail).ToLowerInvariant().EndsWith($suffix) -or
        (@($User.proxyAddresses) -join " ").ToLowerInvariant().Contains($suffix)
    )
}

function Remove-DeletedUsers {
    param([string]$Bearer, [object[]]$Users)
    $deleted = 0
    $errors = 0
    foreach ($user in @($Users)) {
        if (-not $user -or -not $user.id) { continue }
        try {
            Invoke-GraphRequest -Method DELETE -Url "https://graph.microsoft.com/v1.0/directory/deletedItems/$($user.id)" -Bearer $Bearer | Out-Null
            $deleted += 1
            Write-Log "Purged deleted user reference $($user.id)" -Level Info
        } catch {
            $errors += 1
            Write-Log "Deleted-user purge failed for $($user.id): $($_.Exception.Message)" -Level Warning
        }
    }
    return @{ Deleted = $deleted; Errors = $errors }
}

function Remove-AcceptedDomainWithRetry {
    param([string]$Bearer, [string]$Domain, [int]$MaxAttempts = 8)
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
                Start-Sleep -Seconds ([Math]::Min(90, 10 * $attempt))
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

    $expectedConfirm = "FINALIZE OLD DOMAIN $old KEEP NEW $new"
    if ($Live -and $ConfirmText -ne $expectedConfirm) {
        throw "Live finalization requires ConfirmText exactly: $expectedConfirm"
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
    $script:Summary.adminEmail = $adminEmail
    $tenantId = Get-TenantIdFromDomain -Domain (($adminEmail -split "@")[1])
    if (-not $tenantId) { throw "Could not resolve tenant id for admin $adminEmail" }
    $script:Summary.tenantId = $tenantId

    $bearer = Get-ROPCToken -TenantId $tenantId -ClientId $AzureCliPublicClientId -Username $adminEmail -Password ([string]$adminRecord.password)
    $oldGraphDomain = Get-GraphDomain -Bearer $bearer -Domain $old
    $newGraphDomain = Get-GraphDomain -Bearer $bearer -Domain $new
    $oldUsersBefore = @(Get-GraphUsersByDomain -Bearer $bearer -Domain $old)
    $newUsersBefore = @(Get-GraphUsersByDomain -Bearer $bearer -Domain $new)
    $newUserIdsBefore = New-Object System.Collections.Generic.HashSet[string]
    foreach ($user in $newUsersBefore) {
        if ($user.id) { $newUserIdsBefore.Add([string]$user.id) | Out-Null }
    }
    $activeRefsBefore = @(Get-DomainNameReferences -Bearer $bearer -Domain $old)
    $deletedRefsBefore = @(Get-DeletedUsersByDomain -Bearer $bearer -Domain $old)
    $protectedDeletedRefsBefore = @($deletedRefsBefore | Where-Object { Test-DeletedUserReferencesDomain -User $_ -Domain $new })
    $purgeableDeletedRefsBefore = @($deletedRefsBefore | Where-Object { -not (Test-DeletedUserReferencesDomain -User $_ -Domain $new) })

    $script:Summary.oldDomainExistsBefore = [bool]$oldGraphDomain
    $script:Summary.newDomainExistsBefore = [bool]$newGraphDomain
    $script:Summary.oldUsersBefore = $oldUsersBefore.Count
    $script:Summary.newUsersBefore = $newUsersBefore.Count
    $script:Summary.activeDomainReferencesBefore = $activeRefsBefore.Count
    $script:Summary.deletedUserReferencesBefore = $deletedRefsBefore.Count
    $script:Summary.protectedDeletedUserReferencesBefore = $protectedDeletedRefsBefore.Count

    if (-not $oldGraphDomain) { throw "Old domain $old is already absent; refusing finalizer until reviewed." }
    if (-not $newGraphDomain) { throw "New domain $new is not present in tenant $tenantId." }
    if ($newUsersBefore.Count -lt $ExpectedNewUserCount) {
        throw "New domain $new has $($newUsersBefore.Count) users, expected at least $ExpectedNewUserCount. Refusing finalization."
    }
    if ($oldUsersBefore.Count -gt 0) {
        throw "Old domain $old still has $($oldUsersBefore.Count) active users. Use the full deleter, not the finalizer."
    }
    if ($activeRefsBefore.Count -gt 0) {
        throw "Old domain $old still has $($activeRefsBefore.Count) active domain references. Refusing deleted-user purge."
    }

    Write-Log "Validated finalizer state for $old -> $new. Deleted-user refs: $($deletedRefsBefore.Count); protected new-domain refs: $($protectedDeletedRefsBefore.Count)" -Level Success

    if (-not $Live) {
        Write-Log "Dry run complete. No tenant changes were made." -Level Success
        return
    }

    if ($protectedDeletedRefsBefore.Count -gt 0) {
        throw "Refusing finalization: $($protectedDeletedRefsBefore.Count) deleted user(s) still reference protected new domain $new. Restore/repair those before purging old-domain deleted users."
    }

    if ($purgeableDeletedRefsBefore.Count -gt 0) {
        $purgeResult = Remove-DeletedUsers -Bearer $bearer -Users $purgeableDeletedRefsBefore
        $script:Summary.deletedUsersPurged = [int]$purgeResult.Deleted
        $script:Summary.deletedUserPurgeErrors = [int]$purgeResult.Errors
        if ($purgeResult.Errors -gt 0) {
            throw "Failed to purge $($purgeResult.Errors) deleted-user references for $old."
        }
        Start-Sleep -Seconds 20
    }

    $removeResult = Remove-AcceptedDomainWithRetry -Bearer $bearer -Domain $old
    if (-not $removeResult.Success) {
        throw "Failed to remove old accepted domain $old`: $($removeResult.Error)"
    }
    $script:Summary.oldDomainRemoved = $true

    $oldAfter = Get-GraphDomain -Bearer $bearer -Domain $old
    $newAfter = Get-GraphDomain -Bearer $bearer -Domain $new
    $oldUsersAfter = @(Get-GraphUsersByDomain -Bearer $bearer -Domain $old)
    $newUsersAfter = @(Get-GraphUsersByDomain -Bearer $bearer -Domain $new)
    $activeRefsAfter = if ($oldAfter) { @(Get-DomainNameReferences -Bearer $bearer -Domain $old) } else { @() }
    $deletedRefsAfter = @(Get-DeletedUsersByDomain -Bearer $bearer -Domain $old)
    $protectedDeletedRefsAfter = @($deletedRefsAfter | Where-Object { Test-DeletedUserReferencesDomain -User $_ -Domain $new })
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
    $script:Summary.activeDomainReferencesAfter = $activeRefsAfter.Count
    $script:Summary.deletedUserReferencesAfter = $deletedRefsAfter.Count
    $script:Summary.protectedDeletedUserReferencesAfter = $protectedDeletedRefsAfter.Count
    $script:Summary.newUserIdsPreserved = $preserved

    if ($oldAfter) { throw "Post-check failed: old domain $old still exists." }
    if (-not $newAfter) { throw "Post-check failed: new domain $new is missing." }
    if ($oldUsersAfter.Count -gt 0 -or $activeRefsAfter.Count -gt 0 -or $deletedRefsAfter.Count -gt 0 -or $protectedDeletedRefsAfter.Count -gt 0) {
        throw "Post-check failed: old domain references remain."
    }
    if (-not $preserved -or $newUsersAfter.Count -lt $ExpectedNewUserCount) {
        throw "Post-check failed: new domain user set was not preserved."
    }

    Write-Log "Finalized old domain $old; new domain $new preserved." -Level Success
} catch {
    Add-Error -Message ([string]$_.Exception.Message)
    throw
} finally {
    Save-FinalizeLog
}
