param(
    [Parameter(Mandatory=$true)][string]$OldDomain,
    [Parameter(Mandatory=$true)][string]$NewDomain,
    [string]$BatchId = "ffa54dd8-3fd1-4336-8e06-6f73872432f4"
)

. (Join-Path $PSScriptRoot "config.ps1")

function Normalize-Domain {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant().Replace("https://", "").Replace("http://", "").TrimEnd("/")
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
        $params.Body = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 12 }
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

$old = Normalize-Domain $OldDomain
$new = Normalize-Domain $NewDomain
$newDomainRecord = Get-DomainByName -DomainName $new
if (-not $newDomainRecord) { throw "New Simple Inboxes domain not found: $new" }
if ($BatchId -and ([string]$newDomainRecord.order_batch_id) -ne $BatchId) {
    throw "New domain $new belongs to batch $($newDomainRecord.order_batch_id), expected $BatchId"
}

$replacementOld = Normalize-Domain ([string]$newDomainRecord.fulfillment_settings.replacement_old_domain)
if ($replacementOld -ne $old) {
    throw "Replacement mapping mismatch for $new`: expected old $old, found $replacementOld"
}

$adminRecord = Get-AssignedAdmin -DomainId ([string]$newDomainRecord.id)
if (-not $adminRecord -or -not $adminRecord.email -or -not $adminRecord.password) {
    throw "No assigned Microsoft admin credentials found for $new"
}

$adminEmail = ([string]$adminRecord.email).Trim().ToLowerInvariant()
$tenantId = Get-TenantIdFromDomain -Domain (($adminEmail -split "@")[1])
if (-not $tenantId) { throw "Could not resolve tenant id for admin $adminEmail" }
$bearer = Get-ROPCToken -TenantId $tenantId -ClientId $AzureCliPublicClientId -Username $adminEmail -Password ([string]$adminRecord.password)

$oldGraphDomain = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$old" -Bearer $bearer -AllowNotFound
$newGraphDomain = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$new" -Bearer $bearer -AllowNotFound
$allUsers = @(Get-AllGraphItems -Url "https://graph.microsoft.com/v1.0/users?`$select=id,userPrincipalName,mail,displayName,proxyAddresses&`$top=999" -Bearer $bearer)
$oldSuffix = "@$old"
$newSuffix = "@$new"
$oldUsers = @($allUsers | Where-Object {
    ([string]$_.userPrincipalName).ToLowerInvariant().EndsWith($oldSuffix) -or
    ([string]$_.mail).ToLowerInvariant().EndsWith($oldSuffix) -or
    (@($_.proxyAddresses) -join " ").ToLowerInvariant().Contains($oldSuffix)
})
$newUsers = @($allUsers | Where-Object {
    ([string]$_.userPrincipalName).ToLowerInvariant().EndsWith($newSuffix) -or
    ([string]$_.mail).ToLowerInvariant().EndsWith($newSuffix) -or
    (@($_.proxyAddresses) -join " ").ToLowerInvariant().Contains($newSuffix)
})
$domainRefs = @(Get-AllGraphItems -Url "https://graph.microsoft.com/v1.0/domains/$old/domainNameReferences?`$top=999" -Bearer $bearer)
$deletedUsers = @(Get-AllGraphItems -Url "https://graph.microsoft.com/v1.0/directory/deletedItems/microsoft.graph.user?`$select=id,userPrincipalName,mail,displayName,proxyAddresses&`$top=999" -Bearer $bearer)
$deletedMatches = @($deletedUsers | Where-Object {
    ([string]$_.userPrincipalName).ToLowerInvariant().EndsWith($oldSuffix) -or
    ([string]$_.mail).ToLowerInvariant().EndsWith($oldSuffix) -or
    (@($_.proxyAddresses) -join " ").ToLowerInvariant().Contains($oldSuffix)
})
$deletedNewMatches = @($deletedUsers | Where-Object {
    ([string]$_.userPrincipalName).ToLowerInvariant().EndsWith($newSuffix) -or
    ([string]$_.mail).ToLowerInvariant().EndsWith($newSuffix) -or
    (@($_.proxyAddresses) -join " ").ToLowerInvariant().Contains($newSuffix)
})

[pscustomobject]@{
    oldDomain = $old
    newDomain = $new
    adminEmail = $adminEmail
    tenantId = $tenantId
    oldDomainExists = [bool]$oldGraphDomain
    newDomainExists = [bool]$newGraphDomain
    activeOldUserReferenceCount = $oldUsers.Count
    activeNewUserReferenceCount = $newUsers.Count
    activeNewUserSamples = @($newUsers | Select-Object -First 10 | ForEach-Object {
        [pscustomobject]@{
            id = $_.id
            userPrincipalName = $_.userPrincipalName
            mail = $_.mail
            displayName = $_.displayName
            proxyAddresses = @($_.proxyAddresses)
        }
    })
    domainNameReferenceCount = $domainRefs.Count
    domainNameReferenceSamples = @($domainRefs | Select-Object -First 15 | ForEach-Object {
        [pscustomobject]@{
            type = $_.'@odata.type'
            id = $_.id
            displayName = $_.displayName
            userPrincipalName = $_.userPrincipalName
            mail = $_.mail
        }
    })
    deletedUserReferenceCount = $deletedMatches.Count
    deletedUserSamples = @($deletedMatches | Select-Object -First 15 | ForEach-Object {
        [pscustomobject]@{
            id = $_.id
            userPrincipalName = $_.userPrincipalName
            mail = $_.mail
            displayName = $_.displayName
            proxyAddresses = @($_.proxyAddresses)
        }
    })
    deletedNewUserReferenceCount = $deletedNewMatches.Count
    deletedNewUserSamples = @($deletedNewMatches | Select-Object -First 15 | ForEach-Object {
        [pscustomobject]@{
            id = $_.id
            userPrincipalName = $_.userPrincipalName
            mail = $_.mail
            displayName = $_.displayName
            proxyAddresses = @($_.proxyAddresses)
        }
    })
} | ConvertTo-Json -Depth 10
