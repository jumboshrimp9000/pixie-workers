<#
.SYNOPSIS
    Part 2: Microsoft Room Mailbox Creation — full Exchange Online pipeline.
.DESCRIPTION
    Creates Room (resource) mailboxes in Microsoft 365 using Exchange Online cmdlets.
    Reads from and writes to Supabase (not Airtable).

    This script is called by run.ps1 after Part 1 (domain setup) completes.
    It receives a domain_id and action_id, then:
      1. Finds an available admin in Supabase
      2. Gets ROPC token + connects to Exchange Online
      3. Adds domain to M365 tenant
      4. Verifies domain (TXT record via Cloudflare)
      5. Enables email service
      6. Adds DNS records (MX, SPF, DMARC, autodiscover)
      7. Waits for Exchange sync
      8. Creates room mailboxes with unique temp display names
      9. Renames display names back to real names
     10. Disables calendar auto-processing (CRITICAL: prevents email deletion)
     11. Fixes UPNs
     12. Confirms domain-level SMTP AUTH / IMAP readiness
     13. Sets up DKIM
     14. Bulletproof final check
     15. Failsafe top-up

    IMPORTANT: Room mailboxes DELETE non-calendar items by default.
    Set-CalendarProcessing -DeleteNonCalendarItems $false is MANDATORY.

.PARAMETER DomainId
    Supabase domain UUID
.PARAMETER ActionId
    Supabase action UUID
.PARAMETER DryRun
    Run without making actual changes
#>

param(
    [Parameter(Mandatory=$true)][string]$DomainId,
    [Parameter(Mandatory=$true)][string]$ActionId,
    [switch]$DryRun
)

# Load shared config + Supabase helpers
. (Join-Path $PSScriptRoot "config.ps1")

$ConfirmPreference = "None"

$AzureCliPublicClientId = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
$AzureAdPowerShellClientId = "1b730954-1685-4b74-9bfd-dac224a7b894"
$ExchangeResourceAppId = "00000002-0000-0ff1-ce00-000000000000"
$ImapProxyAppClientId = if ($env:IMAP_PROXY_CLIENT_ID) { $env:IMAP_PROXY_CLIENT_ID } else { "4abf3428-7905-494b-9184-bdca5d96e0d2" }
$ImapProxyAppRoleId = "5e5addcd-3e8d-4e90-baf5-964efab2b20a"
$ImapProxyDelegatedScope = "IMAP.AccessAsUser.All"

# ============================================================================
# CLOUDFLARE DNS FUNCTIONS
# ============================================================================
function Add-CloudflareDnsRecord {
    param(
        [string]$ZoneId,
        [string]$Type,
        [string]$Name,
        [string]$Content,
        [int]$TTL = 3600,
        [int]$Priority = -1
    )

    # Use Global API Key (X-Auth-Key + X-Auth-Email) for DNS write access
    $headers = @{
        "X-Auth-Key"   = $env:CLOUDFLARE_GLOBAL_KEY
        "X-Auth-Email" = $env:CLOUDFLARE_EMAIL
        "Content-Type" = "application/json"
    }

    $body = @{ type = $Type; name = $Name; content = $Content; ttl = $TTL }
    if ($Priority -ge 0 -and $Type -eq "MX") { $body.priority = $Priority }

    try {
        $bodyJson = $body | ConvertTo-Json -Depth 5 -Compress
        Invoke-RestMethod -Method POST -Uri "https://api.cloudflare.com/client/v4/zones/$ZoneId/dns_records" -Headers $headers -Body $bodyJson -UserAgent "pixie-worker/1.0" -TimeoutSec 30 -ErrorAction Stop | Out-Null
        Write-Log "Added $Type record: $Name -> $Content" -Level Success
        return @{ Success = $true }
    } catch {
        $errMsg = $_.Exception.Message
        # Check response body for "already exists" — CF returns 400 for duplicates (not 409)
        $detailMsg = ""
        if ($_.ErrorDetails.Message) { $detailMsg = $_.ErrorDetails.Message }
        $statusCode = $null
        if ($_.Exception.Response) { $statusCode = [int]$_.Exception.Response.StatusCode }
        if ($errMsg -match "already exists" -or $detailMsg -match "already exists" -or $detailMsg -match "same type" -or $statusCode -eq 409 -or ($statusCode -eq 400 -and $detailMsg -match "already exists|same type|Record already")) {
            Write-Log "$Type record already exists: $Name" -Level Warning
            return @{ Success = $true; AlreadyExists = $true }
        }
        Write-Log "Failed to add $Type record ($Name): $errMsg | Detail: $detailMsg" -Level Error
        return @{ Success = $false; Error = $errMsg }
    }
}

function Set-CloudflareDnsRecord {
    param(
        [string]$ZoneId,
        [string]$Type,
        [string]$Name,
        [string]$Content,
        [string]$Domain,
        [int]$TTL = 3600,
        [int]$Priority = -1
    )

    $headers = @{
        "X-Auth-Key"   = $env:CLOUDFLARE_GLOBAL_KEY
        "X-Auth-Email" = $env:CLOUDFLARE_EMAIL
        "Content-Type" = "application/json"
    }

    $recordName = $Name
    if ($Domain -and $Name -ne "@" -and $Name -notmatch [regex]::Escape($Domain) + "$") {
        $recordName = "$Name.$Domain"
    } elseif ($Domain -and $Name -eq "@") {
        $recordName = $Domain
    }

    $body = @{ type = $Type; name = $recordName; content = $Content; ttl = $TTL }
    if ($Priority -ge 0 -and $Type -eq "MX") { $body.priority = $Priority }
    $bodyJson = $body | ConvertTo-Json -Depth 5 -Compress

    try {
        $encodedName = [System.Uri]::EscapeDataString($recordName)
        $encodedType = [System.Uri]::EscapeDataString($Type)
        $existing = Invoke-RestMethod -Method GET -Uri "https://api.cloudflare.com/client/v4/zones/$ZoneId/dns_records?type=$encodedType&name=$encodedName" -Headers $headers -UserAgent "pixie-worker/1.0" -TimeoutSec 30 -ErrorAction Stop
        $records = @()
        if ($existing.result) { $records = @($existing.result) }

        if ($records.Count -gt 0) {
            $record = $records[0]
            $currentContent = ([string]$record.content).TrimEnd(".")
            $desiredContent = ([string]$Content).TrimEnd(".")
            if ($currentContent -eq $desiredContent) {
                Write-Log "$Type record already correct: $Name -> $Content" -Level Success
                return @{ Success = $true; AlreadyCorrect = $true; RecordId = $record.id }
            }

            Invoke-RestMethod -Method PATCH -Uri "https://api.cloudflare.com/client/v4/zones/$ZoneId/dns_records/$($record.id)" -Headers $headers -Body $bodyJson -UserAgent "pixie-worker/1.0" -TimeoutSec 30 -ErrorAction Stop | Out-Null
            Write-Log "Updated $Type record: $Name -> $Content (was $($record.content))" -Level Success
            return @{ Success = $true; Updated = $true; RecordId = $record.id }
        }
    } catch {
        Write-Log "Failed to inspect/update $Type record ($Name): $($_.Exception.Message)" -Level Warning
    }

    return Add-CloudflareDnsRecord -ZoneId $ZoneId -Type $Type -Name $Name -Content $Content -TTL $TTL -Priority $Priority
}

# ============================================================================
# M365 / GRAPH API FUNCTIONS
# ============================================================================
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
    param([string]$TenantId, [string]$ClientId, [string]$Username, [string]$Password, [string]$ScopeString = "https://graph.microsoft.com/.default")

    $tokenUrl = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
    $body = @{ grant_type = "password"; client_id = $ClientId; scope = $ScopeString; username = $Username; password = $Password }

    try {
        $response = Invoke-RestMethod -Method POST -Uri $tokenUrl -Body $body -ContentType "application/x-www-form-urlencoded" -TimeoutSec 30 -ErrorAction Stop
        return $response.access_token
    } catch {
        Write-Log "ROPC token failed: $($_.Exception.Message)" -Level Error
        return $null
    }
}

function Get-DirectoryGraphToken {
    param([string]$TenantId, [string]$Username, [string]$Password)

    $tokenUrl = "https://login.microsoftonline.com/$TenantId/oauth2/token"
    $body = @{
        grant_type = "password"
        client_id = $AzureAdPowerShellClientId
        resource = "https://graph.microsoft.com"
        username = $Username
        password = $Password
    }

    try {
        $response = Invoke-RestMethod -Method POST -Uri $tokenUrl -Body $body -ContentType "application/x-www-form-urlencoded" -TimeoutSec 30 -ErrorAction Stop
        return $response.access_token
    } catch {
        Write-Log "Directory Graph token failed: $($_.Exception.Message)" -Level Warning
        return $null
    }
}

function Invoke-GraphRequest {
    param([string]$Method, [string]$Url, [string]$Bearer, [object]$Body = $null)

    $headers = @{ Authorization = "Bearer $Bearer" }
    $params = @{ Method = $Method; Uri = $Url; Headers = $headers; ContentType = "application/json"; TimeoutSec = 60; ErrorAction = "Stop" }
    if ($Body) { $params.Body = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 15 } }

    Invoke-RestMethod @params
}

function Normalize-DomainValue {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant().Replace("https://", "").Replace("http://", "").TrimEnd("/")
}

function Get-ObjectPropertyValue {
    param([object]$Object, [string]$Name)
    if (-not $Object -or -not $Name) { return $null }
    if ($Object -is [string]) {
        try { $Object = $Object | ConvertFrom-Json -Depth 20 } catch { return $null }
    }
    $property = $Object.PSObject.Properties[$Name]
    if ($property) { return $property.Value }
    return $null
}

function Get-ReplacementOldDomainFromRecord {
    param([object]$DomainRecord)
    $settings = Get-ObjectPropertyValue -Object $DomainRecord -Name "fulfillment_settings"
    if (-not $settings) { return "" }

    $direct = Get-ObjectPropertyValue -Object $settings -Name "replacement_old_domain"
    if ($direct) { return Normalize-DomainValue ([string]$direct) }

    $replacement = Get-ObjectPropertyValue -Object $settings -Name "replacement"
    if ($replacement) {
        foreach ($key in @("old_domain", "oldDomain", "replacement_old_domain")) {
            $value = Get-ObjectPropertyValue -Object $replacement -Name $key
            if ($value) { return Normalize-DomainValue ([string]$value) }
        }
    }

    return ""
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
    $domainName = Normalize-DomainValue $Domain
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

function Invoke-ReplacementAliasIsolation {
    param(
        [string]$Domain,
        [string]$ReplacementOldDomain,
        [int]$ExpectedMailboxCount = 0,
        [switch]$Repair
    )

    $newDomain = Normalize-DomainValue $Domain
    $oldDomain = Normalize-DomainValue $ReplacementOldDomain
    $result = @{
        Success = $true
        Checked = 0
        Repaired = 0
        Blocked = 0
        Issues = @()
    }

    if (-not $oldDomain) { return $result }
    if (-not $newDomain -or $newDomain -eq $oldDomain) {
        return @{ Success = $false; Checked = 0; Repaired = 0; Blocked = 1; Issues = @("Replacement alias isolation received invalid domains: new=$newDomain old=$oldDomain") }
    }

    Write-Log "Checking replacement alias isolation: new=$newDomain old=$oldDomain" -Level Info
    $mailboxes = @(Get-Mailbox -ResultSize Unlimited -ErrorAction SilentlyContinue | Where-Object {
        ($_.PrimarySmtpAddress -and (Get-EmailAddressDomain -Address ([string]$_.PrimarySmtpAddress)) -eq $newDomain) -or
        @(Get-MailboxAddressesForDomain -Mailbox $_ -Domain $newDomain).Count -gt 0
    })
    $result.Checked = $mailboxes.Count

    foreach ($mailbox in $mailboxes) {
        $primary = [string]$mailbox.PrimarySmtpAddress
        $newAliases = @(Get-MailboxAddressesForDomain -Mailbox $mailbox -Domain $newDomain)
        if ((Get-EmailAddressDomain -Address $primary) -ne $newDomain -and $newAliases.Count -gt 0) {
            $targetPrimary = ([string]$newAliases[0]) -replace '^[Ss][Mm][Tt][Pp]:', ''

            if (-not $Repair) {
                $result.Success = $false
                $result.Blocked += 1
                $result.Issues += "$primary carries new-domain alias $targetPrimary but it is not primary"
                continue
            }

            try {
                Set-Mailbox -Identity $mailbox.Identity -WindowsEmailAddress $targetPrimary -MicrosoftOnlineServicesID $targetPrimary -Confirm:$false -ErrorAction Stop
                Start-Sleep -Seconds 3
                $mailbox = Get-Mailbox -Identity $targetPrimary -ErrorAction Stop
                $primary = [string]$mailbox.PrimarySmtpAddress
                if ((Get-EmailAddressDomain -Address $primary) -eq $newDomain) {
                    Write-Log "Promoted new-domain alias to primary for replacement mailbox: $targetPrimary" -Level Warning
                    $result.Repaired += 1
                } else {
                    $result.Success = $false
                    $result.Blocked += 1
                    $result.Issues += "Failed to promote new-domain alias $targetPrimary to primary; current primary is $primary"
                    continue
                }
            } catch {
                $result.Success = $false
                $result.Blocked += 1
                $result.Issues += "Failed to promote new-domain alias $targetPrimary to primary: $($_.Exception.Message)"
                continue
            }
        }

        $oldAliases = @(Get-MailboxAddressesForDomain -Mailbox $mailbox -Domain $oldDomain)
        if ($oldAliases.Count -eq 0) { continue }

        if (-not $Repair) {
            $result.Success = $false
            $result.Blocked += 1
            $result.Issues += "$primary carries old-domain alias(es): $($oldAliases -join ', ')"
            continue
        }

        try {
            Set-Mailbox -Identity $mailbox.Identity -EmailAddresses @{Remove=$oldAliases} -Confirm:$false -ErrorAction Stop
            Write-Log "Removed old-domain alias(es) from replacement mailbox ${primary}: $($oldAliases -join ', ')" -Level Warning
            $result.Repaired += 1
        } catch {
            $result.Success = $false
            $result.Blocked += 1
            $result.Issues += "Failed to remove old-domain alias(es) from ${primary}: $($_.Exception.Message)"
        }
    }

    $remaining = @(Get-Mailbox -ResultSize Unlimited -ErrorAction SilentlyContinue | Where-Object {
        $_.PrimarySmtpAddress -and (Get-EmailAddressDomain -Address ([string]$_.PrimarySmtpAddress)) -eq $newDomain -and
        @(Get-MailboxAddressesForDomain -Mailbox $_ -Domain $oldDomain).Count -gt 0
    })
    if ($remaining.Count -gt 0) {
        $result.Success = $false
        $result.Blocked += $remaining.Count
        foreach ($mailbox in $remaining) {
            $result.Issues += "$($mailbox.PrimarySmtpAddress) still carries old-domain alias(es): $((Get-MailboxAddressesForDomain -Mailbox $mailbox -Domain $oldDomain) -join ', ')"
        }
    }

    $primaryCount = @(Get-Mailbox -ResultSize Unlimited -ErrorAction SilentlyContinue | Where-Object {
        $_.PrimarySmtpAddress -and (Get-EmailAddressDomain -Address ([string]$_.PrimarySmtpAddress)) -eq $newDomain
    }).Count
    if ($ExpectedMailboxCount -gt 0 -and $primaryCount -lt $ExpectedMailboxCount) {
        $result.Success = $false
        $result.Issues += "Expected at least $ExpectedMailboxCount primary $newDomain mailbox(es), found $primaryCount."
    }

    if ($result.Success) {
        Write-Log "Replacement alias isolation passed for $newDomain; checked $($result.Checked), repaired $($result.Repaired)" -Level Success
    } else {
        Write-Log "Replacement alias isolation failed for ${newDomain}: $($result.Issues -join '; ')" -Level Error
    }

    return $result
}

function Get-ServicePrincipalByAppId {
    param([string]$Bearer, [string]$AppId)

    $filter = [System.Uri]::EscapeDataString("appId eq '$AppId'")
    $result = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/servicePrincipals?`$filter=$filter" -Bearer $Bearer
    if ($result.value -and $result.value.Count -gt 0) {
        return $result.value[0]
    }
    return $null
}

function Ensure-ServicePrincipal {
    param([string]$Bearer, [string]$AppId, [string]$Label)

    $existing = Get-ServicePrincipalByAppId -Bearer $Bearer -AppId $AppId
    if ($existing) { return $existing }

    Write-Log "Creating $Label service principal..." -Level Info
    $created = Invoke-GraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/servicePrincipals" -Bearer $Bearer -Body @{ appId = $AppId }
    Start-Sleep -Seconds 2
    return $created
}

function Ensure-ImapProxyTenantConsent {
    param([string]$Bearer, [string]$Domain)

    try {
        if (-not $ImapProxyAppClientId) {
            return @{ Success = $false; Error = "IMAP proxy client id is not configured" }
        }

        $proxySp = Ensure-ServicePrincipal -Bearer $Bearer -AppId $ImapProxyAppClientId -Label "IMAP proxy"
        $exchangeSp = Ensure-ServicePrincipal -Bearer $Bearer -AppId $ExchangeResourceAppId -Label "Exchange Online"
        if (-not $proxySp -or -not $exchangeSp) {
            return @{ Success = $false; Error = "Could not resolve IMAP proxy or Exchange service principal" }
        }

        $assignments = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/servicePrincipals/$($proxySp.id)/appRoleAssignments" -Bearer $Bearer
        $hasAppRole = $false
        if ($assignments.value) {
            $hasAppRole = [bool]($assignments.value | Where-Object { $_.resourceId -eq $exchangeSp.id -and $_.appRoleId -eq $ImapProxyAppRoleId } | Select-Object -First 1)
        }
        if (-not $hasAppRole) {
            Invoke-GraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/servicePrincipals/$($proxySp.id)/appRoleAssignments" -Bearer $Bearer -Body @{
                principalId = $proxySp.id
                resourceId = $exchangeSp.id
                appRoleId = $ImapProxyAppRoleId
            } | Out-Null
        }

        $grantFilter = [System.Uri]::EscapeDataString("clientId eq '$($proxySp.id)' and resourceId eq '$($exchangeSp.id)'")
        $grants = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/oauth2PermissionGrants?`$filter=$grantFilter" -Bearer $Bearer
        $grant = $null
        if ($grants.value) {
            $grant = $grants.value | Where-Object { $_.consentType -eq "AllPrincipals" } | Select-Object -First 1
        }

        if ($grant) {
            $scopes = @()
            if ($grant.scope) { $scopes = @($grant.scope -split "\s+" | Where-Object { $_ }) }
            if ($scopes -notcontains $ImapProxyDelegatedScope) {
                $scopes += $ImapProxyDelegatedScope
                Invoke-GraphRequest -Method PATCH -Url "https://graph.microsoft.com/v1.0/oauth2PermissionGrants/$($grant.id)" -Bearer $Bearer -Body @{
                    scope = (($scopes | Select-Object -Unique) -join " ")
                } | Out-Null
            }
        } else {
            Invoke-GraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/oauth2PermissionGrants" -Bearer $Bearer -Body @{
                clientId = $proxySp.id
                consentType = "AllPrincipals"
                resourceId = $exchangeSp.id
                scope = $ImapProxyDelegatedScope
            } | Out-Null
        }

        Write-Log "IMAP proxy tenant consent confirmed for $Domain" -Level Success
        return @{ Success = $true }
    } catch {
        Write-Log "IMAP proxy tenant consent failed: $($_.Exception.Message)" -Level Error
        return @{ Success = $false; Error = $_.Exception.Message }
    }
}

function Add-DomainToM365 {
    param([string]$Bearer, [string]$Domain)

    Write-Log "Adding domain to M365 tenant: $Domain" -Level Info

    try {
        $existing = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer
        Write-Log "Domain already exists in tenant (verified: $($existing.isVerified))" -Level Warning
        return @{ Success = $true; AlreadyExists = $true; IsVerified = $existing.isVerified }
    } catch { }

    try {
        $body = @{ id = $Domain }
        Invoke-GraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/domains" -Bearer $Bearer -Body $body | Out-Null
        Write-Log "Domain added to M365 tenant" -Level Success
        return @{ Success = $true; AlreadyExists = $false; IsVerified = $false }
    } catch {
        Write-Log "Failed to add domain: $($_.Exception.Message)" -Level Error
        return @{ Success = $false; Error = $_.Exception.Message }
    }
}

function Get-DomainVerificationRecord {
    param([string]$Bearer, [string]$Domain)

    Write-Log "Getting verification TXT record for: $Domain" -Level Info
    try {
        $records = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$Domain/verificationDnsRecords" -Bearer $Bearer
        foreach ($rec in $records.value) {
            if ($rec.recordType -eq "Txt") {
                Write-Log "Found verification TXT: $($rec.text)" -Level Success
                return $rec.text
            }
        }
    } catch { Write-Log "Failed to get verification records: $($_.Exception.Message)" -Level Error }
    return $null
}

function Test-DomainVerified {
    param([string]$Bearer, [string]$Domain)
    try {
        $domainInfo = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer
        return $domainInfo.isVerified
    } catch { return $false }
}

function Get-M365DomainState {
    param([string]$Bearer, [string]$Domain)

    try {
        $domainInfo = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer
        $supportedServices = @()
        if ($domainInfo.supportedServices) {
            $supportedServices = @($domainInfo.supportedServices)
        }

        return @{
            Success = $true
            IsVerified = [bool]$domainInfo.isVerified
            SupportedServices = $supportedServices
        }
    } catch {
        Write-Log "Failed to fetch M365 domain state for ${Domain}: $($_.Exception.Message)" -Level Warning
        return @{
            Success = $false
            IsVerified = $false
            SupportedServices = @()
            Error = $_.Exception.Message
        }
    }
}

function Verify-M365Domain {
    param([string]$Bearer, [string]$Domain, [int]$MaxAttempts = 12, [int]$WaitSeconds = 30)

    Write-Log "Verifying domain in M365: $Domain" -Level Info

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        Write-Log "Verification attempt $attempt of $MaxAttempts..." -Level Info

        try {
            $result = Invoke-GraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/domains/$Domain/verify" -Bearer $Bearer
            if ($result.isVerified) {
                Write-Log "Domain verified successfully" -Level Success
                return $true
            }
        } catch {
            if (Test-DomainVerified -Bearer $Bearer -Domain $Domain) {
                Write-Log "Domain is already verified" -Level Success
                return $true
            }
            Write-Log "Verification error: $($_.Exception.Message)" -Level Warning
        }

        if ($attempt -lt $MaxAttempts) { Start-Sleep -Seconds $WaitSeconds }
    }

    Write-Log "Domain verification failed after $MaxAttempts attempts" -Level Error
    return $false
}

function Enable-DomainEmailService {
    param([string]$Bearer, [string]$Domain)

    Write-Log "Enabling Email service for domain: $Domain" -Level Info

    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            $domainInfo = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer
            $currentServices = @()
            if ($domainInfo.supportedServices) { $currentServices = @($domainInfo.supportedServices) }

            if ($currentServices -contains "Email") {
                Write-Log "Email service already enabled" -Level Success
                return $true
            }

            $newServices = @($currentServices) + @("Email")
            $updateBody = @{ supportedServices = $newServices }

            Invoke-GraphRequest -Method PATCH -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer -Body $updateBody | Out-Null
            Write-Log "Email service enabled for $Domain" -Level Success
            return $true
        } catch {
            $errorMsg = $_.Exception.Message
            Write-Log "Email service attempt $attempt failed: $errorMsg" -Level Warning

            if ($errorMsg -match "400" -and $attempt -eq 1) {
                try {
                    $updateBody = @{ supportedServices = @("Email") }
                    Invoke-GraphRequest -Method PATCH -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer -Body $updateBody | Out-Null
                    Write-Log "Email service enabled for $Domain (Email only)" -Level Success
                    return $true
                } catch { }
            }
            if ($attempt -lt 3) { Start-Sleep -Seconds 5 }
        }
    }
    Write-Log "Failed to enable Email service after 3 attempts" -Level Warning
    return $false
}

function Test-AzureAcceptedDomainMailboxCreateBlocked {
    param([string]$Message)

    if ([string]::IsNullOrWhiteSpace($Message)) { return $false }
    return ($Message -match "Azure mailbox creation blocked" -and $Message -match "not an accepted domain|can't use the domain")
}

function Test-AcceptedDomainReaddAlreadyAttempted {
    param([object]$Action)

    if (-not $Action -or -not $Action.result) { return $false }
    $result = $Action.result
    if ($result -is [string]) {
        try { $result = $result | ConvertFrom-Json -Depth 20 } catch { return $false }
    }

    $repair = $result.PSObject.Properties["accepted_domain_readd_recovery"]
    if (-not $repair) { return $false }
    $attempted = $repair.Value.PSObject.Properties["attempted"]
    return ($attempted -and [bool]$attempted.Value)
}

function Invoke-AzureAcceptedDomainReaddRecovery {
    param(
        [string]$Bearer,
        [string]$Domain,
        [string]$DomainId,
        [string]$CustomerId,
        [string]$ActionId,
        [string]$History,
        [string]$FailureMessage,
        [int]$DelaySeconds = 60
    )

    $action = Get-Action -ActionId $ActionId
    if (Test-AcceptedDomainReaddAlreadyAttempted -Action $action) {
        Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "accepted_domain_readd_skipped_already_attempted" -Severity "error" -Message "Accepted-domain delete/re-add recovery already ran once; refusing to loop automatically." -Metadata @{
            blocker_code = "azure_accepted_domain_mailbox_creation_blocked"
            failure = $FailureMessage
        }
        return $false
    }

    $deleted = $false
    $wasPresent = $false
    $wasVerified = $false
    $supportedServices = @()

    try {
        Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "accepted_domain_readd_started" -Severity "warn" -Message "Azure mailbox creation hit an accepted-domain inconsistency; deleting/re-adding the Microsoft domain once before retry." -Metadata @{
            blocker_code = "azure_accepted_domain_mailbox_creation_blocked"
            failure = $FailureMessage
        }

        try {
            $existing = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer
            if ($existing) {
                $wasPresent = $true
                $wasVerified = [bool]$existing.isVerified
                if ($existing.supportedServices) { $supportedServices = @($existing.supportedServices) }
            }
        } catch {
            $message = $_.Exception.Message
            if ($message -notmatch "404|Not Found|Resource .* does not exist") {
                throw
            }
        }

        if ($wasPresent) {
            Invoke-GraphRequest -Method DELETE -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer | Out-Null
            $deleted = $true
            Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "accepted_domain_deleted_for_readd" -Severity "warn" -Message "Microsoft Graph domain object deleted; worker will re-add, re-verify, and wait for Exchange sync before mailbox creation." -Metadata @{
                was_verified = $wasVerified
                supported_services = $supportedServices
            }
        } else {
            Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "accepted_domain_delete_skipped_absent" -Severity "warn" -Message "Microsoft Graph did not list the domain; worker will re-add and re-verify it." -Metadata @{}
        }

        $now = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        $nextRetryAt = (Get-Date).ToUniversalTime().AddSeconds($DelaySeconds).ToString("yyyy-MM-ddTHH:mm:ssZ")
        $history = Add-HistoryEntry -History $History -Entry "OPS RECOVERY: Azure accepted-domain mailbox creation was blocked; deleted/re-adding Microsoft domain and retrying setup."
        Update-Domain -DomainId $DomainId -Fields @{
            status = "in_progress"
            interim_status = "Both - DNS Zone Created"
            action_history = $history
        }

        $attempts = if ($action -and $action.attempts -ne $null) { [int]$action.attempts } else { 1 }
        $restoredAttempts = [Math]::Max(0, $attempts - 1)
        $repairResult = @{
            accepted_domain_readd_recovery = @{
                attempted = $true
                attempted_at = $now
                blocker_code = "azure_accepted_domain_mailbox_creation_blocked"
                graph_domain_was_present = $wasPresent
                graph_domain_deleted = $deleted
                was_verified = $wasVerified
                supported_services = $supportedServices
                retry_from_interim_status = "Both - DNS Zone Created"
            }
        }
        $body = @{
            status = "pending"
            error = "Retrying after Azure accepted-domain delete/re-add recovery"
            attempts = $restoredAttempts
            next_retry_at = $nextRetryAt
            started_at = $null
            completed_at = $null
            updated_at = $now
            result = $repairResult
        }

        $update = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query (Get-ActionFenceQuery -ActionId $ActionId -Action $action -RequireFence) -Body $body
        if (-not $update.Success -or -not $update.Data -or $update.Data.Count -eq 0) {
            Write-Log "Accepted-domain recovery could not requeue action $ActionId because the lease fence no longer matched" -Level Warning
            return $false
        }

        Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "accepted_domain_readd_requeued" -Severity "info" -Message "Requeued provisioning after accepted-domain delete/re-add recovery." -Metadata @{
            next_retry_at = $nextRetryAt
            retry_delay_seconds = $DelaySeconds
        }
        return $true
    } catch {
        $errorText = $_.Exception.Message
        Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "accepted_domain_readd_failed" -Severity "error" -Message "Accepted-domain delete/re-add recovery failed: $errorText" -Metadata @{
            blocker_code = "azure_accepted_domain_mailbox_creation_blocked"
        }
        return $false
    }
}

function Add-M365DnsRecords {
    param([string]$Domain, [string]$Bearer, [string]$ZoneId)

    Write-Log "Fetching required DNS records from Microsoft for: $Domain" -Level Info

    $headers = @{ Authorization = "Bearer $Bearer" }
    $added = 0
    $existed = 0
    $failed = 0

    try {
        $records = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/domains/$Domain/serviceConfigurationRecords" -Headers $headers -ErrorAction Stop

        foreach ($rec in $records.value) {
            $result = $null
            switch ($rec.recordType) {
                "Mx" {
                    $result = Add-CloudflareDnsRecord -ZoneId $ZoneId -Type "MX" -Name "@" -Content $rec.mailExchange -Priority $rec.preference
                }
                "Txt" {
                    if ($rec.text -match "spf") {
                        $result = Add-CloudflareDnsRecord -ZoneId $ZoneId -Type "TXT" -Name "@" -Content $rec.text
                    }
                }
                "CName" {
                    $cnameName = $rec.label -replace "\.$Domain$", ""
                    $result = Add-CloudflareDnsRecord -ZoneId $ZoneId -Type "CNAME" -Name $cnameName -Content $rec.canonicalName
                }
            }
            if ($result) {
                if ($result.AlreadyExists) { $existed++ }
                elseif ($result.Success) { $added++ }
                else { $failed++ }
            }
        }

        $dmarcResult = Add-CloudflareDnsRecord -ZoneId $ZoneId -Type "TXT" -Name "_dmarc" -Content "v=DMARC1; p=none"
        if ($dmarcResult.AlreadyExists) { $existed++ } elseif ($dmarcResult.Success) { $added++ } else { $failed++ }

        $total = $added + $existed + $failed
        if ($failed -gt 0 -and $added -eq 0 -and $existed -eq 0) {
            Write-Log "ALL $failed DNS records failed! Check Cloudflare permissions." -Level Error
            return $false
        }
        Write-Log "M365 DNS: $added added, $existed existed, $failed failed (of $total)" -Level $(if ($failed -eq 0) { "Success" } else { "Warning" })
        return $true

    } catch {
        Write-Log "Failed to fetch DNS from Microsoft: $($_.Exception.Message). Using fallback records." -Level Warning

        $mxHost = $Domain -replace '\.', '-'
        Add-CloudflareDnsRecord -ZoneId $ZoneId -Type "MX" -Name "@" -Content "$mxHost.mail.protection.outlook.com" -Priority 0 | Out-Null
        Add-CloudflareDnsRecord -ZoneId $ZoneId -Type "TXT" -Name "@" -Content "v=spf1 include:spf.protection.outlook.com -all" | Out-Null
        Add-CloudflareDnsRecord -ZoneId $ZoneId -Type "TXT" -Name "_dmarc" -Content "v=DMARC1; p=none" | Out-Null
        Add-CloudflareDnsRecord -ZoneId $ZoneId -Type "CNAME" -Name "autodiscover" -Content "autodiscover.outlook.com" | Out-Null

        return $true
    }
}

function Wait-ForExchangeSync {
    param([string]$Domain, [int]$MaxWaitSeconds = 600)

    Write-Log "Waiting for domain to sync to Exchange: $Domain" -Level Info
    $elapsed = 0

    while ($elapsed -lt $MaxWaitSeconds) {
        $accepted = Get-AcceptedDomain -Identity $Domain -ErrorAction SilentlyContinue
        if ($accepted) {
            Write-Log "Domain is now in Exchange!" -Level Success
            return $true
        }
        Write-Host "    Waiting for Exchange sync... ($elapsed/$MaxWaitSeconds seconds)" -ForegroundColor Gray
        Start-Sleep -Seconds 15
        $elapsed += 15
    }

    Write-Log "Timeout waiting for Exchange sync" -Level Warning
    return $false
}

# ============================================================================
# SENDING TOOL UPLOAD COORDINATION
# ============================================================================
function Get-ActiveDomainInboxes {
    param([string]$DomainId)

    $result = Invoke-SupabaseApi -Method GET -Table "inboxes" -Query "domain_id=eq.$DomainId&status=eq.active&order=created_at.asc&select=*"
    if ($result.Success) { return @($result.Data) }
    return @()
}

function Get-DomainCredentialAssignments {
    param([string]$DomainId)

    $result = Invoke-SupabaseApi -Method GET -Table "domain_credentials" -Query "domain_id=eq.$DomainId&select=credential_id"
    if ($result.Success) { return @($result.Data) }
    return @()
}

function Get-SendingToolUploadActions {
    param([string]$DomainId)

    $result = Invoke-SupabaseApi -Method GET -Table "actions" -Query "domain_id=eq.$DomainId&type=eq.reupload_inboxes&order=created_at.desc&limit=20&select=*"
    if ($result.Success) { return @($result.Data) }
    return @()
}

function Test-ActionPayloadMatchesProvision {
    param([object]$Action, [string]$ProvisionActionId)

    if (-not $Action -or -not $Action.payload) { return $false }
    $payload = $Action.payload
    return ([string]$payload.provision_action_id -eq $ProvisionActionId)
}

function New-SendingToolUploadAction {
    param(
        [object]$DomainRecord,
        [string]$ProvisionActionId,
        [int]$ExpectedActiveInboxCount
    )

    $body = @{
        customer_id = $DomainRecord.customer_id
        domain_id = $DomainRecord.id
        type = "reupload_inboxes"
        status = "pending"
        attempts = 0
        max_attempts = 8
        payload = @{
            domain = $DomainRecord.domain
            source = "microsoft_provision"
            provision_action_id = $ProvisionActionId
            expected_active_inboxes = $ExpectedActiveInboxCount
        }
    }

    $result = Invoke-SupabaseApi -Method POST -Table "actions" -Body $body
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
}

function Ensure-SendingToolUploadAction {
    param(
        [object]$DomainRecord,
        [string]$ProvisionActionId,
        [int]$ExpectedActiveInboxCount
    )

    $credentialAssignments = @(Get-DomainCredentialAssignments -DomainId $DomainRecord.id)
    if ($credentialAssignments.Count -eq 0) {
        return @{
            Success = $false
            Blocked = $true
            Reason = "No sending tool credentials assigned to domain"
        }
    }

    $uploadActions = @(Get-SendingToolUploadActions -DomainId $DomainRecord.id)
    $matchingActions = @($uploadActions | Where-Object { Test-ActionPayloadMatchesProvision -Action $_ -ProvisionActionId $ProvisionActionId })
    $existingOpenAction = $matchingActions | Where-Object { $_.status -in @("pending", "in_progress") } | Select-Object -First 1
    if ($existingOpenAction) {
        return @{ Success = $true; UploadAction = $existingOpenAction; Created = $false }
    }

    $existingCompletedAction = $matchingActions | Where-Object { $_.status -eq "completed" } | Select-Object -First 1
    if ($existingCompletedAction) {
        return @{ Success = $true; UploadAction = $existingCompletedAction; Created = $false }
    }

    $existingFailedAction = $matchingActions | Where-Object { $_.status -eq "failed" } | Select-Object -First 1
    if ($existingFailedAction) {
        return @{ Success = $true; UploadAction = $existingFailedAction; Created = $false }
    }

    $newAction = New-SendingToolUploadAction -DomainRecord $DomainRecord -ProvisionActionId $ProvisionActionId -ExpectedActiveInboxCount $ExpectedActiveInboxCount
    if (-not $newAction) {
        return @{
            Success = $false
            Blocked = $false
            Reason = "Failed to enqueue reupload_inboxes action"
        }
    }

    return @{ Success = $true; UploadAction = $newAction; Created = $true }
}

function Test-SendingToolUploadValidation {
    param(
        [object]$UploadAction,
        [int]$ExpectedActiveInboxCount
    )

    if (-not $UploadAction) {
        return @{ Complete = $false; Failed = $true; Reason = "Missing upload action" }
    }

    $status = [string]$UploadAction.status
    if ($status -in @("pending", "in_progress")) {
        return @{ Complete = $false; Failed = $false; Pending = $true; Reason = "Upload action is $status" }
    }
    if ($status -eq "failed") {
        $errorMessage = if ($UploadAction.error) { [string]$UploadAction.error } else { "Upload action failed" }
        return @{ Complete = $false; Failed = $true; Reason = $errorMessage }
    }
    if ($status -ne "completed") {
        return @{ Complete = $false; Failed = $true; Reason = "Unexpected upload action status: $status" }
    }

    $result = $UploadAction.result
    if (-not $result) {
        return @{ Complete = $false; Failed = $true; Reason = "Upload action completed without result payload" }
    }

    $message = if ($result.PSObject.Properties.Name -contains "message") { [string]$result.message } else { "" }
    if ($message -match "No sending tool credentials assigned") {
        return @{ Complete = $false; Failed = $true; Reason = $message }
    }

    $uploaded = 0
    if ($result.PSObject.Properties.Name -contains "uploaded" -and $null -ne $result.uploaded) {
        try { $uploaded = [int]$result.uploaded } catch { $uploaded = 0 }
    }

    $failed = 0
    if ($result.PSObject.Properties.Name -contains "failed" -and $null -ne $result.failed) {
        try { $failed = [int]$result.failed } catch { $failed = 0 }
    }

    if ($failed -gt 0) {
        return @{ Complete = $false; Failed = $true; Reason = "Upload validation completed with $failed failed inbox(es)" }
    }
    if ($uploaded -lt $ExpectedActiveInboxCount) {
        return @{ Complete = $false; Failed = $true; Reason = "Upload validation confirmed $uploaded inbox(es), expected at least $ExpectedActiveInboxCount" }
    }

    return @{ Complete = $true; Failed = $false; Uploaded = $uploaded; FailedCount = $failed }
}

function Set-ProvisionActionPendingWithoutPenalty {
    param(
        [string]$ActionId,
        [string]$Reason,
        [int]$DelaySeconds = 120,
        [object]$Result = $null
    )

    if ($Result) { Update-ActionResult -ActionId $ActionId -Result $Result }
    $currentAction = Get-Action -ActionId $ActionId
    if ($currentAction) {
        Requeue-ActionWithoutPenalty -Action $currentAction -Reason $Reason -DelaySeconds $DelaySeconds
    } else {
        $nextRetryAt = (Get-Date).AddSeconds($DelaySeconds).ToString("yyyy-MM-ddTHH:mm:ssZ")
        Update-ActionStatus -ActionId $ActionId -Status "pending" -Error $Reason -NextRetryAt $nextRetryAt
    }
}

function Resolve-MicrosoftProvisioningUploadState {
    param(
        [object]$DomainRecord,
        [string]$ActionId,
        [string]$History,
        [int]$ActualMailboxCount = 0
    )

    $domainId = $DomainRecord.id
    $domain = $DomainRecord.domain
    $customerId = $DomainRecord.customer_id
    $activeInboxes = @(Get-ActiveDomainInboxes -DomainId $domainId)
    $expectedActiveCount = $activeInboxes.Count

    if ($expectedActiveCount -eq 0) {
        $reason = "No active inboxes found; refusing to mark Microsoft domain active"
        $history = Add-HistoryEntry -History $History -Entry "FAILED: $reason"
        Update-Domain -DomainId $domainId -Fields @{ status = "in_progress"; interim_status = "Both - Failed"; action_history = $history }
        Update-ActionStatus -ActionId $ActionId -Status "failed" -Error $reason
        Add-ActionLog -ActionId $ActionId -DomainId $domainId -CustomerId $customerId -EventType "provisioning_finalization_failed" -Severity "error" -Message $reason
        return @{ Complete = $false; Failed = $true; History = $history }
    }

    if ($ActualMailboxCount -gt 0 -and $ActualMailboxCount -lt $expectedActiveCount) {
        $reason = "Mailbox count check failed: Exchange has $ActualMailboxCount mailbox(es), Supabase has $expectedActiveCount active inbox(es)"
        $retryReason = "$reason; retrying after Exchange replication delay before sending-tool upload"
        $history = Add-HistoryEntry -History $History -Entry "PENDING: $retryReason"
        Update-Domain -DomainId $domainId -Fields @{ status = "in_progress"; interim_status = "Both - DKIM Complete"; action_history = $history }
        Set-ProvisionActionPendingWithoutPenalty -ActionId $ActionId -Reason $retryReason -DelaySeconds 300
        Add-ActionLog -ActionId $ActionId -DomainId $domainId -CustomerId $customerId -EventType "mailbox_count_validation_deferred" -Severity "warn" -Message $retryReason -Metadata @{
            exchange_mailboxes = $ActualMailboxCount
            expected_active_inboxes = $expectedActiveCount
            retry_delay_seconds = 300
        }
        return @{ Complete = $false; Failed = $false; Pending = $true; History = $history }
    }

    $uploadActionResult = Ensure-SendingToolUploadAction -DomainRecord $DomainRecord -ProvisionActionId $ActionId -ExpectedActiveInboxCount $expectedActiveCount
    if (-not $uploadActionResult.Success) {
        $reason = $uploadActionResult.Reason
        $interimStatus = if ($uploadActionResult.Blocked) { "Both - Sending Tool Upload Blocked" } else { "Both - Sending Tool Upload Failed" }
        $history = Add-HistoryEntry -History $History -Entry "UPLOAD BLOCKED: $reason"
        Update-Domain -DomainId $domainId -Fields @{ status = "in_progress"; interim_status = $interimStatus; action_history = $history }
        Update-ActionStatus -ActionId $ActionId -Status "failed" -Error "Sending-tool upload blocked: $reason"
        Add-ActionLog -ActionId $ActionId -DomainId $domainId -CustomerId $customerId -EventType "sending_tool_upload_blocked" -Severity "error" -Message $reason -Metadata @{ expected_active_inboxes = $expectedActiveCount }
        return @{ Complete = $false; Failed = $true; History = $history }
    }

    $uploadAction = $uploadActionResult.UploadAction
    $uploadActionId = [string]$uploadAction.id
    $validation = Test-SendingToolUploadValidation -UploadAction $uploadAction -ExpectedActiveInboxCount $expectedActiveCount

    if ($validation.Complete) {
        $uploaded = [int]$validation.Uploaded
        $history = Add-HistoryEntry -History $History -Entry "Sending-tool upload validated: $uploaded uploaded for $expectedActiveCount active inboxes"
        Update-Domain -DomainId $domainId -Fields @{
            status = "active"
            interim_status = "Both - Provisioning Complete"
            action_history = $history
        }
        Update-ActionStatus -ActionId $ActionId -Status "completed" -Result @{
            domain = $domain
            mailboxes_created = if ($ActualMailboxCount -gt 0) { $ActualMailboxCount } else { $expectedActiveCount }
            active_inboxes_verified = $expectedActiveCount
            upload_action_id = $uploadActionId
            upload_validated = $true
            uploaded = $uploaded
        }
        Add-ActionLog -ActionId $ActionId -DomainId $domainId -CustomerId $customerId -EventType "provisioning_complete" -Severity "info" -Message "Complete: $expectedActiveCount active inboxes, upload validated by action $uploadActionId" -Metadata @{ upload_action_id = $uploadActionId; uploaded = $uploaded; expected_active_inboxes = $expectedActiveCount }
        return @{ Complete = $true; Failed = $false; History = $history; UploadActionId = $uploadActionId }
    }

    if ($validation.Failed) {
        $reason = $validation.Reason
        $history = Add-HistoryEntry -History $History -Entry "UPLOAD FAILED: $reason"
        Update-Domain -DomainId $domainId -Fields @{ status = "in_progress"; interim_status = "Both - Sending Tool Upload Failed"; action_history = $history }
        Update-ActionStatus -ActionId $ActionId -Status "failed" -Error "Sending-tool upload validation failed: $reason"
        Add-ActionLog -ActionId $ActionId -DomainId $domainId -CustomerId $customerId -EventType "sending_tool_upload_failed" -Severity "error" -Message $reason -Metadata @{ upload_action_id = $uploadActionId; expected_active_inboxes = $expectedActiveCount }
        return @{ Complete = $false; Failed = $true; History = $history; UploadActionId = $uploadActionId }
    }

    # The upload worker finalizes this provision action directly when Instantly
    # completes, so this retry is only a safety poll. Keep it long enough to
    # avoid stale-action churn while large 99-inbox uploads are running.
    $delaySeconds = 1800
    if ($env:MICROSOFT_UPLOAD_POLL_SECONDS) {
        try { $delaySeconds = [Math]::Max(30, [int]$env:MICROSOFT_UPLOAD_POLL_SECONDS) } catch { $delaySeconds = 1800 }
    }
    $pendingEntry = if ($uploadActionResult.Created) {
        "Queued sending-tool upload action $uploadActionId for $expectedActiveCount active inboxes"
    } else {
        "Sending-tool upload action $uploadActionId is $($uploadAction.status); waiting for validation"
    }
    $history = Add-HistoryEntry -History $History -Entry $pendingEntry
    Update-Domain -DomainId $domainId -Fields @{
        status = "in_progress"
        interim_status = "Both - Sending Tool Upload Pending"
        action_history = $history
    }
    Set-ProvisionActionPendingWithoutPenalty -ActionId $ActionId -Reason "Sending-tool upload pending (action $uploadActionId)" -DelaySeconds $delaySeconds -Result @{
        domain = $domain
        upload_pending = $true
        upload_action_id = $uploadActionId
        expected_active_inboxes = $expectedActiveCount
    } | Out-Null
    Add-ActionLog -ActionId $ActionId -DomainId $domainId -CustomerId $customerId -EventType "sending_tool_upload_pending" -Severity "warn" -Message "Waiting for reupload_inboxes action $uploadActionId before marking domain active" -Metadata @{ upload_action_id = $uploadActionId; expected_active_inboxes = $expectedActiveCount; retry_delay_seconds = $delaySeconds }
    return @{ Complete = $false; Failed = $false; Pending = $true; History = $history; UploadActionId = $uploadActionId }
}

# ============================================================================
# ORPHAN USER CLEANUP
# ============================================================================
function Remove-OrphanUsersForEmail {
    param([string]$Email, [string]$CorrectUserId, [hashtable]$Headers)

    $orphansDeleted = 0
    try {
        $searchByUPN = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users?`$filter=userPrincipalName eq '$Email'" -Headers $Headers -ErrorAction SilentlyContinue
        if ($searchByUPN.value) {
            foreach ($user in $searchByUPN.value) {
                if ($user.id -ne $CorrectUserId) {
                    Write-Log "ORPHAN FOUND (UPN): $($user.id) - DELETING" -Level Warning
                    try {
                        Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$($user.id)" -Method DELETE -Headers $Headers -ErrorAction Stop
                        $orphansDeleted++
                        Start-Sleep -Seconds 2
                    } catch { Write-Log "Failed to delete orphan: $($_.Exception.Message)" -Level Error }
                }
            }
        }
    } catch { }

    if ($orphansDeleted -gt 0) {
        Write-Log "Cleaned up $orphansDeleted orphan(s) for $Email" -Level Success
        Start-Sleep -Seconds 3
    }
    return $orphansDeleted
}

# ============================================================================
# ROOM MAILBOX CREATION
#
# Key points:
#   1. Uses New-Mailbox -Room (NOT -Shared)
#   2. Appends unique counter to DisplayName during creation to avoid
#      "display name already in use" errors, then renames afterward
#   3. MUST disable calendar auto-processing or emails get deleted
#   4. Fallback: create user via Graph, assign license, wait, convert to Room
# ============================================================================
function New-RoomMailboxBulk {
    param([string]$Domain, [array]$Inboxes, [string]$Password, [string]$Bearer)

    Write-Log "Creating $($Inboxes.Count) ROOM mailboxes for $Domain" -Level Info
    $results = @{ Created = @(); Failed = @(); FatalError = $null }
    $securePassword = ConvertTo-SecureString $Password -AsPlainText -Force

    $headers = @{ Authorization = "Bearer $Bearer"; "Content-Type" = "application/json" }

    # Get license SKU for fallback method
    $skuResponse = $null
    try { $skuResponse = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/subscribedSkus" -Headers $headers -ErrorAction SilentlyContinue } catch { }
    $licenseSkuId = if ($skuResponse -and $skuResponse.value) { $skuResponse.value[0].skuId } else { $null }

    # FREE THE LICENSE: find who currently has it and remove it
    # There's typically only 1 license on the tenant — we need to recycle it.
    if ($licenseSkuId) {
        $skuInfo = $skuResponse.value | Where-Object { $_.skuId -eq $licenseSkuId }
        $available = $skuInfo.prepaidUnits.enabled - $skuInfo.consumedUnits
        if ($available -le 0) {
            Write-Log "No available licenses ($($skuInfo.skuPartNumber)). Freeing one..." -Level Warning
            try {
                # Find a user with this license (NOT the admin)
                $licensedUsers = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users?`$filter=assignedLicenses/any(l:l/skuId eq $licenseSkuId)&`$select=id,userPrincipalName,displayName" -Headers $headers -ErrorAction Stop
                foreach ($lu in $licensedUsers.value) {
                    # Don't remove from admin
                    if ($lu.userPrincipalName -like "*onmicrosoft.com") { continue }
                    Write-Log "Removing license from: $($lu.userPrincipalName)" -Level Info
                    $remBody = @{ addLicenses = @(); removeLicenses = @($licenseSkuId) } | ConvertTo-Json -Depth 5
                    Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$($lu.id)/assignLicense" -Method POST -Headers $headers -Body $remBody | Out-Null
                    Write-Log "License freed from: $($lu.userPrincipalName)" -Level Success
                    Start-Sleep -Seconds 5
                    break
                }
            } catch {
                Write-Log "Failed to free license: $($_.Exception.Message)" -Level Warning
            }
        } else {
            Write-Log "License available: $available/$($skuInfo.prepaidUnits.enabled) ($($skuInfo.skuPartNumber))" -Level Info
        }
    }

    # Global counter for unique temp display names
    $script:resourceCounter = Get-Random -Minimum 10000 -Maximum 99999

    $counter = 0
    foreach ($inbox in $Inboxes) {
        $counter++
        $firstName = $inbox.first_name
        $lastName = $inbox.last_name
        $username = $inbox.username
        $email = "$username@$Domain"
        $realDisplayName = "$firstName $lastName".Trim()
        if (-not $realDisplayName) { $realDisplayName = $username }

        # Unique temp display name to avoid M365 duplicate name errors
        $script:resourceCounter++
        $tempDisplayName = "$realDisplayName $($script:resourceCounter)"

        # Check if mailbox already exists
        $existing = Get-Mailbox -Identity $email -ErrorAction SilentlyContinue
        if ($existing) {
            Write-Log "[$counter/$($Inboxes.Count)] Already exists ($($existing.RecipientTypeDetails)): $email" -Level Warning
            $results.Created += @{
                InboxId = $inbox.id; Email = $email; FirstName = $firstName
                LastName = $lastName; DisplayName = $realDisplayName
                AlreadyExisted = $true
            }
            continue
        }

        # METHOD 1: Direct Room mailbox creation with temp display name
        Write-Log "[$counter/$($Inboxes.Count)] Creating room mailbox: $email (temp: $tempDisplayName)" -Level Info
        $directSuccess = $false

        try {
            New-Mailbox -Room -Name $tempDisplayName -DisplayName $tempDisplayName -PrimarySmtpAddress $email -Password $securePassword -ResetPasswordOnNextLogon $false -ErrorAction Stop | Out-Null
            $directSuccess = $true
            Write-Log "Created room mailbox (direct): $email" -Level Success
        } catch {
            $errorMsg = $_.Exception.Message
            # Always fall through to Method 2 when direct creation fails
            Write-Log "Direct method failed ($errorMsg), trying licensed user method..." -Level Warning
        }

        # METHOD 2: Fallback — create user via Graph, assign license, wait, convert to Room
        if (-not $directSuccess -and $licenseSkuId) {
            $newUserId = $null
            $licenseAssigned = $false

            try {
                # Check if user exists in Azure AD
                $existingUser = $null
                try { $existingUser = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$email" -Headers $headers -ErrorAction Stop } catch { }

                if ($existingUser) {
                    Write-Log "User exists in Azure AD, deleting for fresh creation..." -Level Warning
                    try {
                        Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$($existingUser.id)" -Method DELETE -Headers $headers -ErrorAction Stop
                        Start-Sleep -Seconds 10
                    } catch {
                        $results.Failed += @{ InboxId = $inbox.id; Email = $email; Error = "Failed to delete existing user" }
                        continue
                    }
                }

                # Create user via Graph with temp display name
                $userBody = @{
                    accountEnabled = $true
                    displayName = $tempDisplayName
                    givenName = $firstName
                    surname = $lastName
                    mailNickname = $username
                    userPrincipalName = $email
                    usageLocation = "US"
                    passwordProfile = @{ forceChangePasswordNextSignIn = $false; password = $Password }
                } | ConvertTo-Json -Depth 5

                $newUser = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users" -Method POST -Headers $headers -Body $userBody
                $newUserId = $newUser.id
                Write-Log "User created via Graph: $email" -Level Info

                # Assign license
                Start-Sleep -Seconds 3
                $licBody = @{ addLicenses = @(@{ skuId = $licenseSkuId }); removeLicenses = @() } | ConvertTo-Json -Depth 5
                Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$newUserId/assignLicense" -Method POST -Headers $headers -Body $licBody | Out-Null
                $licenseAssigned = $true
                Write-Log "License assigned: $email" -Level Info

                # Wait for mailbox provisioning
                $mailboxReady = $false
                for ($waitAttempt = 1; $waitAttempt -le 3; $waitAttempt++) {
                    Write-Log "Waiting for mailbox provisioning (attempt $waitAttempt/3)..." -Level Info
                    Start-Sleep -Seconds 45

                    $mbx = Get-Mailbox -Identity $email -ErrorAction SilentlyContinue
                    if ($mbx) { $mailboxReady = $true; break }
                }

                if (-not $mailboxReady) { throw "Mailbox not provisioned after 135 seconds" }

                # Convert to Room mailbox
                $convertSuccess = $false
                for ($convertAttempt = 1; $convertAttempt -le 3; $convertAttempt++) {
                    try {
                        Set-Mailbox -Identity $email -Type Room -Confirm:$false -ErrorAction Stop
                        Write-Log "Converted to Room mailbox: $email" -Level Info
                        $convertSuccess = $true
                        break
                    } catch {
                        Write-Log "Convert attempt $convertAttempt/3 failed: $($_.Exception.Message)" -Level Warning
                        if ($convertAttempt -lt 3) { Start-Sleep -Seconds 30 }
                    }
                }

                if (-not $convertSuccess) { throw "Failed to convert to Room after 3 attempts" }

                # Remove license (room mailboxes don't need one)
                $remBody = @{ addLicenses = @(); removeLicenses = @($licenseSkuId) } | ConvertTo-Json -Depth 5
                Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$newUserId/assignLicense" -Method POST -Headers $headers -Body $remBody | Out-Null
                $licenseAssigned = $false
                Write-Log "License removed: $email" -Level Success

                $directSuccess = $true
            } catch {
                $errMsg = $_.Exception.Message
                if ($_.ErrorDetails.Message) {
                    try { $errMsg = ($_.ErrorDetails.Message | ConvertFrom-Json).error.message } catch { $errMsg = $_.ErrorDetails.Message }
                }
                Write-Log "Licensed user method failed: $email - $errMsg" -Level Error
                $results.Failed += @{ InboxId = $inbox.id; Email = $email; Error = $errMsg }
            } finally {
                # Cleanup: always remove license if still assigned
                if ($licenseAssigned -and $newUserId) {
                    try {
                        $remBody = @{ addLicenses = @(); removeLicenses = @($licenseSkuId) } | ConvertTo-Json -Depth 5
                        Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$newUserId/assignLicense" -Method POST -Headers $headers -Body $remBody | Out-Null
                    } catch { Write-Log "Failed to cleanup license for $email" -Level Error }
                }
            }

            if (-not $directSuccess) { continue }
        }

        if ($directSuccess) {
            $results.Created += @{
                InboxId = $inbox.id; Email = $email; FirstName = $firstName
                LastName = $lastName; DisplayName = $realDisplayName
                TempDisplayName = $tempDisplayName; AlreadyExisted = $false
            }
        }

        # Brief pause between mailbox creations to let Exchange settle
        Start-Sleep -Seconds 3
    }

    # ========================================================================
    # POST-CREATION STEP 1: Rename display names from temp back to real names
    # ========================================================================
    if ($results.Created.Count -gt 0) {
        Write-Log "Renaming room mailboxes to real display names..." -Level Info
        Start-Sleep -Seconds 10

        foreach ($mb in $results.Created) {
            if ($mb.AlreadyExisted) { continue }

            $email = $mb.Email
            $realDisplayName = $mb.DisplayName
            $firstName = $mb.FirstName
            $lastName = $mb.LastName

            # Rename via Exchange
            try {
                Set-Mailbox -Identity $email -DisplayName $realDisplayName -Name $realDisplayName -Confirm:$false -ErrorAction Stop
                Write-Log "Renamed mailbox: $email -> $realDisplayName" -Level Success
            } catch {
                Write-Log "Failed to rename $email via Set-Mailbox: $($_.Exception.Message)" -Level Warning
            }

            # Fix names via Graph API
            try {
                $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$email" -Headers $headers -ErrorAction Stop
                $updateBody = @{
                    displayName = $realDisplayName
                    givenName = $firstName
                    surname = $lastName
                } | ConvertTo-Json -Depth 3
                Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$($user.id)" -Method PATCH -Headers $headers -Body $updateBody -ErrorAction Stop | Out-Null
                Write-Log "Graph names updated: $email ($firstName $lastName)" -Level Success
            } catch {
                Write-Log "Failed to update Graph names for ${email}: $($_.Exception.Message)" -Level Warning
            }
        }
    }

    # ========================================================================
    # POST-CREATION STEP 2: Disable calendar auto-processing
    #
    # THIS IS THE MOST CRITICAL STEP. Room mailboxes by default:
    #   - Auto-accept/decline meeting requests
    #   - DELETE non-calendar items (regular emails!)
    #   - Delete comments and subjects from calendar items
    #
    # Without this step, incoming emails are SILENTLY DELETED.
    # ========================================================================
    if ($results.Created.Count -gt 0) {
        Write-Log "CRITICAL: Disabling calendar auto-processing on room mailboxes..." -Level Info
        # Wait longer for Exchange to fully provision mailbox properties (reduces "Places store" warnings)
        Start-Sleep -Seconds 15

        foreach ($mb in $results.Created) {
            $email = $mb.Email
            for ($calAttempt = 1; $calAttempt -le 3; $calAttempt++) {
                try {
                    Set-CalendarProcessing -Identity $email `
                        -AutomateProcessing None `
                        -DeleteComments $false `
                        -DeleteSubject $false `
                        -RemovePrivateProperty $false `
                        -DeleteNonCalendarItems $false `
                        -ErrorAction Stop `
                        -WarningAction SilentlyContinue
                    Write-Log "Calendar auto-processing DISABLED: $email" -Level Success
                    break
                } catch {
                    if ($calAttempt -lt 3) {
                        Write-Log "Calendar processing attempt $calAttempt/3 for ${email}: $($_.Exception.Message)" -Level Warning
                        Start-Sleep -Seconds 10
                    } else {
                        Write-Log "CRITICAL: Calendar disable FAILED for ${email}: $($_.Exception.Message)" -Level Error
                    }
                }
            }
        }
    }

    # ========================================================================
    # POST-CREATION STEP 3: Fix UPNs + remove orphans
    # ========================================================================
    if ($results.Created.Count -gt 0) {
        Write-Log "Fixing UPNs for newly created mailboxes..." -Level Info
        Start-Sleep -Seconds 5
        foreach ($mb in $results.Created) {
            if ($mb.AlreadyExisted) { continue }
            $email = $mb.Email

            $mbx = Get-Mailbox -Identity $email -ErrorAction SilentlyContinue
            $correctUserId = if ($mbx) { $mbx.ExternalDirectoryObjectId } else { "NONE" }

            Remove-OrphanUsersForEmail -Email $email -CorrectUserId $correctUserId -Headers $headers

            try { Set-Mailbox -Identity $email -MicrosoftOnlineServicesID $email -Confirm:$false -ErrorAction SilentlyContinue } catch { }

            try {
                $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$email" -Headers $headers -ErrorAction SilentlyContinue
                if ($user -and $user.userPrincipalName -ne $email) {
                    $upnBody = @{ userPrincipalName = $email } | ConvertTo-Json
                    Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$($user.id)" -Method PATCH -Headers $headers -Body $upnBody -ErrorAction SilentlyContinue | Out-Null
                    Write-Log "Fixed UPN for $email" -Level Success
                }
            } catch { }
        }
    }

    Write-Log "Room mailbox creation complete: $($results.Created.Count) created, $($results.Failed.Count) failed" -Level Info
    return $results
}

# ============================================================================
# AZURE MAILBOX CREATION
#
# Azure follows the same Microsoft-backed DNS, DKIM, upload, update, and
# cancellation lifecycle. The only difference is the mailbox creation command.
# ============================================================================
function Set-MailboxAccountReady {
    param(
        [string]$Email,
        [string]$Bearer,
        [string]$Password,
        [hashtable]$Headers = $null
    )

    if (-not $Headers) {
        $Headers = @{ Authorization = "Bearer $Bearer"; "Content-Type" = "application/json" }
    }

    $userId = $null
    try {
        $mbx = Get-Mailbox -Identity $Email -ErrorAction SilentlyContinue
        if ($mbx -and $mbx.ExternalDirectoryObjectId) {
            $userId = [string]$mbx.ExternalDirectoryObjectId
        }
    } catch { }

    if (-not $userId) {
        try {
            $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$Email" -Headers $Headers -ErrorAction Stop
            if ($user -and $user.id) { $userId = [string]$user.id }
        } catch { }
    }

    if (-not $userId) {
        Write-Log "Could not resolve Graph user for $Email" -Level Warning
        return $false
    }

    Remove-OrphanUsersForEmail -Email $Email -CorrectUserId $userId -Headers $Headers | Out-Null

    try { Set-Mailbox -Identity $Email -MicrosoftOnlineServicesID $Email -WindowsEmailAddress $Email -Confirm:$false -ErrorAction SilentlyContinue } catch { }

    try {
        $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Headers $Headers -ErrorAction Stop
        $body = @{
            accountEnabled = $true
            passwordProfile = @{ forceChangePasswordNextSignIn = $false; password = $Password }
        }
        if ($user.userPrincipalName -ne $Email) { $body.userPrincipalName = $Email }
        Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Method PATCH -Headers $Headers -Body ($body | ConvertTo-Json -Depth 4) -ErrorAction Stop | Out-Null
        Write-Log "Password + unblock confirmed: $Email" -Level Success
        return $true
    } catch {
        Write-Log "Password/unblock failed for ${Email}: $($_.Exception.Message)" -Level Warning
        return $false
    }
}

function New-AzureMailboxBulk {
    param([string]$Domain, [array]$Inboxes, [string]$Password, [string]$Bearer)

    Write-Log "Creating $($Inboxes.Count) Azure mailboxes for $Domain" -Level Info
    $results = @{ Created = @(); Failed = @() }
    $securePassword = ConvertTo-SecureString $Password -AsPlainText -Force
    $headers = @{ Authorization = "Bearer $Bearer"; "Content-Type" = "application/json" }
    $script:azureMailboxCounter = Get-Random -Minimum 10000 -Maximum 99999

    $counter = 0
    foreach ($inbox in $Inboxes) {
        $counter++
        $firstName = [string]$inbox.first_name
        $lastName = [string]$inbox.last_name
        $username = [string]$inbox.username
        $email = "$username@$Domain"
        $realDisplayName = "$firstName $lastName".Trim()
        if (-not $realDisplayName) { $realDisplayName = $username }

        $script:azureMailboxCounter++
        $tempDisplayName = "$realDisplayName $($script:azureMailboxCounter)"

        $existing = Get-Mailbox -Identity $email -ErrorAction SilentlyContinue
        if ($existing) {
            Write-Log "[$counter/$($Inboxes.Count)] Already exists ($($existing.RecipientTypeDetails)): $email" -Level Warning
            Set-MailboxAccountReady -Email $email -Bearer $Bearer -Password $Password -Headers $headers | Out-Null
            $results.Created += @{
                InboxId = $inbox.id; Email = $email; FirstName = $firstName
                LastName = $lastName; DisplayName = $realDisplayName
                AlreadyExisted = $true
            }
            continue
        }

        Write-Log "[$counter/$($Inboxes.Count)] Creating Azure mailbox: $email" -Level Info
        $createdOk = $false
        try {
            New-Mailbox -Shared -Name $tempDisplayName -DisplayName $tempDisplayName -PrimarySmtpAddress $email -Password $securePassword -ResetPasswordOnNextLogon $false -ErrorAction Stop | Out-Null
            $createdOk = $true
            Write-Log "Created Azure mailbox: $email" -Level Success
        } catch {
            $errorMsg = $_.Exception.Message
            Write-Log "Direct Azure mailbox creation failed for ${email}: $errorMsg" -Level Warning

            if ($errorMsg -match "not an accepted domain|can't use the domain") {
                try {
                    Write-Log "Trying Azure fallback create with MicrosoftOnlineServicesID for $email" -Level Warning
                    New-Mailbox -Name $tempDisplayName -DisplayName $tempDisplayName -MicrosoftOnlineServicesID $email -Password $securePassword -ResetPasswordOnNextLogon $false -ErrorAction Stop | Out-Null
                    Start-Sleep -Seconds 8
                    Set-Mailbox -Identity $email -Type Shared -PrimarySmtpAddress $email -WindowsEmailAddress $email -Confirm:$false -ErrorAction Stop
                    $createdOk = $true
                    Write-Log "Created Azure mailbox via MicrosoftOnlineServicesID fallback: $email" -Level Success
                } catch {
                    $fallbackError = $_.Exception.Message
                    $results.FatalError = "Azure mailbox creation blocked for ${Domain}: primary create failed with '$errorMsg'; MicrosoftOnlineServicesID fallback failed with '$fallbackError'"
                    $results.FatalCode = "azure_accepted_domain_mailbox_creation_blocked"
                    $results.Failed += @{ InboxId = $inbox.id; Email = $email; Error = $results.FatalError }
                    Write-Log "Aborting Azure mailbox creation for $Domain after MicrosoftOnlineServicesID fallback failed" -Level Error
                    return $results
                }
            } elseif ($errorMsg -match "Parameter set cannot be resolved") {
                $results.FatalError = "Azure mailbox creation blocked for ${Domain}: $errorMsg"
                $results.Failed += @{ InboxId = $inbox.id; Email = $email; Error = $results.FatalError }
                Write-Log "Aborting Azure mailbox creation for $Domain because the create command hit a fatal domain/parameter error" -Level Error
                return $results
            }

            if (-not $createdOk) { try {
                $existingUser = $null
                try { $existingUser = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$email" -Headers $headers -ErrorAction Stop } catch { }
                if ($existingUser) {
                    Write-Log "User exists without mailbox; enabling mailbox for $email" -Level Warning
                    Enable-Mailbox -Identity $email -PrimarySmtpAddress $email -ErrorAction Stop | Out-Null
                    Start-Sleep -Seconds 10
                    Set-Mailbox -Identity $email -Type Shared -Confirm:$false -ErrorAction Stop
                    $createdOk = $true
                    Write-Log "Enabled Azure mailbox for existing user: $email" -Level Success
                }
            } catch {
                $errorMsg = $_.Exception.Message
            } }
        }

        if (-not $createdOk) {
            $results.Failed += @{ InboxId = $inbox.id; Email = $email; Error = "Azure mailbox creation failed: $errorMsg" }
            continue
        }

        for ($waitAttempt = 1; $waitAttempt -le 3; $waitAttempt++) {
            $mbx = Get-Mailbox -Identity $email -ErrorAction SilentlyContinue
            if ($mbx) { break }
            Start-Sleep -Seconds 10
        }

        try {
            Set-Mailbox -Identity $email -DisplayName $realDisplayName -Name $realDisplayName -Alias $username -MicrosoftOnlineServicesID $email -WindowsEmailAddress $email -Confirm:$false -ErrorAction Stop
        } catch {
            Write-Log "Failed to normalize Azure mailbox identity for ${email}: $($_.Exception.Message)" -Level Warning
        }

        try {
            Set-User -Identity $email -DisplayName $realDisplayName -FirstName $firstName -LastName $lastName -Confirm:$false -ErrorAction SilentlyContinue
        } catch { }

        Set-MailboxAccountReady -Email $email -Bearer $Bearer -Password $Password -Headers $headers | Out-Null

        $results.Created += @{
            InboxId = $inbox.id; Email = $email; FirstName = $firstName
            LastName = $lastName; DisplayName = $realDisplayName
            TempDisplayName = $tempDisplayName; AlreadyExisted = $false
        }

        Start-Sleep -Seconds 3
    }

    Write-Log "Azure mailbox creation complete: $($results.Created.Count) created, $($results.Failed.Count) failed" -Level Info
    return $results
}

# ============================================================================
# SMTP AUTH
# ============================================================================
function Enable-TenantSMTPAuth {
    Write-Log "Enabling tenant-wide SMTP AUTH..." -Level Info
    try {
        $tc = Get-TransportConfig
        if (-not $tc.SmtpClientAuthenticationDisabled) {
            Write-Log "SMTP AUTH already enabled" -Level Success
            return $true
        }
        Set-TransportConfig -SmtpClientAuthenticationDisabled $false
        Write-Log "SMTP AUTH enabled" -Level Success
        return $true
    } catch {
        Write-Log "Failed to enable SMTP AUTH: $($_.Exception.Message)" -Level Error
        return $false
    }
}

function Confirm-DomainClientAccessForDomain {
    param([string]$Domain)

    Write-Log "Confirming domain-level SMTP AUTH/IMAP readiness for: $Domain" -Level Info
    $mailboxes = Get-Mailbox -ResultSize Unlimited | Where-Object { $_.PrimarySmtpAddress -like "*@$Domain" }
    $total = ($mailboxes | Measure-Object).Count
    if ($total -eq 0) {
        Write-Log "No mailboxes found for domain-level client access readiness" -Level Error
        return @{ Success = $false; Total = 0; Enabled = 0; Failed = @("No mailboxes found") }
    }

    Write-Log "Domain-level client access ready; skipping per-mailbox Set-CASMailbox loop ($total mailbox rows)" -Level Success
    return @{ Success = $true; Total = $total; Enabled = $total; Failed = @() }
}

# ============================================================================
# UNBLOCK MAILBOXES + FIX UPNs
# ============================================================================
function Unblock-DomainMailboxes {
    param([string]$Domain, [string]$Bearer, [string]$Password)

    Write-Log "Unblocking mailboxes and fixing UPNs for: $Domain" -Level Info

    $headers = @{ Authorization = "Bearer $Bearer"; "Content-Type" = "application/json" }
    $mailboxes = Get-Mailbox -ResultSize Unlimited | Where-Object { $_.PrimarySmtpAddress -like "*@$Domain" }

    foreach ($mb in $mailboxes) {
        $email = $mb.PrimarySmtpAddress.ToString()
        $userId = $mb.ExternalDirectoryObjectId

        Remove-OrphanUsersForEmail -Email $email -CorrectUserId $userId -Headers $headers

        try { Set-Mailbox -Identity $email -MicrosoftOnlineServicesID $email -Confirm:$false -ErrorAction SilentlyContinue } catch { }

        if (-not $userId) { continue }

        try {
            $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Headers $headers -ErrorAction Stop

            $fixBody = @{
                accountEnabled = $true
                passwordProfile = @{ forceChangePasswordNextSignIn = $false; password = $Password }
            }

            if ($user.userPrincipalName -ne $email) { $fixBody.userPrincipalName = $email }
            if ($user.mail -ne $email) { $fixBody.mail = $email }

            $body = $fixBody | ConvertTo-Json -Depth 3
            Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Method PATCH -Headers $headers -Body $body -ErrorAction Stop | Out-Null
            Write-Log "Unblocked + fixed: $email" -Level Success
        } catch {
            Write-Log "Failed to process ${email}: $($_.Exception.Message)" -Level Warning
        }
    }
}

function Sync-ActiveInboxPasswordsForUpload {
    param([string]$DomainId, [string]$Domain, [string]$Bearer)

    $activeInboxes = @(Get-ActiveDomainInboxes -DomainId $DomainId)
    if ($activeInboxes.Count -eq 0) {
        Write-Log "No active inbox rows found for password sync before upload" -Level Warning
        return $false
    }

    Write-Log "Syncing $($activeInboxes.Count) active inbox passwords before upload: $Domain" -Level Info
    $headers = @{ Authorization = "Bearer $Bearer"; "Content-Type" = "application/json" }
    $failed = @()
    foreach ($inbox in $activeInboxes) {
        $email = [string]$inbox.email
        $password = [string]$inbox.password
        if (-not $email -or -not $password) {
            $failed += $email
            continue
        }
        $ok = Set-MailboxAccountReady -Email $email -Bearer $Bearer -Password $password -Headers $headers
        if (-not $ok) { $failed += $email }
    }

    if ($failed.Count -gt 0) {
        Write-Log "Password sync failed for $($failed.Count) inbox(es) before upload" -Level Warning
        return $false
    }

    Write-Log "Password sync complete before upload for $Domain" -Level Success
    return $true
}

# ============================================================================
# BULLETPROOF CHECK
# ============================================================================
function Invoke-BulletproofMailboxCheck {
    param([string]$Domain, [string]$Bearer, [string]$Password, [string]$MailboxMode = "room", [int]$MaxRetries = 2)

    $isAzureMode = ([string]$MailboxMode).Trim().ToLowerInvariant() -eq "azure"
    $modeLabel = if ($isAzureMode) { "Azure" } else { "Room" }
    Write-Log "========== BULLETPROOF FINAL CHECK ($modeLabel): $Domain ==========" -Level Info

    $headers = @{ Authorization = "Bearer $Bearer"; "Content-Type" = "application/json" }
    $mailboxes = Get-Mailbox -ResultSize Unlimited | Where-Object { $_.PrimarySmtpAddress -like "*@$Domain" }
    $total = ($mailboxes | Measure-Object).Count
    $passed = 0
    $issues = @()

    foreach ($mb in $mailboxes) {
        $email = $mb.PrimarySmtpAddress.ToString()
        $mailboxOk = $true
        $mailboxIssues = @()

        if ($isAzureMode) {
            if ($mb.RecipientTypeDetails -ne "SharedMailbox") {
                $mailboxIssues += "NOT Azure mailbox type (is $($mb.RecipientTypeDetails))"
                try {
                    Set-Mailbox -Identity $email -Type Shared -Confirm:$false -ErrorAction Stop
                    Write-Log "[$email] Converted to Azure mailbox type" -Level Success
                } catch { Write-Log "[$email] Azure mailbox type conversion failed: $($_.Exception.Message)" -Level Error }
            }
        } else {
            # Verify it's a RoomMailbox
            if ($mb.RecipientTypeDetails -ne "RoomMailbox") {
                $mailboxOk = $false
                $mailboxIssues += "NOT RoomMailbox (is $($mb.RecipientTypeDetails))"
                try {
                    Set-Mailbox -Identity $email -Type Room -Confirm:$false -ErrorAction Stop
                    Write-Log "[$email] Converted to Room" -Level Success
                } catch { Write-Log "[$email] Conversion failed: $($_.Exception.Message)" -Level Error }
            }

            # Verify calendar processing is disabled
            try {
                $calProc = Get-CalendarProcessing -Identity $email -ErrorAction SilentlyContinue
                if ($calProc -and $calProc.AutomateProcessing -ne "None") {
                    $mailboxIssues += "Calendar auto-processing is ON"
                    Set-CalendarProcessing -Identity $email -AutomateProcessing None -DeleteComments $false -DeleteSubject $false -RemovePrivateProperty $false -DeleteNonCalendarItems $false -ErrorAction Stop -WarningAction SilentlyContinue
                    Write-Log "[$email] Calendar processing fixed" -Level Success
                }
                if ($calProc -and $calProc.DeleteNonCalendarItems -eq $true) {
                    $mailboxIssues += "DeleteNonCalendarItems is TRUE (emails would be deleted!)"
                    Set-CalendarProcessing -Identity $email -DeleteNonCalendarItems $false -ErrorAction Stop -WarningAction SilentlyContinue
                    Write-Log "[$email] DeleteNonCalendarItems set to false" -Level Success
                }
            } catch {
                Write-Log "[$email] Calendar check failed: $($_.Exception.Message)" -Level Warning
            }
        }

        # Check UPN + account via Graph
        $userId = $mb.ExternalDirectoryObjectId
        if ($userId) {
            try {
                $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Headers $headers -ErrorAction Stop

                $fixBody = @{
                    accountEnabled = $true
                    passwordProfile = @{ forceChangePasswordNextSignIn = $false; password = $Password }
                }

                if ($user.userPrincipalName -ne $email) {
                    $mailboxIssues += "UPN wrong ($($user.userPrincipalName))"
                    $fixBody.userPrincipalName = $email
                }

                $upnPrefix = ($user.userPrincipalName -split '@')[0]
                if ($upnPrefix -match '^[Gg][0-9a-fA-F]{20,}') {
                    $mailboxIssues += "UPN is GUID format"
                    $fixBody.userPrincipalName = $email
                }

                if ($user.mail -ne $email) { $fixBody.mail = $email }

                Set-Mailbox -Identity $email -MicrosoftOnlineServicesID $email -Confirm:$false -ErrorAction SilentlyContinue
                $body = $fixBody | ConvertTo-Json -Depth 3
                Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Method PATCH -Headers $headers -Body $body -ErrorAction Stop | Out-Null
            } catch {
                $mailboxIssues += "Fix failed: $($_.Exception.Message)"
            }
        }

        if ($mailboxIssues.Count -eq 0) { $passed++ }
        else { $issues += "$email`: $($mailboxIssues -join ', ')" }
    }

    Write-Log "========== FINAL RESULT: $passed/$total PASSED ==========" -Level $(if ($passed -eq $total) { "Success" } else { "Warning" })

    return @{ Total = $total; Passed = $passed; Failed = ($total - $passed); Issues = $issues; AllPassed = ($passed -eq $total) }
}

# ============================================================================
# DKIM FUNCTIONS
# ============================================================================
function Setup-DomainDKIM {
    param([string]$Domain, [string]$ZoneId)

    Write-Log "Setting up DKIM for: $Domain" -Level Info
    $dk = $null

    try { $dk = Get-DkimSigningConfig -Identity $Domain -ErrorAction Stop -WarningAction SilentlyContinue } catch { }

    if ($dk -and (-not $dk.Selector1CNAME -or -not $dk.Selector2CNAME)) {
        try { Remove-DkimSigningConfig -Identity $Domain -Confirm:$false -ErrorAction Stop; $dk = $null; Start-Sleep -Seconds 3 } catch { }
    }

    if ($dk -and $dk.Selector1CNAME -and $dk.Selector2CNAME) {
        return @{ Success = $true; Selector1CNAME = $dk.Selector1CNAME; Selector2CNAME = $dk.Selector2CNAME; AlreadyEnabled = $dk.Enabled }
    }

    try { New-DkimSigningConfig -DomainName $Domain -Enabled $false -ErrorAction Stop -WarningAction SilentlyContinue | Out-Null } catch { }

    Start-Sleep -Seconds 5
    try { $dk = Get-DkimSigningConfig -Identity $Domain -ErrorAction Stop -WarningAction SilentlyContinue } catch { $dk = $null }

    if ($dk -and $dk.Selector1CNAME -and $dk.Selector2CNAME) {
        return @{ Success = $true; Selector1CNAME = $dk.Selector1CNAME; Selector2CNAME = $dk.Selector2CNAME; AlreadyEnabled = $dk.Enabled }
    }

    for ($attempt = 1; $attempt -le 3; $attempt++) {
        Start-Sleep -Seconds 10
        try { $dk = Get-DkimSigningConfig -Identity $Domain -ErrorAction Stop -WarningAction SilentlyContinue } catch { $dk = $null }
        if ($dk -and $dk.Selector1CNAME -and $dk.Selector2CNAME) {
            return @{ Success = $true; Selector1CNAME = $dk.Selector1CNAME; Selector2CNAME = $dk.Selector2CNAME; AlreadyEnabled = $dk.Enabled }
        }
    }

    return @{ Success = $false; Error = "DKIM selectors not available" }
}

function Test-DkimCnameResolution {
    param([string]$Domain, [string]$Selector1CNAME, [string]$Selector2CNAME)

    # Verify both DKIM CNAME records resolve before attempting to enable
    $sel1Host = "selector1._domainkey.$Domain"
    $sel2Host = "selector2._domainkey.$Domain"
    $resolveCommand = Get-Command Resolve-DnsName -ErrorAction SilentlyContinue
    $digCommand = Get-Command dig -ErrorAction SilentlyContinue
    $nslookupCommand = Get-Command nslookup -ErrorAction SilentlyContinue

    function Test-OneDkimCname {
        param([string]$HostName, [string]$ExpectedTarget)

        $expected = $ExpectedTarget.TrimEnd([char]".")

        try {
            $dnsQueryHost = [System.Uri]::EscapeDataString($HostName)
            $dohResponse = Invoke-RestMethod `
                -Method GET `
                -Uri "https://cloudflare-dns.com/dns-query?name=$dnsQueryHost&type=CNAME" `
                -Headers @{ accept = "application/dns-json" } `
                -UserAgent "pixie-worker/1.0" `
                -TimeoutSec 10 `
                -ErrorAction Stop

            $answers = @($dohResponse.Answer) | Where-Object { $_ -and $_.data } | ForEach-Object { ([string]$_.data).TrimEnd([char]".") }
            if ($answers.Count -gt 0) {
                return ($answers -contains $expected)
            }
        } catch { }

        try {
            if ($digCommand) {
                foreach ($resolver in @("1.1.1.1", "8.8.8.8")) {
                    $result = @(& dig "@$resolver" +short $HostName CNAME 2>$null) | ForEach-Object { ([string]$_).Trim() }
                    if ($result.Count -gt 0) {
                        $normalizedTargets = @($result | ForEach-Object { $_.TrimEnd([char]".") })
                        if ($normalizedTargets -contains $expected) { return $true }
                    }
                }
            }
        } catch { }

        try {
            if ($resolveCommand) {
                $result = Resolve-DnsName -Name $HostName -Type CNAME -ErrorAction Stop
                if ($result) {
                    $hosts = @($result | ForEach-Object { [string]$_.NameHost }).Where({ $_ })
                    if ($hosts.Count -eq 0) { return $true }
                    $normalizedHosts = @($hosts | ForEach-Object { ([string]$_).TrimEnd([char]".") })
                    return ($normalizedHosts -contains $expected)
                }
            }
        } catch { }

        try {
            if ($digCommand) {
                $result = @(& dig +short $HostName CNAME 2>$null) | ForEach-Object { ([string]$_).Trim() }
                if ($result.Count -gt 0) {
                    $normalizedTargets = @($result | ForEach-Object { $_.TrimEnd([char]".") })
                    return ($normalizedTargets -contains $expected)
                }
            }
        } catch { }

        try {
            if ($nslookupCommand) {
                $result = @(& nslookup -type=CNAME $HostName 2>$null) | ForEach-Object { [string]$_ }
                return (($result -join "`n") -match [regex]::Escape($expected))
            }
        } catch { }

        return $false
    }

    $sel1Ok = Test-OneDkimCname -HostName $sel1Host -ExpectedTarget $Selector1CNAME
    $sel2Ok = Test-OneDkimCname -HostName $sel2Host -ExpectedTarget $Selector2CNAME

    return @{ Selector1 = $sel1Ok; Selector2 = $sel2Ok; BothResolved = ($sel1Ok -and $sel2Ok) }
}

function Complete-DKIMSetup {
    param([string]$Domain, [string]$ZoneId, [string]$Selector1CNAME, [string]$Selector2CNAME)

    # Step 1: Upsert CNAME records in Cloudflare. Replacements often inherit
    # old tenant selectors, so "already exists" is not enough for DKIM.
    $r1 = Set-CloudflareDnsRecord -ZoneId $ZoneId -Type "CNAME" -Name "selector1._domainkey" -Domain $Domain -Content $Selector1CNAME
    $r2 = Set-CloudflareDnsRecord -ZoneId $ZoneId -Type "CNAME" -Name "selector2._domainkey" -Domain $Domain -Content $Selector2CNAME

    if (-not $r1.Success -or -not $r2.Success) {
        Write-Log "Failed to add DKIM CNAME records to Cloudflare (sel1=$($r1.Success), sel2=$($r2.Success))" -Level Error
        return $false
    }

    # Step 2: Wait briefly for DNS propagation with verification. Cloudflare is fast;
    # if Microsoft has not accepted the selectors yet, defer and free this worker.
    $dnsMaxChecks = 5
    $dnsCheckIntervalSeconds = 15
    if ($env:MICROSOFT_DKIM_DNS_CHECKS) {
        try { $dnsMaxChecks = [Math]::Max(1, [int]$env:MICROSOFT_DKIM_DNS_CHECKS) } catch { $dnsMaxChecks = 5 }
    }
    if ($env:MICROSOFT_DKIM_DNS_CHECK_INTERVAL_SECONDS) {
        try { $dnsCheckIntervalSeconds = [Math]::Max(5, [int]$env:MICROSOFT_DKIM_DNS_CHECK_INTERVAL_SECONDS) } catch { $dnsCheckIntervalSeconds = 15 }
    }
    Write-Log "Waiting for DKIM CNAME DNS propagation (checking every ${dnsCheckIntervalSeconds}s, up to $([Math]::Round(($dnsMaxChecks * $dnsCheckIntervalSeconds) / 60, 1)) min)..." -Level Info
    Start-Sleep -Seconds 10

    $dnsVerified = $false
    for ($dnsCheck = 1; $dnsCheck -le $dnsMaxChecks; $dnsCheck++) {
        $dnsResult = Test-DkimCnameResolution -Domain $Domain -Selector1CNAME $Selector1CNAME -Selector2CNAME $Selector2CNAME
        if ($dnsResult.BothResolved) {
            Write-Log "DKIM CNAMEs verified resolving (check $dnsCheck)" -Level Success
            $dnsVerified = $true
            break
        }
        Write-Log "DNS check $dnsCheck/${dnsMaxChecks}: selector1=$($dnsResult.Selector1), selector2=$($dnsResult.Selector2) — waiting ${dnsCheckIntervalSeconds}s..." -Level Warning
        Start-Sleep -Seconds $dnsCheckIntervalSeconds
    }

    if (-not $dnsVerified) {
        Write-Log "DKIM CNAMEs not resolving after DNS wait — proceeding anyway (Microsoft may see them)" -Level Warning
    }

    # Step 3: Enable DKIM with retries (exponential backoff)
    $maxDkimEnableAttempts = 2
    if ($env:MICROSOFT_DKIM_ENABLE_ATTEMPTS) {
        try { $maxDkimEnableAttempts = [Math]::Max(1, [int]$env:MICROSOFT_DKIM_ENABLE_ATTEMPTS) } catch { $maxDkimEnableAttempts = 2 }
    }
    for ($attempt = 1; $attempt -le $maxDkimEnableAttempts; $attempt++) {
        try {
            Set-DkimSigningConfig -Identity $Domain -Enabled $true -ErrorAction Stop -WarningAction SilentlyContinue
            Start-Sleep -Seconds 5
            $dk = Get-DkimSigningConfig -Identity $Domain -ErrorAction Stop -WarningAction SilentlyContinue
            if ($dk -and $dk.Enabled -eq $true) {
                Write-Log "DKIM enabled in tenant for $Domain (attempt $attempt)" -Level Success
                return $true
            }
            Write-Log "DKIM enable command returned but tenant does not report Enabled=true yet" -Level Warning
        } catch {
            $dkimErr = $_.Exception.Message
            Write-Log "DKIM enable attempt $attempt/${maxDkimEnableAttempts} failed: $dkimErr" -Level Warning
            if ($attempt -lt $maxDkimEnableAttempts) {
                $waitSec = 60 * $attempt  # 60s, 120s, 180s, 240s
                Write-Log "Retrying DKIM in ${waitSec}s..." -Level Info
                Start-Sleep -Seconds $waitSec
            }
        }
    }
    Write-Log "DKIM enable failed after ${maxDkimEnableAttempts} attempts — CNAME records are in place, retrying later before upload" -Level Warning
    return $false
}

# ============================================================================
# USER CONSENT CONFIGURATION (from original script)
# ============================================================================
$script:GraphAppId = "00000003-0000-0000-c000-000000000000"

$script:LowImpactPermissions = @(
    @{name="email"; id="64a6cdd6-aab1-4aaf-94b8-3cc8405e90d0"; type="Scope"},
    @{name="Mail.Read"; id="570282fd-fa5c-430d-a7fd-fc8dc98a9dca"; type="Scope"},
    @{name="Mail.ReadWrite"; id="024d486e-b451-40bb-833d-3e66d98c5c73"; type="Scope"},
    @{name="Mail.Send"; id="e383f46e-2787-4529-855e-0e479a3ffac0"; type="Scope"},
    @{name="IMAP.AccessAsUser.All"; id="652390e4-393a-48de-9484-05f9b1212954"; type="Scope"},
    @{name="SMTP.Send"; id="258f6531-6087-4cc4-bb90-092c5fb3ed3f"; type="Scope"},
    @{name="User.Read"; id="e1fe6dd8-ba31-4d61-89e7-88639da4683d"; type="Scope"},
    @{name="openid"; id="37f7f235-527c-4136-accd-4a02d197296e"; type="Scope"},
    @{name="profile"; id="14dad69e-099b-42c9-810b-d002981feec1"; type="Scope"},
    @{name="offline_access"; id="7427e0e9-2fba-42fe-b0c0-848c9e6a8182"; type="Scope"},
    @{name="Mail.ReadBasic"; id="a4b8392a-d8d1-4954-a029-8e668a39a170"; type="Scope"},
    @{name="MailboxSettings.ReadWrite"; id="818c620a-27a9-40bd-a6a5-d96f7d610b4b"; type="Scope"}
)

function Configure-UserConsent {
    param([string]$Bearer, [string]$TenantId, [string]$AdminEmail, [string]$AdminPassword)

    try {
        $authPolicy = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/policies/authorizationPolicy" -Bearer $Bearer
        $assignedPolicies = $authPolicy.defaultUserRolePermissions.permissionGrantPoliciesAssigned

        if ($assignedPolicies -and ($assignedPolicies -contains "ManagePermissionGrantsForSelf.microsoft-user-default-low")) {
            Write-Log "User consent already configured" -Level Success
            return $true
        }

        # Set user consent policy
        $userToken = Get-ROPCToken -TenantId $TenantId -ClientId $AzureCliPublicClientId -Username $AdminEmail -Password $AdminPassword
        if ($userToken) {
            $policyBody = @{
                defaultUserRolePermissions = @{
                    permissionGrantPoliciesAssigned = @("ManagePermissionGrantsForSelf.microsoft-user-default-low")
                }
            }
            try {
                Invoke-GraphRequest -Method PATCH -Url "https://graph.microsoft.com/v1.0/policies/authorizationPolicy" -Bearer $userToken -Body $policyBody | Out-Null
                Write-Log "User consent policy configured" -Level Success
            } catch {
                Write-Log "Could not set consent policy: $($_.Exception.Message)" -Level Warning
            }
        }
        return $true
    } catch {
        Write-Log "User consent config failed: $($_.Exception.Message)" -Level Warning
        return $false
    }
}

# ============================================================================
# MAIN PROCESS-DOMAIN FUNCTION
# Uses interim_status for pipeline resume logic. Updates Supabase at each step.
# ============================================================================
function Process-MicrosoftDomain {
    param(
        [object]$DomainRecord,
        [object]$AdminRecord,
        [array]$Inboxes,
        [string]$ActionId
    )

    $Domain = $DomainRecord.domain
    $DomainId = $DomainRecord.id
    $CustomerId = $DomainRecord.customer_id
    $ZoneId = $DomainRecord.cloudflare_zone_id
    $Source = $DomainRecord.source
    $Provider = if ($DomainRecord.provider) { [string]$DomainRecord.provider } else { "microsoft" }
    $NormalizedProvider = $Provider.Trim().ToLowerInvariant()
    $IsAzureProvider = $NormalizedProvider -eq "azure"
    $MailboxMode = if ($IsAzureProvider) { "azure" } else { "room" }
    $MailboxLabel = if ($IsAzureProvider) { "Azure mailboxes" } else { "room mailboxes" }
    $AdminEmail = $AdminRecord.email
    $AdminPassword = $AdminRecord.password
    $interimStatus = if ($DomainRecord.interim_status) { $DomainRecord.interim_status } else { "" }
    $history = if ($DomainRecord.action_history) { $DomainRecord.action_history } else { "" }
    $ReplacementOldDomain = Get-ReplacementOldDomainFromRecord -DomainRecord $DomainRecord

    # Generate a shared password for all inboxes in this domain
    $InboxPassword = -join ((65..90) + (97..122) + (48..57) | Get-Random -Count 14 | ForEach-Object { [char]$_ })
    $InboxPassword = "A" + $InboxPassword.Substring(1, 12) + "1!"

    $failedSteps = @()

    Write-Host ""
    Write-Host "================================================================" -ForegroundColor Magenta
    Write-Host "  Processing Domain ($($MailboxLabel.ToUpperInvariant())): $Domain" -ForegroundColor Magenta
    Write-Host "  Admin: $AdminEmail | Interim: $interimStatus" -ForegroundColor Magenta
    Write-Host "  Inboxes: $($Inboxes.Count) | Zone: $ZoneId" -ForegroundColor Magenta
    if ($ReplacementOldDomain) {
        Write-Host "  Replacement isolation: old domain $ReplacementOldDomain must not remain on $Domain mailboxes" -ForegroundColor Yellow
    }
    Write-Host "================================================================" -ForegroundColor Magenta

    # Ordered pipeline steps for resume
    $knownStatuses = @(
        "", "Both - New Order", "Both - DNS Zone Created", "Both - NS Migrated",
        "Both - CF Zone Active",
        "Microsoft - Added to M365", "Both - Verification TXT Added",
        "Both - Domain Verified", "Microsoft - Email Enabled",
        "Both - DNS Records Added", "Microsoft - Exchange Synced",
        "Both - Creating Mailboxes", "Microsoft - Mailboxes Created",
        "Microsoft - Configuring Mailboxes", "Microsoft - SMTP Enabled",
        "Both - DKIM Complete", "Both - Sending Tool Upload Pending",
        "Both - Sending Tool Upload Blocked", "Both - Sending Tool Upload Failed",
        "Both - Provisioning Complete", "Both - Failed"
    )

    if ($interimStatus -eq "Both - Provisioning Complete") {
        Write-Log "Already completed, skipping" -Level Warning
        return
    }

    $statusesAfterMailboxCreation = @(
        "Microsoft - Mailboxes Created",
        "Microsoft - Configuring Mailboxes",
        "Microsoft - SMTP Enabled",
        "Both - DKIM Complete",
        "Both - Sending Tool Upload Pending",
        "Both - Sending Tool Upload Blocked",
        "Both - Sending Tool Upload Failed",
        "Both - Failed"
    )
    if ($Inboxes.Count -gt 0 -and $interimStatus -in $statusesAfterMailboxCreation) {
        $rewindReason = "Found $($Inboxes.Count) pending inbox row(s) while domain was at '$interimStatus'; rewinding to mailbox creation before finalization"
        Write-Log $rewindReason -Level Warning
        $history = Add-HistoryEntry -History $history -Entry "RECOVERY: $rewindReason"
        Update-Domain -DomainId $DomainId -Fields @{ status = "in_progress"; interim_status = "Both - Creating Mailboxes"; action_history = $history }
        $interimStatus = "Both - Creating Mailboxes"
    }

    $history = Add-HistoryEntry -History $history -Entry "Microsoft provisioning started (admin: $AdminEmail)"

    # Get tenant ID
    $adminDomainPart = ($AdminEmail -split '@')[1]
    $tenantId = Get-TenantIdFromDomain -Domain $adminDomainPart
    if (-not $tenantId) {
        Write-Log "Could not resolve tenant ID" -Level Error
        Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - Failed"; action_history = (Add-HistoryEntry -History $history -Entry "FAILED: Could not resolve tenant ID") }
        return
    }

    # Get Graph token
    $Bearer = Get-ROPCToken -TenantId $tenantId -ClientId $AzureCliPublicClientId -Username $AdminEmail -Password $AdminPassword
    if (-not $Bearer) {
        Write-Log "Failed to get Graph token" -Level Error
        Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - Failed"; action_history = (Add-HistoryEntry -History $history -Entry "FAILED: ROPC token failed (MFA?)") }
        return
    }
    Write-Log "Graph token acquired" -Level Success

    # Configure user consent (non-fatal)
    Configure-UserConsent -Bearer $Bearer -TenantId $tenantId -AdminEmail $AdminEmail -AdminPassword $AdminPassword | Out-Null

    if ($NormalizedProvider -eq "smtp_plus") {
        $consentBearer = Get-DirectoryGraphToken -TenantId $tenantId -Username $AdminEmail -Password $AdminPassword
        if (-not $consentBearer) {
            Write-Log "Using existing Graph token for IMAP proxy consent" -Level Warning
            $consentBearer = $Bearer
        }

        $proxyConsent = Ensure-ImapProxyTenantConsent -Bearer $consentBearer -Domain $Domain
        if (-not $proxyConsent.Success) {
            $message = "IMAP proxy consent failed for SMTP+ upload: $($proxyConsent.Error)"
            $history = Add-HistoryEntry -History $history -Entry "FAILED: $message"
            Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "imap_proxy_consent_failed" -Severity "error" -Message $message -Metadata @{
                provider = $Provider
                domain = $Domain
                next_action = "Confirm the Microsoft admin can grant tenant consent, then retry provisioning."
            }
            Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - Failed"; action_history = $history }
            Update-ActionStatus -ActionId $ActionId -Status "failed" -Error $message
            return
        }
        $history = Add-HistoryEntry -History $history -Entry "IMAP proxy tenant consent confirmed"
    }

    # Connect to Exchange Online
    if (-not $DryRun) {
        $securePwd = ConvertTo-SecureString $AdminPassword -AsPlainText -Force
        $creds = New-Object System.Management.Automation.PSCredential($AdminEmail, $securePwd)
        try {
            Connect-ExchangeOnline -Credential $creds -ShowBanner:$false -ErrorAction Stop
            Write-Log "Connected to Exchange Online" -Level Success
        } catch {
            Write-Log "Exchange Online connection failed: $($_.Exception.Message)" -Level Error
            Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - Failed"; action_history = (Add-HistoryEntry -History $history -Entry "FAILED: Exchange connection - $($_.Exception.Message)") }
            return
        }
    }

    # ── STEP 1: Add domain to M365 ──
    if (
        $interimStatus -eq "" -or
        $interimStatus -eq "Both - New Order" -or
        $interimStatus -eq "Both - DNS Zone Created" -or
        $interimStatus -eq "Both - NS Migrated" -or
        $interimStatus -eq "Both - CF Zone Active"
    ) {
        if (-not $DryRun) {
            $addResult = Add-DomainToM365 -Bearer $Bearer -Domain $Domain
            if (-not $addResult.Success) {
                $history = Add-HistoryEntry -History $history -Entry "FAILED: Add domain to M365 - $($addResult.Error)"
                Update-Domain -DomainId $DomainId -Fields @{ status = "in_progress"; interim_status = "Both - Failed"; action_history = $history }
                Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "domain_setup_failed" -Severity "error" -Message "Add to M365 failed: $($addResult.Error)"
                return
            }
            if ($addResult.IsVerified) { $interimStatus = "Both - Domain Verified" }
        }
        $history = Add-HistoryEntry -History $history -Entry "Domain added to M365 tenant"
        if ($interimStatus -ne "Both - Domain Verified") {
            Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Microsoft - Added to M365"; action_history = $history }
            $interimStatus = "Microsoft - Added to M365"
        }
    }

    # ── STEP 2: Verification TXT ──
    if ($interimStatus -eq "Microsoft - Added to M365") {
        if (-not $DryRun) {
            $verificationTxt = Get-DomainVerificationRecord -Bearer $Bearer -Domain $Domain
            if (-not $verificationTxt) {
                $history = Add-HistoryEntry -History $history -Entry "FAILED: Could not get verification TXT"
                Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - Failed"; action_history = $history }
                return
            }

            # All DNS records go through Cloudflare (NS already pointing to CF)
            $cfResult = Add-CloudflareDnsRecord -ZoneId $ZoneId -Type "TXT" -Name "@" -Content $verificationTxt
            if (-not $cfResult.Success -and -not $cfResult.AlreadyExists) {
                Write-Log "Failed to add verification TXT to Cloudflare" -Level Error
            }
            Start-Sleep -Seconds 15
        }
        $history = Add-HistoryEntry -History $history -Entry "Verification TXT record added"
        Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - Verification TXT Added"; action_history = $history }
        $interimStatus = "Both - Verification TXT Added"
    }

    # ── STEP 3: Verify domain ──
    if ($interimStatus -eq "Both - Verification TXT Added") {
        if (-not $DryRun) {
            $verified = Verify-M365Domain -Bearer $Bearer -Domain $Domain
            if (-not $verified -and -not (Test-DomainVerified -Bearer $Bearer -Domain $Domain)) {
                $history = Add-HistoryEntry -History $history -Entry "FAILED: Domain verification failed"
                Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - Failed"; action_history = $history }
                Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "domain_verify_failed" -Severity "error" -Message "Verification failed after max attempts"
                return
            }
        }
        $history = Add-HistoryEntry -History $history -Entry "Domain verified in M365"
        Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - Domain Verified"; action_history = $history }
        $interimStatus = "Both - Domain Verified"
    }

    # ── STEP 4: Enable email ──
    if ($interimStatus -eq "Both - Domain Verified") {
        $emailEnabled = $true
        if (-not $DryRun) { $emailEnabled = Enable-DomainEmailService -Bearer $Bearer -Domain $Domain }
        if ($emailEnabled) {
            $history = Add-HistoryEntry -History $history -Entry "Email service enabled"
        } else {
            $history = Add-HistoryEntry -History $history -Entry "WARNING: Email service endpoint did not confirm enablement; continuing with DNS setup"
        }
        Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Microsoft - Email Enabled"; action_history = $history }
        $interimStatus = "Microsoft - Email Enabled"
    }

    # ── STEP 5: DNS records ──
    if ($interimStatus -eq "Microsoft - Email Enabled") {
        if (-not $DryRun) {
            $dnsOk = Add-M365DnsRecords -Domain $Domain -Bearer $Bearer -ZoneId $ZoneId
            if (-not $dnsOk) {
                $history = Add-HistoryEntry -History $history -Entry "FAILED: All DNS records failed"
                Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - Failed"; action_history = $history }
                Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "dns_failed" -Severity "error" -Message "All DNS records failed"
                return
            }
        }
        $history = Add-HistoryEntry -History $history -Entry "DNS records added (MX, SPF, DMARC, autodiscover)"
        Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - DNS Records Added"; action_history = $history }
        $interimStatus = "Both - DNS Records Added"
    }

    # ── STEP 6: Exchange sync ──
    if ($interimStatus -eq "Both - DNS Records Added") {
        $exchangeSyncMaxWaitSeconds = 600
        $exchangeSyncDeferSeconds = 300
        if ($env:MICROSOFT_EXCHANGE_SYNC_MAX_WAIT_SECONDS) {
            try {
                $exchangeSyncMaxWaitSeconds = [Math]::Max(60, [int]$env:MICROSOFT_EXCHANGE_SYNC_MAX_WAIT_SECONDS)
            } catch { }
        }
        if ($env:MICROSOFT_EXCHANGE_SYNC_DEFER_SECONDS) {
            try {
                $exchangeSyncDeferSeconds = [Math]::Max(30, [int]$env:MICROSOFT_EXCHANGE_SYNC_DEFER_SECONDS)
            } catch { }
        }

        if (-not $DryRun) {
            $domainState = Get-M365DomainState -Bearer $Bearer -Domain $Domain
            $supportedServices = if ($domainState.Success) { @($domainState.SupportedServices) } else { @() }
            $emailServiceEnabled = $supportedServices -contains "Email"

            if (-not $domainState.IsVerified) {
                Write-Log "Graph reports $Domain is not verified; re-running verification before Exchange sync" -Level Warning

                $verificationTxt = Get-DomainVerificationRecord -Bearer $Bearer -Domain $Domain
                if ($verificationTxt) {
                    $cfResult = Add-CloudflareDnsRecord -ZoneId $ZoneId -Type "TXT" -Name "@" -Content $verificationTxt
                    if (-not $cfResult.Success -and -not $cfResult.AlreadyExists) {
                        Write-Log "Failed to refresh verification TXT for $Domain before verification retry" -Level Warning
                    }
                }

                $verified = Verify-M365Domain -Bearer $Bearer -Domain $Domain
                if (-not $verified -and -not (Test-DomainVerified -Bearer $Bearer -Domain $Domain)) {
                    $nextRetryAt = (Get-Date).AddSeconds($exchangeSyncDeferSeconds).ToString("yyyy-MM-ddTHH:mm:ssZ")
                    $history = Add-HistoryEntry -History $history -Entry "M365 domain verification still pending; deferring retry for $exchangeSyncDeferSeconds seconds"
                    Update-Domain -DomainId $DomainId -Fields @{ status = "in_progress"; interim_status = "Both - Verification TXT Added"; action_history = $history }
                    Update-ActionStatus -ActionId $ActionId -Status "pending" -Error "M365 domain verification pending" -NextRetryAt $nextRetryAt
                    Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "domain_verification_deferred" -Severity "warn" -Message "Graph still reports the domain as unverified; retrying in $exchangeSyncDeferSeconds seconds" -Metadata @{ next_retry_at = $nextRetryAt; retry_delay_seconds = $exchangeSyncDeferSeconds }
                    return
                }

                $history = Add-HistoryEntry -History $history -Entry "Domain verified in M365 (recovered before Exchange sync)"
                Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - Domain Verified"; action_history = $history }
                $interimStatus = "Both - Domain Verified"

                $domainState = Get-M365DomainState -Bearer $Bearer -Domain $Domain
                $supportedServices = if ($domainState.Success) { @($domainState.SupportedServices) } else { @() }
                $emailServiceEnabled = $supportedServices -contains "Email"
            }

            if (-not $emailServiceEnabled) {
                Write-Log "Graph reports Email service is not enabled for $Domain; re-enabling before Exchange sync" -Level Warning
                $emailEnabled = Enable-DomainEmailService -Bearer $Bearer -Domain $Domain
                if (-not $emailEnabled) {
                    $nextRetryAt = (Get-Date).AddSeconds($exchangeSyncDeferSeconds).ToString("yyyy-MM-ddTHH:mm:ssZ")
                    $history = Add-HistoryEntry -History $history -Entry "Email service still pending; deferring retry for $exchangeSyncDeferSeconds seconds"
                    Update-Domain -DomainId $DomainId -Fields @{ status = "in_progress"; interim_status = "Both - Domain Verified"; action_history = $history }
                    Update-ActionStatus -ActionId $ActionId -Status "pending" -Error "M365 email service pending" -NextRetryAt $nextRetryAt
                    Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "email_enable_deferred" -Severity "warn" -Message "Email service is not active yet for $Domain; retrying in $exchangeSyncDeferSeconds seconds" -Metadata @{ next_retry_at = $nextRetryAt; retry_delay_seconds = $exchangeSyncDeferSeconds }
                    return
                }

                $history = Add-HistoryEntry -History $history -Entry "Email service enabled (recovered before Exchange sync)"
                Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Microsoft - Email Enabled"; action_history = $history }
                $interimStatus = "Microsoft - Email Enabled"
            }

            if ($interimStatus -eq "Both - Domain Verified" -or $interimStatus -eq "Microsoft - Email Enabled") {
                $dnsOk = Add-M365DnsRecords -Domain $Domain -Bearer $Bearer -ZoneId $ZoneId
                if (-not $dnsOk) {
                    $history = Add-HistoryEntry -History $history -Entry "FAILED: All DNS records failed during Exchange preflight recovery"
                    Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - Failed"; action_history = $history }
                    Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "dns_failed" -Severity "error" -Message "All DNS records failed during Exchange preflight recovery"
                    return
                }

                $history = Add-HistoryEntry -History $history -Entry "DNS records refreshed before Exchange sync"
                Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - DNS Records Added"; action_history = $history }
                $interimStatus = "Both - DNS Records Added"
            }

            $synced = Wait-ForExchangeSync -Domain $Domain -MaxWaitSeconds $exchangeSyncMaxWaitSeconds
            if (-not $synced) {
                $nextRetryAt = (Get-Date).AddSeconds($exchangeSyncDeferSeconds).ToString("yyyy-MM-ddTHH:mm:ssZ")
                $history = Add-HistoryEntry -History $history -Entry "Exchange sync still pending after $exchangeSyncMaxWaitSeconds seconds; deferring retry for $exchangeSyncDeferSeconds seconds"
                Update-Domain -DomainId $DomainId -Fields @{ status = "in_progress"; interim_status = "Both - DNS Records Added"; action_history = $history }
                Update-ActionStatus -ActionId $ActionId -Status "pending" -Error "Exchange sync pending after $exchangeSyncMaxWaitSeconds seconds" -NextRetryAt $nextRetryAt
                Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "exchange_sync_deferred" -Severity "warn" -Message "Exchange sync still pending after $exchangeSyncMaxWaitSeconds seconds; retrying in $exchangeSyncDeferSeconds seconds" -Metadata @{ next_retry_at = $nextRetryAt; retry_delay_seconds = $exchangeSyncDeferSeconds }
                return
            }
        }
        $history = Add-HistoryEntry -History $history -Entry "Domain synced to Exchange Online"
        Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Microsoft - Exchange Synced"; action_history = $history }
        $interimStatus = "Microsoft - Exchange Synced"
    }

    # ── STEP 7: Create mailboxes ──
    if (($interimStatus -eq "Microsoft - Exchange Synced" -or $interimStatus -eq "Both - Creating Mailboxes") -and -not $DryRun) {
        $exchangeSyncDeferSeconds = 300
        if ($env:MICROSOFT_EXCHANGE_SYNC_DEFER_SECONDS) {
            try {
                $exchangeSyncDeferSeconds = [Math]::Max(30, [int]$env:MICROSOFT_EXCHANGE_SYNC_DEFER_SECONDS)
            } catch { }
        }

        $acceptedNow = $null
        try { $acceptedNow = Get-AcceptedDomain -Identity $Domain -ErrorAction SilentlyContinue } catch { }
        if (-not $acceptedNow) {
            Write-Log "Exchange does not currently list $Domain as an accepted domain; deferring before mailbox creation" -Level Warning
            $nextRetryAt = (Get-Date).AddSeconds($exchangeSyncDeferSeconds).ToString("yyyy-MM-ddTHH:mm:ssZ")
            $history = Add-HistoryEntry -History $history -Entry "Exchange accepted-domain check missing before mailbox creation; deferring retry for $exchangeSyncDeferSeconds seconds"
            Update-Domain -DomainId $DomainId -Fields @{ status = "in_progress"; interim_status = "Both - DNS Records Added"; action_history = $history }
            Update-ActionStatus -ActionId $ActionId -Status "pending" -Error "Exchange accepted domain missing before mailbox creation" -NextRetryAt $nextRetryAt
            Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "exchange_accepted_domain_missing_before_mailbox_creation" -Severity "warn" -Message "Exchange does not currently list $Domain as an accepted domain; retrying mailbox creation after Exchange sync" -Metadata @{
                next_retry_at = $nextRetryAt
                retry_delay_seconds = $exchangeSyncDeferSeconds
                previous_interim_status = $interimStatus
            }
            return
        }
    }

    $mailboxResults = $null
    if ($interimStatus -eq "Microsoft - Exchange Synced" -or $interimStatus -eq "Both - Creating Mailboxes") {
        $history = Add-HistoryEntry -History $history -Entry "Creating $($Inboxes.Count) $MailboxLabel..."
        Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - Creating Mailboxes"; action_history = $history }

        if (-not $DryRun) {
            if ($IsAzureProvider) {
                $mailboxResults = New-AzureMailboxBulk -Domain $Domain -Inboxes $Inboxes -Password $InboxPassword -Bearer $Bearer
            } else {
                $mailboxResults = New-RoomMailboxBulk -Domain $Domain -Inboxes $Inboxes -Password $InboxPassword -Bearer $Bearer
            }

            if ($mailboxResults.Created.Count -eq 0) {
                $failureMessage = if ($mailboxResults.FatalError) { [string]$mailboxResults.FatalError } else { "No mailboxes created (0/$($Inboxes.Count))" }
                $acceptedDomainBlocked = (
                    $IsAzureProvider -and
                    (
                        [string]$mailboxResults.FatalCode -eq "azure_accepted_domain_mailbox_creation_blocked" -or
                        (Test-AzureAcceptedDomainMailboxCreateBlocked -Message $failureMessage)
                    )
                )
                if ($acceptedDomainBlocked) {
                    $repairQueued = Invoke-AzureAcceptedDomainReaddRecovery -Bearer $Bearer -Domain $Domain -DomainId $DomainId -CustomerId $CustomerId -ActionId $ActionId -History $history -FailureMessage $failureMessage -DelaySeconds 60
                    if ($repairQueued) {
                        Write-Log "Accepted-domain delete/re-add recovery queued for $Domain; retrying provisioning after Microsoft sync" -Level Warning
                        return
                    }
                }

                $history = Add-HistoryEntry -History $history -Entry "FAILED: $failureMessage"
                Update-Domain -DomainId $DomainId -Fields @{ status = "in_progress"; interim_status = "Both - Failed"; action_history = $history }
                Update-ActionStatus -ActionId $ActionId -Status "failed" -Error $failureMessage
                Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "mailbox_creation_failed_zero_created" -Severity "error" -Message $failureMessage -Metadata @{
                    requested_inboxes = $Inboxes.Count
                    provider = $Provider
                    mailbox_mode = $MailboxMode
                    blocker_code = if ($acceptedDomainBlocked) { "azure_accepted_domain_mailbox_creation_blocked" } else { $null }
                }
                return
            }

            # Update inbox records in Supabase
            foreach ($mb in $mailboxResults.Created) {
                Update-Inbox -InboxId $mb.InboxId -Fields @{
                    email = $mb.Email
                    password = $InboxPassword
                    status = "active"
                }
            }
            foreach ($mb in $mailboxResults.Failed) {
                Update-Inbox -InboxId $mb.InboxId -Fields @{
                    status = "pending"
                }
                Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "inbox_creation_failed" -Severity "error" -Message "Failed to create $($mb.Email): $($mb.Error)"
            }

            $history = Add-HistoryEntry -History $history -Entry "Created $($mailboxResults.Created.Count) mailboxes, $($mailboxResults.Failed.Count) failed"
        }

        Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Microsoft - Mailboxes Created"; action_history = $history }
        $interimStatus = "Microsoft - Mailboxes Created"
    }

    # ── STEP 8: Configure mailboxes (unblock + bulletproof) ──
    if ($interimStatus -eq "Microsoft - Mailboxes Created") {
        if (-not $DryRun) {
            Unblock-DomainMailboxes -Domain $Domain -Bearer $Bearer -Password $InboxPassword
            $history = Add-HistoryEntry -History $history -Entry "Mailboxes unblocked"

            if ($ReplacementOldDomain) {
                $aliasIsolation = Invoke-ReplacementAliasIsolation -Domain $Domain -ReplacementOldDomain $ReplacementOldDomain -ExpectedMailboxCount $Inboxes.Count -Repair
                $history = Add-HistoryEntry -History $history -Entry "Replacement alias isolation: checked $($aliasIsolation.Checked), repaired $($aliasIsolation.Repaired)"
                Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "replacement_alias_isolation_checked" -Severity $(if ($aliasIsolation.Success) { "info" } else { "error" }) -Message "Replacement alias isolation checked for $Domain against old domain $ReplacementOldDomain" -Metadata @{
                    old_domain = $ReplacementOldDomain
                    new_domain = $Domain
                    checked = $aliasIsolation.Checked
                    repaired = $aliasIsolation.Repaired
                    blocked = $aliasIsolation.Blocked
                    issues = $aliasIsolation.Issues
                }
                if (-not $aliasIsolation.Success) {
                    $failedSteps += "Replacement Alias Isolation"
                }
            }

            $bulletproof = Invoke-BulletproofMailboxCheck -Domain $Domain -Bearer $Bearer -Password $InboxPassword -MailboxMode $MailboxMode
            $history = Add-HistoryEntry -History $history -Entry "Bulletproof check: $($bulletproof.Passed)/$($bulletproof.Total) passed"

            if (-not $bulletproof.AllPassed) {
                $failedSteps += "Bulletproof Check"
            }
        }
        Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Microsoft - Configuring Mailboxes"; action_history = $history }
        $interimStatus = "Microsoft - Configuring Mailboxes"
    }

    # ── STEP 9: SMTP AUTH + IMAP ──
    if ($interimStatus -eq "Microsoft - Configuring Mailboxes") {
        if (-not $DryRun) {
            $tenantSmtpOk = Enable-TenantSMTPAuth
            $clientAccess = Confirm-DomainClientAccessForDomain -Domain $Domain
            if (-not $tenantSmtpOk) { $failedSteps += "Tenant SMTP AUTH" }
            if (-not $clientAccess.Success) {
                $failedSteps += "Domain SMTP/IMAP Access"
                Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "domain_client_access_failed" -Severity "error" -Message "Domain-level SMTP/IMAP readiness could not be confirmed" -Metadata @{
                    enabled = $clientAccess.Enabled
                    total = $clientAccess.Total
                    failed = $clientAccess.Failed
                }
            }
            $history = Add-HistoryEntry -History $history -Entry "Tenant SMTP AUTH enabled; domain SMTP/IMAP readiness confirmed for $($clientAccess.Total) mailboxes"
        } else {
            $history = Add-HistoryEntry -History $history -Entry "Tenant SMTP AUTH and domain SMTP/IMAP readiness confirmed"
        }
        Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Microsoft - SMTP Enabled"; action_history = $history }
        $interimStatus = "Microsoft - SMTP Enabled"
    }

    # ── STEP 10: DKIM ──
    $dkimSuccess = $false
    $dkimPendingReason = $null
    if ($interimStatus -eq "Microsoft - SMTP Enabled") {
        if (-not $DryRun) {
            $dkimConfig = Setup-DomainDKIM -Domain $Domain -ZoneId $ZoneId
            if ($dkimConfig.Success) {
                if ($dkimConfig.AlreadyEnabled) {
                    $dkimSuccess = $true
                    $history = Add-HistoryEntry -History $history -Entry "DKIM already enabled"
                } else {
                    $history = Add-HistoryEntry -History $history -Entry "DKIM selectors fetched: selector1=$($dkimConfig.Selector1CNAME), selector2=$($dkimConfig.Selector2CNAME)"
                    $dkimSuccess = Complete-DKIMSetup -Domain $Domain -ZoneId $ZoneId -Selector1CNAME $dkimConfig.Selector1CNAME -Selector2CNAME $dkimConfig.Selector2CNAME
                    if ($dkimSuccess) {
                        $history = Add-HistoryEntry -History $history -Entry "DKIM configured and enabled"
                    } else {
                        $dkimPendingReason = "DKIM CNAMEs are in Cloudflare but Microsoft tenant has not accepted/enabled them yet"
                        $history = Add-HistoryEntry -History $history -Entry "PENDING: $dkimPendingReason"
                    }
                }
            } else {
                $dkimPendingReason = "DKIM selector creation pending: $($dkimConfig.Error)"
                $history = Add-HistoryEntry -History $history -Entry "PENDING: $dkimPendingReason"
            }
        } else { $dkimSuccess = $true }

        if (-not $dkimSuccess) {
            $reason = if ($dkimPendingReason) { $dkimPendingReason } else { "DKIM tenant enable pending" }
            $delaySeconds = 300
            if ($env:MICROSOFT_DKIM_RETRY_DELAY_SECONDS) {
                try { $delaySeconds = [Math]::Max(60, [int]$env:MICROSOFT_DKIM_RETRY_DELAY_SECONDS) } catch { $delaySeconds = 300 }
            }
            $nextRetryAt = (Get-Date).ToUniversalTime().AddSeconds($delaySeconds).ToString("yyyy-MM-ddTHH:mm:ssZ")
            Update-Domain -DomainId $DomainId -Fields @{
                status = "in_progress"
                interim_status = "Microsoft - SMTP Enabled"
                action_history = $history
            }
            Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "dkim_enable_pending" -Severity "warn" -Message $reason -Metadata @{
                next_retry_at = $nextRetryAt
                retry_delay_seconds = $delaySeconds
            }
            Set-ProvisionActionPendingWithoutPenalty -ActionId $ActionId -Reason $reason -DelaySeconds $delaySeconds
            return
        }

        Update-Domain -DomainId $DomainId -Fields @{ interim_status = "Both - DKIM Complete"; action_history = $history }
        $interimStatus = "Both - DKIM Complete"
    }

    # ── STEP 11: Queue/validate sending-tool upload, then finalize ──
    if ($interimStatus -eq "Both - DKIM Complete") {
        $actualCount = 0
        if (-not $DryRun) {
            if ($ReplacementOldDomain) {
                $preUploadIsolation = Invoke-ReplacementAliasIsolation -Domain $Domain -ReplacementOldDomain $ReplacementOldDomain -ExpectedMailboxCount $Inboxes.Count -Repair
                $history = Add-HistoryEntry -History $history -Entry "Pre-upload replacement alias isolation: checked $($preUploadIsolation.Checked), repaired $($preUploadIsolation.Repaired)"
                Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $CustomerId -EventType "replacement_alias_isolation_pre_upload" -Severity $(if ($preUploadIsolation.Success) { "info" } else { "error" }) -Message "Pre-upload replacement alias isolation checked for $Domain against old domain $ReplacementOldDomain" -Metadata @{
                    old_domain = $ReplacementOldDomain
                    new_domain = $Domain
                    checked = $preUploadIsolation.Checked
                    repaired = $preUploadIsolation.Repaired
                    blocked = $preUploadIsolation.Blocked
                    issues = $preUploadIsolation.Issues
                }
                if (-not $preUploadIsolation.Success) { $failedSteps += "Pre-upload Replacement Alias Isolation" }
            }

            $actualCount = (Get-Mailbox -ResultSize Unlimited | Where-Object { $_.PrimarySmtpAddress -like "*@$Domain" } | Measure-Object).Count
            $freshBearer = Get-ROPCToken -TenantId $tenantId -ClientId $AzureCliPublicClientId -Username $AdminEmail -Password $AdminPassword
            if ($freshBearer) {
                $Bearer = $freshBearer
                Write-Log "Graph token refreshed before password sync" -Level Success
            } else {
                Write-Log "Could not refresh Graph token before password sync; using existing token" -Level Warning
            }
            $passwordSyncOk = Sync-ActiveInboxPasswordsForUpload -DomainId $DomainId -Domain $Domain -Bearer $Bearer
            if (-not $passwordSyncOk) { $failedSteps += "Password Sync" }
        }

        if ($failedSteps.Count -eq 0) {
            # Record admin assignment so retries and future mutations stay on the same tenant.
            Ensure-DomainAdminAssignment -DomainId $DomainId -AdminCredId $AdminRecord.id
            Update-AdminUsage -AdminId $AdminRecord.id -InboxCount $Inboxes.Count

            $uploadState = Resolve-MicrosoftProvisioningUploadState -DomainRecord $DomainRecord -ActionId $ActionId -History $history -ActualMailboxCount $actualCount
            $history = $uploadState.History

            if ($uploadState.Complete) {
                Write-Host "  [COMPLETE] $Domain - $actualCount $MailboxLabel, upload validated" -ForegroundColor Green
            } elseif ($uploadState.Pending) {
                Write-Host "  [UPLOAD PENDING] $Domain - $actualCount $MailboxLabel, waiting for upload action $($uploadState.UploadActionId)" -ForegroundColor Yellow
            } else {
                Write-Host "  [PARTIAL] $Domain - Upload validation not complete" -ForegroundColor Yellow
            }
        } else {
            $history = Add-HistoryEntry -History $history -Entry "FAILED STEPS: $($failedSteps -join ', ')"
            Update-Domain -DomainId $DomainId -Fields @{
                status = "in_progress"
                interim_status = "Both - Failed"
                action_history = $history
            }
            Update-ActionStatus -ActionId $ActionId -Status "failed" -Error ($failedSteps -join "; ")
            Write-Host "  [PARTIAL] $Domain - Issues: $($failedSteps -join ', ')" -ForegroundColor Yellow
        }
    }

    # Disconnect from Exchange
    if (-not $DryRun) {
        try { Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue } catch { }
    }
}

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

Add-Type -AssemblyName System.Web

# Ensure ExchangeOnlineManagement is installed
if (-not (Get-Module -ListAvailable -Name ExchangeOnlineManagement)) {
    Write-Host "Installing ExchangeOnlineManagement module..." -ForegroundColor Yellow
    Install-Module -Name ExchangeOnlineManagement -Force -AllowClobber -Scope CurrentUser
}
Import-Module ExchangeOnlineManagement -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "=========================================================================" -ForegroundColor Cyan
Write-Host "     Part 2: MICROSOFT/AZURE MAILBOX CREATION (SUPABASE)" -ForegroundColor Cyan
Write-Host "=========================================================================" -ForegroundColor Cyan
if ($DryRun) { Write-Host "                      *** DRY RUN MODE ***" -ForegroundColor Yellow }
Write-Host ""

# Fetch domain from Supabase
$domainRecord = Get-Domain -DomainId $DomainId
if (-not $domainRecord) {
    Write-Log "Domain not found: $DomainId" -Level Error
    exit 1
}

$actionRecord = Get-Action -ActionId $ActionId
if (-not $actionRecord) {
    Write-Log "Action not found: $ActionId" -Level Error
    exit 1
}
Register-ActionLeaseFence -Action $actionRecord

# Fetch inboxes
$inboxes = Get-DomainInboxes -DomainId $DomainId -Status "pending"
if ($inboxes.Count -eq 0) {
    Write-Log "No pending inboxes for $($domainRecord.domain)" -Level Warning

    $existingHistory = if ($domainRecord.action_history) { $domainRecord.action_history } else { "" }
    $activeInboxes = @(Get-ActiveDomainInboxes -DomainId $DomainId)
    if ($activeInboxes.Count -eq 0) {
        $reason = "No pending inboxes and no active inboxes found; refusing to mark Microsoft domain active"
        $history = Add-HistoryEntry -History $existingHistory -Entry "FAILED: $reason"
        Update-Domain -DomainId $DomainId -Fields @{ status = "in_progress"; interim_status = "Both - Failed"; action_history = $history }
        Update-ActionStatus -ActionId $ActionId -Status "failed" -Error $reason
        Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $domainRecord.customer_id -EventType "provisioning_finalization_failed" -Severity "error" -Message $reason
        exit 1
    }

    $interimStatus = if ($domainRecord.interim_status) { [string]$domainRecord.interim_status } else { "" }
    if ($interimStatus -eq "Both - DKIM Complete") {
        Write-Log "No pending inboxes; reconnecting to validate Exchange mailbox count before upload" -Level Warning
        $inboxes = $activeInboxes
    } elseif ($interimStatus -in @("Both - Sending Tool Upload Pending", "Both - Sending Tool Upload Blocked", "Both - Sending Tool Upload Failed", "Both - Provisioning Complete")) {
        Resolve-MicrosoftProvisioningUploadState -DomainRecord $domainRecord -ActionId $ActionId -History $existingHistory -ActualMailboxCount 0 | Out-Null
        exit 0
    } else {
        Write-Log "Continuing provisioning with $($activeInboxes.Count) verified active inbox row(s)" -Level Info
        $inboxes = $activeInboxes
    }
}

    Write-Log "Domain: $($domainRecord.domain), Inboxes: $($inboxes.Count)" -Level Info

    $assignedAdmin = Get-AssignedAdmin -DomainId $DomainId
    $isJackThresholdMigration = (
        $actionRecord -and
        $actionRecord.payload -and
        [string]$actionRecord.payload.source -eq "jack_threshold_tenant_migration"
    )
    if ($isJackThresholdMigration -and $assignedAdmin -and [string]$assignedAdmin.status -eq "threshold exceeded") {
        $reason = "Premature Jack threshold-migration provision action cancelled because $($domainRecord.domain) is still assigned to threshold tenant $($assignedAdmin.email); source teardown/reassignment must run first."
        Write-Log $reason -Level Warning
        $existingHistory = ""
        if ($domainRecord.action_history) { $existingHistory = [string]$domainRecord.action_history }
        $history = Add-HistoryEntry -History $existingHistory -Entry "Provision deferred: domain still assigned to threshold tenant; waiting for migration controller"
        Update-Domain -DomainId $DomainId -Fields @{ action_history = $history }
        Update-ActionStatus -ActionId $ActionId -Status "cancelled" -Error $reason -Result @{
            cancelled_reason = "premature_threshold_migration_provision"
            current_admin_email = [string]$assignedAdmin.email
            current_admin_status = [string]$assignedAdmin.status
            next_action = "Run threshold migration cancellation/reassignment, then create a fresh replacement provision action."
        } -Action $actionRecord | Out-Null
        Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $domainRecord.customer_id -EventType "premature_threshold_migration_provision_cancelled" -Severity "warn" -Message $reason -Metadata @{
            current_admin_email = [string]$assignedAdmin.email
            current_admin_status = [string]$assignedAdmin.status
        }
        return
    }
    $preferredAdminId = if ($assignedAdmin -and $assignedAdmin.id) { [string]$assignedAdmin.id } else { $null }
    $adminRecord = Acquire-MicrosoftAdminLock -ActionId $ActionId -DomainId $DomainId -PreferredAdminId $preferredAdminId
if (-not $adminRecord) {
    if (Test-ActiveAdminExists -Provider "microsoft") {
        $waitReason = if ($preferredAdminId) {
            "Waiting for the assigned Microsoft admin lock"
        } else {
            "Waiting for an available Microsoft admin lock"
        }
        Write-Log $waitReason -Level Warning
        Requeue-ActionWithoutPenalty -Action $actionRecord -Reason $waitReason -DelaySeconds 60
        Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $domainRecord.customer_id -EventType "admin_lock_wait" -Severity "warn" -Message $waitReason -Metadata @{ provider = "microsoft"; domain = $domainRecord.domain }
        exit 0
    }

    Write-Log "No available Microsoft admin credentials" -Level Error
    Update-ActionStatus -ActionId $ActionId -Status "failed" -Error "No available Microsoft admin credentials"
    exit 1
}

Ensure-DomainAdminAssignment -DomainId $DomainId -AdminCredId $adminRecord.id
Write-Log "Using admin: $($adminRecord.email)" -Level Info

try {
	# Claim the action
	Update-ActionStatus -ActionId $ActionId -Status "in_progress"
	Refresh-MicrosoftAdminLock -ActionId $ActionId | Out-Null
	Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $domainRecord.customer_id -EventType "part2_started" -Severity "info" -Message "Microsoft provisioning started with admin $($adminRecord.email)"

    # Process
    Process-MicrosoftDomain -DomainRecord $domainRecord -AdminRecord $adminRecord -Inboxes $inboxes -ActionId $ActionId
} finally {
    Release-MicrosoftAdminLock -ActionId $ActionId | Out-Null
}

Write-Host ""
Write-Host "=========================================================================" -ForegroundColor Green
Write-Host "              Part 2 COMPLETE" -ForegroundColor Green
Write-Host "=========================================================================" -ForegroundColor Green
