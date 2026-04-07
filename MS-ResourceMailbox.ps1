<#
.SYNOPSIS
  MS-ResourceMailbox.ps1 - Bulk creates M365 domains and resource mailboxes with Airtable tracking
.DESCRIPTION
  Complete M365 domain and resource mailbox provisioning with Airtable for status tracking.
  Based on MS-BulkWithAirtable.ps1 (shared mailbox script) with key differences:
    1. Creates Room resource mailboxes instead of shared mailboxes
    2. Appends a number to DisplayName during creation to avoid duplicate name conflicts,
       then renames to the real first/last name after creation
    3. Disables calendar auto-processing on each resource mailbox
  Includes: Email service enablement, dual mailbox creation, UPN updates, SMTP AUTH, DKIM
.PARAMETER OutputFolder
  Folder for local error logs (default: .\output)
.PARAMETER DryRun
  Run without making actual changes
.PARAMETER MaxDomains
  Limit number of domains to process (0 = all)
.EXAMPLE
  .\MS-ResourceMailbox.ps1
  .\MS-ResourceMailbox.ps1 -DryRun
  .\MS-ResourceMailbox.ps1 -MaxDomains 5
#>

param(
    [string]$OutputFolder = ".\output",
    [switch]$DryRun,
    [int]$MaxDomains = 0,
    [int]$DelayBetweenMailboxesMs = 500,
    [int]$DelayBetweenDomainsMs = 2000
)

# ============================================================================
# AIRTABLE CONFIGURATION
# ============================================================================
$AirtableConfig = @{
    ApiKey         = $env:AIRTABLE_API_KEY  # Set via environment variable
    BaseId         = "appcwfRT57924pTxF"
    AdminsTableId  = "MS Admins"
    DomainsTableId = "Orders"
}

# ============================================================================
# DNSIMPLE CONFIGURATION
# ============================================================================
$DNSimpleConfig = @{
    ApiToken  = $env:DNSIMPLE_API_TOKEN  # Set via environment variable
    AccountId = "151782"
    BaseUrl   = "https://api.dnsimple.com/v2"
}

$AzureCliPublicClientId = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"

# ============================================================================
# LOGGING FUNCTIONS
# ============================================================================
function Write-Log {
    param(
        [string]$Message,
        [ValidateSet("Info","Success","Warning","Error")][string]$Level = "Info"
    )
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $colors = @{ "Info" = "Cyan"; "Success" = "Green"; "Warning" = "Yellow"; "Error" = "Red" }
    $prefix = @{ "Info" = "-->"; "Success" = "[OK]"; "Warning" = "[WARN]"; "Error" = "[ERR]" }
    Write-Host "[$timestamp] $($prefix[$Level]) $Message" -ForegroundColor $colors[$Level]
}

# ============================================================================
# AIRTABLE API FUNCTIONS
# ============================================================================
function Invoke-AirtableApi {
    param(
        [string]$Method,
        [string]$TableId,
        [string]$RecordId = "",
        [object]$Body = $null,
        [string]$Query = ""
    )

    $headers = @{
        "Authorization" = "Bearer $($AirtableConfig.ApiKey)"
        "Content-Type"  = "application/json"
    }

    $encodedTableId = [System.Web.HttpUtility]::UrlEncode($TableId)
    $url = "https://api.airtable.com/v0/$($AirtableConfig.BaseId)/$encodedTableId"
    if ($RecordId) { $url += "/$RecordId" }
    if ($Query) { $url += "?$Query" }

    $params = @{
        Method      = $Method
        Uri         = $url
        Headers     = $headers
        TimeoutSec  = 60
        ErrorAction = "Stop"
    }

    if ($Body) {
        $params.Body = $Body | ConvertTo-Json -Depth 10 -Compress
    }

    try {
        $response = Invoke-RestMethod @params
        return @{ Success = $true; Data = $response }
    } catch {
        $errorMsg = $_.Exception.Message
        if ($_.ErrorDetails.Message) {
            try {
                $errorDetails = $_.ErrorDetails.Message | ConvertFrom-Json
                $errorMsg = $errorDetails.error.message
            } catch {
                $errorMsg = $_.ErrorDetails.Message
            }
        }
        Write-Log "Airtable API error: $errorMsg" -Level Error
        return @{ Success = $false; Error = $errorMsg }
    }
}

function Get-AirtableAdmins {
    Write-Log "Fetching admins from Airtable..." -Level Info
    $admins = @{}
    $offset = $null

    do {
        $query = if ($offset) { "offset=$offset" } else { "" }
        $result = Invoke-AirtableApi -Method GET -TableId $AirtableConfig.AdminsTableId -Query $query

        if ($result.Success) {
            foreach ($record in $result.Data.records) {
                $admins[$record.id] = @{
                    RecordId       = $record.id
                    AdminEmail     = $record.fields.'Admin Email'
                    AdminPassword  = $record.fields.'Admin Password'
                    TenantId       = $record.fields.'Tenant ID'
                    TenantStatus   = $record.fields.'Tenant Status'
                    ProcessingLock = if ($record.fields.'Processing Lock') { $true } else { $false }
                }
            }
            $offset = $result.Data.offset
        } else {
            break
        }
    } while ($offset)

    Write-Log "Found $($admins.Count) admins" -Level Success
    return $admins
}

function Test-AdminLocked {
    param([string]$RecordId)

    $result = Invoke-AirtableApi -Method GET -TableId $AirtableConfig.AdminsTableId -RecordId $RecordId
    if ($result.Success) {
        return [bool]$result.Data.fields.'Processing Lock'
    }
    return $true
}

function Request-AdminLock {
    param([string]$RecordId, [string]$AdminEmail)

    if (Test-AdminLocked -RecordId $RecordId) {
        Write-Log "Admin $AdminEmail is locked by another process - skipping" -Level Warning
        return $false
    }

    $lockResult = Update-AirtableAdmin -RecordId $RecordId -SetProcessingLock -ProcessingLock $true
    if (-not $lockResult) {
        Write-Log "Failed to acquire lock for $AdminEmail" -Level Warning
        return $false
    }

    Start-Sleep -Milliseconds 500
    if (-not (Test-AdminLocked -RecordId $RecordId)) {
        Write-Log "Lock verification failed for $AdminEmail" -Level Warning
        return $false
    }

    Write-Log "Acquired processing lock for $AdminEmail" -Level Success
    return $true
}

function Release-AdminLock {
    param([string]$RecordId, [string]$AdminEmail)

    $result = Update-AirtableAdmin -RecordId $RecordId -SetProcessingLock -ProcessingLock $false
    if ($result) {
        Write-Log "Released processing lock for $AdminEmail" -Level Info
    } else {
        Write-Log "Failed to release lock for $AdminEmail - may need manual unlock" -Level Warning
    }
    return $result
}

function Get-AirtableDomains {
    Write-Log "Fetching domains from Airtable (Status=Processing)..." -Level Info

    # Filter for orders with Status = "Processing" that are ready for provisioning
    $filterFormula = [System.Web.HttpUtility]::UrlEncode("{Status} = 'Processing'")
    $query = "filterByFormula=$filterFormula"

    $result = Invoke-AirtableApi -Method GET -TableId $AirtableConfig.DomainsTableId -Query $query
    if ($result.Success) {
        $domains = @()
        foreach ($record in $result.Data.records) {
            $namesJson = $record.fields.'Detailed Usernames'
            $names = @()
            if ($namesJson) {
                try { $names = $namesJson | ConvertFrom-Json } catch { }
            }

            $domains += @{
                RecordId         = $record.id
                Domain           = $record.fields.'Domain'
                AdminRecordId    = if ($record.fields.'MS Admins') { $record.fields.'MS Admins'[0] } else { $null }
                InboxPassword    = $record.fields.'Inbox Password'
                Status           = if ($record.fields.'Status') { $record.fields.'Status' } else { "Processing" }
                InterimStatus    = if ($record.fields.'Interim Status') { $record.fields.'Interim Status' } else { "" }
                ActionHistory    = if ($record.fields.'Action History') { $record.fields.'Action History' } else { "" }
                ActionRequest    = if ($record.fields.'Action Request') { $record.fields.'Action Request' } else { "" }
                ActionError      = if ($record.fields.'Action Error') { $record.fields.'Action Error' } else { "" }
                MailboxesTarget  = if ($record.fields.'Mailboxes Target') { [int]$record.fields.'Mailboxes Target' } else { 99 }
                MailboxesCreated = if ($record.fields.'Mailboxes Created') { [int]$record.fields.'Mailboxes Created' } else { 0 }
                RetryCount       = if ($record.fields.'Retry Count') { [int]$record.fields.'Retry Count' } else { 0 }
                Tolerance        = if ($record.fields.'Tolerance') { [int]$record.fields.'Tolerance' } else { 0 }
                Names            = $names
            }
        }
        Write-Log "Found $($domains.Count) domains to process" -Level Success
        return $domains
    }
    return @()
}

function Update-AirtableAdmin {
    param(
        [string]$RecordId,
        [string]$TenantId = $null,
        [string]$TenantStatus = $null,
        [switch]$UpdateLastTokenSuccess,
        [bool]$UserConsentConfigured = $false,
        [switch]$SetUserConsent,
        [bool]$ProcessingLock = $false,
        [switch]$SetProcessingLock
    )

    $fields = @{}
    if ($TenantId) { $fields.'Tenant ID' = $TenantId }
    if ($TenantStatus) { $fields.'Tenant Status' = $TenantStatus }
    if ($UpdateLastTokenSuccess) { $fields.'Last Token Success' = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fffZ") }
    if ($SetUserConsent) { $fields.'User Consent Configured' = $UserConsentConfigured }
    if ($SetProcessingLock) { $fields.'Processing Lock' = $ProcessingLock }

    if ($fields.Count -gt 0) {
        $body = @{ fields = $fields }
        $result = Invoke-AirtableApi -Method PATCH -TableId $AirtableConfig.AdminsTableId -RecordId $RecordId -Body $body
        return $result.Success
    }
    return $true
}

function Update-AirtableDomain {
    param(
        [string]$RecordId,
        [string]$Status = $null,
        [string]$InterimStatus = $null,
        [string]$ActionHistory = $null,
        [string]$ActionRequest = $null,
        [string]$ActionError = $null,
        [array]$StepFailedAt = $null,
        [string]$ErrorMessage = $null,
        [int]$MailboxesCreated = -1,
        [bool]$DKIMEnabled = $false,
        [switch]$SetDKIM,
        [string]$CreatedUserJSON = $null,
        [switch]$ClearFailedSteps,
        [switch]$ClearActionError
    )

    $fields = @{}
    if ($Status) { $fields.'Status' = $Status }
    if ($InterimStatus) { $fields.'Interim Status' = $InterimStatus }
    if ($null -ne $ActionHistory) { $fields.'Action History' = $ActionHistory }
    if ($null -ne $ActionRequest) { $fields.'Action Request' = $ActionRequest }
    if ($null -ne $ActionError) { $fields.'Action Error' = $ActionError }
    if ($ClearActionError) { $fields.'Action Error' = "" }
    if ($StepFailedAt -and $StepFailedAt.Count -gt 0) { $fields.'Step Failed At' = $StepFailedAt }
    if ($ClearFailedSteps) { $fields.'Step Failed At' = @() }
    if ($null -ne $ErrorMessage) { $fields.'Error Message' = $ErrorMessage }
    if ($MailboxesCreated -ge 0) { $fields.'Mailboxes Created' = $MailboxesCreated }
    if ($SetDKIM) { $fields.'DKIM Enabled' = $DKIMEnabled }
    if ($CreatedUserJSON) { $fields.'Created User JSON' = $CreatedUserJSON }
    $fields.'Last Updated via Automation' = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fffZ")

    if ($fields.Count -gt 0) {
        $body = @{ fields = $fields }
        $result = Invoke-AirtableApi -Method PATCH -TableId $AirtableConfig.DomainsTableId -RecordId $RecordId -Body $body
        if (-not $result.Success) {
            Write-Log "Failed to update Airtable: $($result.Error)" -Level Warning
        }
        return $result.Success
    }
    return $true
}

# Helper: Append a timestamped entry to Action History (in-memory string)
function Add-HistoryEntry {
    param(
        [string]$History,
        [string]$Entry
    )
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $newLine = "[$timestamp] $Entry"
    if ($History) { return "$History`n$newLine" } else { return $newLine }
}

# ============================================================================
# DNSIMPLE FUNCTIONS
# ============================================================================
function Invoke-DNSimpleApi {
    param([string]$Method, [string]$Endpoint, [object]$Body = $null)

    $headers = @{
        "Authorization" = "Bearer $($DNSimpleConfig.ApiToken)"
        "Content-Type"  = "application/json"
        "Accept"        = "application/json"
    }
    $url = "$($DNSimpleConfig.BaseUrl)/$($DNSimpleConfig.AccountId)$Endpoint"
    $params = @{ Method = $Method; Uri = $url; Headers = $headers; TimeoutSec = 60; ErrorAction = "Stop" }
    if ($Body) { $params.Body = $Body | ConvertTo-Json -Depth 10 }

    try {
        $response = Invoke-RestMethod @params
        return @{ Success = $true; Data = $response }
    } catch {
        $errorMsg = $_.Exception.Message
        if ($_.ErrorDetails.Message) {
            try {
                $errorDetails = $_.ErrorDetails.Message | ConvertFrom-Json
                if ($errorDetails.message) { $errorMsg = $errorDetails.message }
            } catch { $errorMsg = $_.ErrorDetails.Message }
        }
        return @{ Success = $false; Error = $errorMsg }
    }
}

function Add-DNSimpleZone {
    param([string]$Domain)

    Write-Log "Adding domain to DNSimple: $Domain" -Level Info
    $body = @{ name = $Domain }
    $result = Invoke-DNSimpleApi -Method POST -Endpoint "/domains" -Body $body

    if ($result.Success) {
        Write-Log "Domain added to DNSimple" -Level Success
        return @{ Success = $true; AlreadyExists = $false }
    } else {
        if ($result.Error -match "already" -or $result.Error -match "exists") {
            Write-Log "Domain already exists in DNSimple" -Level Warning
            return @{ Success = $true; AlreadyExists = $true }
        }
        Write-Log "Failed to add domain: $($result.Error)" -Level Error
        return @{ Success = $false; Error = $result.Error }
    }
}

function Add-DNSimpleDnsRecord {
    param([string]$Domain, [string]$Name, [string]$Type, [string]$Content, [int]$TTL = 3600, [int]$Priority = $null)

    $recordName = if ($Name -eq "" -or $Name -eq "@") { "" } else { $Name }
    Write-Log "Adding $Type record for $Domain$(if($recordName){" ($recordName)"})" -Level Info

    $body = @{ name = $recordName; type = $Type; content = $Content; ttl = $TTL }
    if ($null -ne $Priority -and $Type -eq "MX") { $body.priority = $Priority }

    $result = Invoke-DNSimpleApi -Method POST -Endpoint "/zones/$Domain/records" -Body $body
    if ($result.Success) {
        Write-Log "$Type record added" -Level Success
        return @{ Success = $true }
    } else {
        if ($result.Error -match "already" -or $result.Error -match "taken" -or $result.Error -match "exists") {
            Write-Log "$Type record already exists" -Level Warning
            return @{ Success = $true; AlreadyExists = $true }
        }
        $checkResult = Invoke-DNSimpleApi -Method GET -Endpoint "/zones/$Domain/records?type=$Type&name=$recordName"
        if ($checkResult.Success -and $checkResult.Data.data.Count -gt 0) {
            Write-Log "$Type record already exists (verified)" -Level Warning
            return @{ Success = $true; AlreadyExists = $true }
        }
        Write-Log "Failed to add $Type record: $($result.Error)" -Level Error
        return @{ Success = $false; Error = $result.Error }
    }
}

function Add-M365DnsRecords {
    param([string]$Domain, [string]$Bearer)

    Write-Log "Fetching required DNS records from Microsoft for: $Domain" -Level Info

    $headers = @{ Authorization = "Bearer $Bearer" }

    try {
        $records = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/domains/$Domain/serviceConfigurationRecords" -Headers $headers -ErrorAction Stop

        foreach ($rec in $records.value) {
            switch ($rec.recordType) {
                "Mx" {
                    Write-Log "Adding MX record (from Microsoft): $($rec.mailExchange)" -Level Info
                    Add-DNSimpleDnsRecord -Domain $Domain -Name "" -Type "MX" -Content $rec.mailExchange -Priority $rec.preference
                }
                "Txt" {
                    if ($rec.text -match "spf") {
                        Write-Log "Adding SPF record (from Microsoft): $($rec.text)" -Level Info
                        Add-DNSimpleDnsRecord -Domain $Domain -Name "" -Type "TXT" -Content $rec.text
                    }
                }
                "CName" {
                    Write-Log "Adding CNAME record (from Microsoft): $($rec.label) -> $($rec.canonicalName)" -Level Info
                    $cnameName = $rec.label -replace "\.$Domain$", ""
                    Add-DNSimpleDnsRecord -Domain $Domain -Name $cnameName -Type "CNAME" -Content $rec.canonicalName
                }
            }
        }

        Add-DNSimpleDnsRecord -Domain $Domain -Name "_dmarc" -Type "TXT" -Content "v=DMARC1; p=none"

        Write-Log "M365 DNS records added successfully (fetched from Microsoft)" -Level Success
        return $true

    } catch {
        Write-Log "Failed to fetch DNS records from Microsoft: $($_.Exception.Message)" -Level Error

        Write-Log "Using fallback method with correct MX format (dots -> hyphens)..." -Level Warning
        $mxHost = $Domain -replace '\.', '-'
        Add-DNSimpleDnsRecord -Domain $Domain -Name "" -Type "MX" -Content "$mxHost.mail.protection.outlook.com" -Priority 0
        Add-DNSimpleDnsRecord -Domain $Domain -Name "" -Type "TXT" -Content "v=spf1 include:spf.protection.outlook.com -all"
        Add-DNSimpleDnsRecord -Domain $Domain -Name "_dmarc" -Type "TXT" -Content "v=DMARC1; p=none"
        Add-DNSimpleDnsRecord -Domain $Domain -Name "autodiscover" -Type "CNAME" -Content "autodiscover.outlook.com"

        return $true
    }
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

function Connect-ToExchangeOnline {
    param([string]$AdminEmail, [string]$AdminPassword)

    $securePwd = ConvertTo-SecureString $AdminPassword -AsPlainText -Force
    $creds = New-Object System.Management.Automation.PSCredential($AdminEmail, $securePwd)

    try {
        Connect-ExchangeOnline -Credential $creds -ShowBanner:$false -ErrorAction Stop
        Write-Log "Connected to Exchange Online as $AdminEmail" -Level Success
        return $true
    } catch {
        Write-Log "Failed to connect to Exchange Online: $($_.Exception.Message)" -Level Error
        return $false
    }
}

function Invoke-GraphRequest {
    param([string]$Method, [string]$Url, [string]$Bearer, [object]$Body = $null)

    $headers = @{ Authorization = "Bearer $Bearer" }
    $params = @{ Method = $Method; Uri = $Url; Headers = $headers; ContentType = "application/json"; TimeoutSec = 60; ErrorAction = "Stop" }
    if ($Body) { $params.Body = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 15 } }

    Invoke-RestMethod @params
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
        Invoke-GraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/domains" -Bearer $Bearer -Body $body
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

function Verify-M365Domain {
    param([string]$Bearer, [string]$Domain, [int]$MaxAttempts = 6, [int]$WaitSeconds = 30)

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

            Invoke-GraphRequest -Method PATCH -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer -Body $updateBody
            Write-Log "Email service enabled for $Domain" -Level Success
            return $true
        } catch {
            $errorMsg = $_.Exception.Message
            Write-Log "Email service attempt $attempt failed: $errorMsg" -Level Warning

            if ($errorMsg -match "400" -and $attempt -eq 1) {
                try {
                    Write-Log "Retrying with Email only..." -Level Info
                    $updateBody = @{ supportedServices = @("Email") }
                    Invoke-GraphRequest -Method PATCH -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer -Body $updateBody
                    Write-Log "Email service enabled for $Domain (Email only)" -Level Success
                    return $true
                } catch {
                    Write-Log "Email-only attempt failed: $($_.Exception.Message)" -Level Warning
                }
            }

            if ($attempt -lt 3) { Start-Sleep -Seconds 5 }
        }
    }

    Write-Log "Failed to enable Email service after 3 attempts" -Level Warning
    return $false
}

# ============================================================================
# USER CONSENT CONFIGURATION (Tenant-level - run once per tenant)
# ============================================================================
$script:GraphAppId = "00000003-0000-0000-c000-000000000000"
$script:ConsentAppName = "Tenant Consent Configurator"

$script:ConsentAppPermissions = @(
    @{name="Policy.ReadWrite.PermissionGrant"; id="2672f8bb-fd5e-42e0-85e1-ec764dd2614e"; type="Scope"},
    @{name="Policy.ReadWrite.Authorization"; id="edd3c878-b384-41fd-95ad-e7407dd775be"; type="Scope"},
    @{name="DelegatedPermissionGrant.ReadWrite.All"; id="41ce6ca6-6826-4807-84f1-1c82854f7ee5"; type="Scope"},
    @{name="Directory.AccessAsUser.All"; id="0e263e50-5827-48a4-b97c-d940288653c7"; type="Scope"},
    @{name="Directory.ReadWrite.All"; id="c5366453-9fb0-48a5-a156-24f0c49a4b84"; type="Scope"},
    @{name="Directory.Read.All"; id="06da0dbc-49e2-44d2-8312-53f166ab848a"; type="Scope"},
    @{name="RoleManagement.ReadWrite.Directory"; id="d01b97e9-cbc0-49fe-810a-750afd5527a3"; type="Scope"},
    @{name="User.Read.All"; id="a154be20-db9c-4678-8ab7-66f6cc099a59"; type="Scope"},
    @{name="Application.ReadWrite.All"; id="bdfbf15f-ee85-4955-8675-146e8e5296b5"; type="Scope"},
    @{name="AppRoleAssignment.ReadWrite.All"; id="84bccea3-f856-4a8a-967b-dbe0a3d53a64"; type="Scope"},
    @{name="Mail.Read"; id="570282fd-fa5c-430d-a7fd-fc8dc98a9dca"; type="Scope"},
    @{name="Mail.ReadWrite"; id="024d486e-b451-40bb-833d-3e66d98c5c73"; type="Scope"},
    @{name="Mail.Send"; id="e383f46e-2787-4529-855e-0e479a3ffac0"; type="Scope"},
    @{name="IMAP.AccessAsUser.All"; id="652390e4-393a-48de-9484-05f9b1212954"; type="Scope"},
    @{name="SMTP.Send"; id="258f6531-6087-4cc4-bb90-092c5fb3ed3f"; type="Scope"},
    @{name="openid"; id="37f7f235-527c-4136-accd-4a02d197296e"; type="Scope"},
    @{name="profile"; id="14dad69e-099b-42c9-810b-d002981feec1"; type="Scope"},
    @{name="User.Read"; id="e1fe6dd8-ba31-4d61-89e7-88639da4683d"; type="Scope"},
    @{name="email"; id="64a6cdd6-aab1-4aaf-94b8-3cc8405e90d0"; type="Scope"},
    @{name="offline_access"; id="7427e0e9-2fba-42fe-b0c0-848c9e6a8182"; type="Scope"}
)

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

function Configure-TenantUserConsent {
    param(
        [string]$Bearer,
        [string]$TenantId,
        [string]$AdminEmail,
        [string]$AdminPassword
    )

    Write-Log "Checking user consent configuration..." -Level Info

    try {
        $authPolicy = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/policies/authorizationPolicy" -Bearer $Bearer
        $assignedPolicies = $authPolicy.defaultUserRolePermissions.permissionGrantPoliciesAssigned

        if ($assignedPolicies -and ($assignedPolicies -contains "ManagePermissionGrantsForSelf.microsoft-user-default-low")) {
            Write-Log "User consent already configured (policy active) - skipping" -Level Success
            return $true
        }

        Write-Log "User consent not configured - setting up..." -Level Info

        $filterStr = [System.Web.HttpUtility]::UrlEncode("displayName eq '$($script:ConsentAppName)'")
        $apps = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/applications?`$filter=$filterStr" -Bearer $Bearer

        $app = $null
        if ($apps.value -and $apps.value.Count -gt 0) {
            $app = $apps.value[0]
            Write-Log "Found existing consent app: $($app.appId)" -Level Info
        } else {
            Write-Log "Creating consent configurator app..." -Level Info
            $appBody = @{
                displayName = $script:ConsentAppName
                signInAudience = "AzureADMyOrg"
                isFallbackPublicClient = $true
            }
            $app = Invoke-GraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/applications" -Bearer $Bearer -Body $appBody
            Write-Log "Created consent app: $($app.appId)" -Level Success
        }

        $spResp = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/servicePrincipals?`$filter=appId eq '$($app.appId)'" -Bearer $Bearer

        $sp = $null
        if ($spResp.value -and $spResp.value.Count -gt 0) {
            $sp = $spResp.value[0]
            Write-Log "Service principal exists: $($sp.id)" -Level Info
        } else {
            Write-Log "Creating service principal..." -Level Info
            $sp = Invoke-GraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/servicePrincipals" -Bearer $Bearer -Body @{appId = $app.appId}
            Write-Log "Created service principal: $($sp.id)" -Level Success
        }

        Write-Log "Assigning permissions to consent app..." -Level Info
        $resourceAccess = $script:ConsentAppPermissions | ForEach-Object { @{id = $_.id; type = $_.type} }
        $patchBody = @{
            requiredResourceAccess = @(@{
                resourceAppId = $script:GraphAppId
                resourceAccess = $resourceAccess
            })
        }
        Invoke-GraphRequest -Method PATCH -Url "https://graph.microsoft.com/v1.0/applications/$($app.id)" -Bearer $Bearer -Body $patchBody

        Write-Log "Granting admin consent..." -Level Info
        $graphSpResp = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/servicePrincipals?`$filter=appId eq '$($script:GraphAppId)'" -Bearer $Bearer

        if ($graphSpResp.value -and $graphSpResp.value.Count -gt 0) {
            $graphSp = $graphSpResp.value[0]

            $grantFilter = [System.Web.HttpUtility]::UrlEncode("clientId eq '$($sp.id)' and resourceId eq '$($graphSp.id)'")
            $existing = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/oauth2PermissionGrants?`$filter=$grantFilter" -Bearer $Bearer

            if (-not $existing.value -or $existing.value.Count -eq 0) {
                $scopeList = ($script:ConsentAppPermissions | Where-Object { $_.type -eq "Scope" } | ForEach-Object { $_.name }) -join " "
                $grantBody = @{
                    clientId = $sp.id
                    consentType = "AllPrincipals"
                    resourceId = $graphSp.id
                    scope = $scopeList
                }
                try {
                    Invoke-GraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/oauth2PermissionGrants" -Bearer $Bearer -Body $grantBody
                    Write-Log "Admin consent granted" -Level Success
                } catch {
                    Write-Log "Could not create grant (may already exist): $($_.Exception.Message)" -Level Warning
                }
            } else {
                Write-Log "Admin consent already exists" -Level Info
            }
        }

        Write-Log "Configuring low-impact permission classifications..." -Level Info
        $userToken = Get-ROPCToken -TenantId $TenantId -ClientId $app.appId -Username $AdminEmail -Password $AdminPassword

        if ($userToken) {
            $graphSp2Resp = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/servicePrincipals?`$filter=appId eq '$($script:GraphAppId)'" -Bearer $userToken

            if ($graphSp2Resp.value -and $graphSp2Resp.value.Count -gt 0) {
                $graphSp2 = $graphSp2Resp.value[0]

                $existingIds = @()
                try {
                    $existingClass = Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/servicePrincipals/$($graphSp2.id)/delegatedPermissionClassifications" -Bearer $userToken
                    $existingIds = $existingClass.value | ForEach-Object { $_.permissionId }
                } catch { }

                foreach ($perm in $script:LowImpactPermissions) {
                    if ($perm.id -in $existingIds) { continue }
                    try {
                        $classBody = @{
                            permissionId = $perm.id
                            permissionName = $perm.name
                            classification = "low"
                        }
                        Invoke-GraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/servicePrincipals/$($graphSp2.id)/delegatedPermissionClassifications" -Bearer $userToken -Body $classBody
                    } catch { }
                }
                Write-Log "Low-impact classifications configured" -Level Success
            }

            Write-Log "Setting user consent policy..." -Level Info
            $policyBody = @{
                defaultUserRolePermissions = @{
                    permissionGrantPoliciesAssigned = @("ManagePermissionGrantsForSelf.microsoft-user-default-low")
                }
            }
            try {
                Invoke-GraphRequest -Method PATCH -Url "https://graph.microsoft.com/v1.0/policies/authorizationPolicy" -Bearer $userToken -Body $policyBody
                Write-Log "User consent policy configured" -Level Success
            } catch {
                Write-Log "Could not set consent policy: $($_.Exception.Message)" -Level Warning
            }
        }

        Write-Log "Tenant user consent configuration complete" -Level Success
        return $true
    } catch {
        Write-Log "Failed to configure user consent: $($_.Exception.Message)" -Level Error
        return $false
    }
}

function Wait-ForExchangeSync {
    param([string]$Domain, [int]$MaxWaitSeconds = 180)

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
# ORPHAN USER DETECTION AND CLEANUP
# ============================================================================
function Remove-OrphanUsersForEmail {
    param(
        [string]$Email,
        [string]$CorrectUserId,
        [hashtable]$Headers
    )

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
# RESOURCE MAILBOX CREATION
# Key differences from shared mailbox:
#   1. Uses New-Mailbox -Room instead of -Shared
#   2. Appends a unique number to DisplayName during creation to avoid
#      "The display name ... is already being used" errors, then renames
#      the user to the real first/last name after creation via Graph API
#   3. Disables calendar auto-processing (AutomateProcessing = None) so the
#      resource mailbox behaves like a regular inbox
# ============================================================================
function New-ResourceMailboxBulk {
    param([string]$Domain, [array]$Names, [string]$Password, [string]$Bearer)

    Write-Log "Creating $($Names.Count) RESOURCE mailboxes for $Domain" -Level Info
    $results = @{ Created = @(); Failed = @() }
    $securePassword = ConvertTo-SecureString $Password -AsPlainText -Force

    $headers = @{ Authorization = "Bearer $Bearer"; "Content-Type" = "application/json" }

    # Get license SKU for fallback method
    $skuResponse = $null
    try { $skuResponse = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/subscribedSkus" -Headers $headers -ErrorAction SilentlyContinue } catch { }
    $licenseSkuId = if ($skuResponse -and $skuResponse.value) { $skuResponse.value[0].skuId } else { $null }

    # Global counter to guarantee unique display names across this run
    $script:resourceCounter = Get-Random -Minimum 1000 -Maximum 9999

    $counter = 0
    foreach ($name in $Names) {
        $counter++
        $firstName = $name.FirstName
        $lastName = $name.LastName
        $email = "$($firstName.ToLower())@$Domain"
        $realDisplayName = "$firstName $lastName"

        # --- Unique temporary display name to avoid M365 duplicate name errors ---
        $script:resourceCounter++
        $tempDisplayName = "$firstName $lastName $($script:resourceCounter)"

        # Check if mailbox already exists
        $existing = Get-Mailbox -Identity $email -ErrorAction SilentlyContinue
        if ($existing) {
            Write-Log "[$counter/$($Names.Count)] Already exists ($($existing.RecipientTypeDetails)): $email" -Level Warning
            $results.Created += @{ Email = $email; FirstName = $firstName; LastName = $lastName; DisplayName = $realDisplayName; AlreadyExisted = $true }
            continue
        }

        # METHOD 1: Try direct Room resource mailbox creation with temp display name
        Write-Log "[$counter/$($Names.Count)] Creating resource mailbox: $email (temp name: $tempDisplayName)" -Level Info
        $directSuccess = $false

        try {
            New-Mailbox -Room -Name $tempDisplayName -DisplayName $tempDisplayName -PrimarySmtpAddress $email -Password $securePassword -ResetPasswordOnNextLogon $false -ErrorAction Stop | Out-Null
            $directSuccess = $true
            Write-Log "Created resource mailbox (direct): $email" -Level Success
        } catch {
            $errorMsg = $_.Exception.Message
            if ($errorMsg -match "not an accepted domain" -or $errorMsg -match "couldn't be found") {
                Write-Log "Direct method failed, trying licensed user method..." -Level Warning
            } else {
                Write-Log "Failed: $email - $errorMsg" -Level Error
                $results.Failed += @{ Email = $email; Error = $errorMsg }
                continue
            }
        }

        # METHOD 2: Fallback - Licensed user method then convert to Room
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
                        Write-Log "Deleted user, waiting 10 seconds..." -Level Info
                        Start-Sleep -Seconds 10
                    } catch {
                        Write-Log "Failed to delete user: $($_.Exception.Message)" -Level Error
                        $results.Failed += @{ Email = $email; Error = "Failed to delete existing user" }
                        continue
                    }
                }

                # Create user via Graph with temp display name
                $userBody = @{
                    accountEnabled = $true
                    displayName = $tempDisplayName
                    givenName = $firstName
                    surname = $lastName
                    mailNickname = $firstName.ToLower()
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
                    if ($mbx) {
                        $mailboxReady = $true
                        break
                    }
                }

                if (-not $mailboxReady) {
                    throw "Mailbox not provisioned after 135 seconds"
                }

                # Convert to Room resource mailbox (with retries)
                $convertSuccess = $false
                for ($convertAttempt = 1; $convertAttempt -le 3; $convertAttempt++) {
                    try {
                        Set-Mailbox -Identity $email -Type Room -ErrorAction Stop
                        Write-Log "Converted to Room resource mailbox: $email" -Level Info
                        $convertSuccess = $true
                        break
                    } catch {
                        Write-Log "Convert to Room attempt $convertAttempt/3 failed: $($_.Exception.Message)" -Level Warning
                        if ($convertAttempt -lt 3) {
                            Write-Log "Waiting 30 seconds before retry..." -Level Info
                            Start-Sleep -Seconds 30
                        }
                    }
                }

                if (-not $convertSuccess) {
                    throw "Failed to convert to Room resource mailbox after 3 attempts"
                }

                # Remove license
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
                $results.Failed += @{ Email = $email; Error = $errMsg }
            } finally {
                if ($licenseAssigned -and $newUserId) {
                    try {
                        Write-Log "Cleaning up: removing license from $email" -Level Warning
                        $remBody = @{ addLicenses = @(); removeLicenses = @($licenseSkuId) } | ConvertTo-Json -Depth 5
                        Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$newUserId/assignLicense" -Method POST -Headers $headers -Body $remBody | Out-Null
                        Write-Log "License cleaned up for: $email" -Level Info
                    } catch {
                        Write-Log "Failed to cleanup license for $email - may need manual removal" -Level Error
                    }
                }
            }

            if (-not $directSuccess) { continue }
        }

        if ($directSuccess) {
            $results.Created += @{ Email = $email; FirstName = $firstName; LastName = $lastName; DisplayName = $realDisplayName; TempDisplayName = $tempDisplayName; AlreadyExisted = $false }
        }

        Start-Sleep -Milliseconds $DelayBetweenMailboxesMs
    }

    # ============================================================================
    # POST-CREATION STEP 1: Rename display names from temp back to real names
    # and fix first/last name via Graph API
    # ============================================================================
    if ($results.Created.Count -gt 0) {
        Write-Log "Renaming resource mailboxes to real display names..." -Level Info
        Start-Sleep -Seconds 10

        foreach ($mb in $results.Created) {
            if ($mb.AlreadyExisted) { continue }

            $email = $mb.Email
            $realDisplayName = $mb.DisplayName
            $firstName = $mb.FirstName
            $lastName = $mb.LastName

            # Rename via Exchange (DisplayName)
            try {
                Set-Mailbox -Identity $email -DisplayName $realDisplayName -Name $realDisplayName -ErrorAction Stop
                Write-Log "Renamed mailbox display name: $email -> $realDisplayName" -Level Success
            } catch {
                Write-Log "Failed to rename mailbox $email via Set-Mailbox: $($_.Exception.Message)" -Level Warning
            }

            # Fix first name, last name, and display name via Graph API
            try {
                $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$email" -Headers $headers -ErrorAction Stop
                $updateBody = @{
                    displayName = $realDisplayName
                    givenName = $firstName
                    surname = $lastName
                } | ConvertTo-Json -Depth 3
                Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$($user.id)" -Method PATCH -Headers $headers -Body $updateBody -ErrorAction Stop | Out-Null
                Write-Log "Updated Graph user names: $email ($firstName $lastName)" -Level Success
            } catch {
                Write-Log "Failed to update Graph names for $email: $($_.Exception.Message)" -Level Warning
            }
        }
    }

    # ============================================================================
    # POST-CREATION STEP 2: Disable calendar auto-processing
    # Resource mailboxes auto-accept/decline meetings by default. Disable this
    # so they behave like regular mailboxes for cold email use.
    # ============================================================================
    if ($results.Created.Count -gt 0) {
        Write-Log "Disabling calendar auto-processing on resource mailboxes..." -Level Info
        Start-Sleep -Seconds 5

        foreach ($mb in $results.Created) {
            $email = $mb.Email
            try {
                Set-CalendarProcessing -Identity $email -AutomateProcessing None -DeleteComments $false -DeleteSubject $false -RemovePrivateProperty $false -DeleteNonCalendarItems $false -ErrorAction Stop
                Write-Log "Calendar auto-processing disabled: $email" -Level Success
            } catch {
                Write-Log "Failed to disable calendar for $email: $($_.Exception.Message)" -Level Warning
                # Retry once after a short wait
                Start-Sleep -Seconds 3
                try {
                    Set-CalendarProcessing -Identity $email -AutomateProcessing None -DeleteComments $false -DeleteSubject $false -RemovePrivateProperty $false -DeleteNonCalendarItems $false -ErrorAction Stop
                    Write-Log "Calendar auto-processing disabled (retry): $email" -Level Success
                } catch {
                    Write-Log "Calendar disable retry failed for $email: $($_.Exception.Message)" -Level Error
                }
            }
        }
    }

    # ============================================================================
    # POST-CREATION STEP 3: Fix UPNs (same as shared mailbox script)
    # ============================================================================
    if ($results.Created.Count -gt 0) {
        Write-Log "Fixing UPNs for newly created mailboxes..." -Level Info
        Start-Sleep -Seconds 5
        foreach ($mb in $results.Created) {
            if (-not $mb.AlreadyExisted) {
                $email = $mb.Email

                $mbx = Get-Mailbox -Identity $email -ErrorAction SilentlyContinue
                $correctUserId = if ($mbx) { $mbx.ExternalDirectoryObjectId } else { "NONE" }

                Remove-OrphanUsersForEmail -Email $email -CorrectUserId $correctUserId -Headers $headers

                try { Set-Mailbox -Identity $email -MicrosoftOnlineServicesID $email -ErrorAction SilentlyContinue } catch { }

                try {
                    $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$email" -Headers $headers -ErrorAction SilentlyContinue
                    if ($user -and $user.userPrincipalName -ne $email) {
                        $upnBody = @{ userPrincipalName = $email } | ConvertTo-Json
                        Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$($user.id)" -Method PATCH -Headers $headers -Body $upnBody -ErrorAction SilentlyContinue | Out-Null
                    }
                } catch { }
            }
        }
    }

    Write-Log "Resource mailbox creation complete: $($results.Created.Count) created, $($results.Failed.Count) failed" -Level Info
    return $results
}

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

function Unblock-DomainMailboxes {
    param([string]$Domain, [string]$Bearer)

    Write-Log "Unblocking mailboxes and fixing UPNs for: $Domain" -Level Info

    $headers = @{ Authorization = "Bearer $Bearer"; "Content-Type" = "application/json" }

    $mailboxes = Get-Mailbox -ResultSize Unlimited | Where-Object { $_.PrimarySmtpAddress -like "*@$Domain" }
    $total = ($mailboxes | Measure-Object).Count
    $unblocked = 0
    $upnFixed = 0
    $failed = 0

    foreach ($mb in $mailboxes) {
        $email = $mb.PrimarySmtpAddress.ToString()
        $userId = $mb.ExternalDirectoryObjectId

        Remove-OrphanUsersForEmail -Email $email -CorrectUserId $userId -Headers $headers

        try {
            Set-Mailbox -Identity $email -MicrosoftOnlineServicesID $email -ErrorAction SilentlyContinue
            Write-Log "Set-Mailbox UPN applied: $email" -Level Info
        } catch { }

        if (-not $userId) {
            Write-Log "No ExternalDirectoryObjectId for $email - skipping Graph update" -Level Warning
            $failed++
            continue
        }

        try {
            $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Headers $headers -ErrorAction Stop

            $needsUpdate = $false
            $updateBody = @{}

            if ($user.accountEnabled -eq $false) {
                $updateBody.accountEnabled = $true
                $needsUpdate = $true
                Write-Log "Will unblock: $email (currently blocked)" -Level Info
            }

            if ($user.userPrincipalName -ne $email) {
                $updateBody.userPrincipalName = $email
                $needsUpdate = $true
                $upnFixed++
                Write-Log "Will fix UPN: $($user.userPrincipalName) -> $email" -Level Info
            }

            if ($needsUpdate) {
                $body = $updateBody | ConvertTo-Json
                Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Method PATCH -Headers $headers -Body $body -ErrorAction Stop | Out-Null
                if ($updateBody.accountEnabled) {
                    Write-Log "Unblocked: $email" -Level Success
                    $unblocked++
                }
            } else {
                Write-Log "OK: $email (already unblocked, UPN correct)" -Level Info
            }
        } catch {
            Write-Log "Failed to process $email (ID: $userId): $($_.Exception.Message)" -Level Warning
            $failed++
        }
    }

    Write-Log "Unblock complete: $unblocked unblocked, $upnFixed UPNs fixed, $failed failed" -Level Info
    return @{ Total = $total; Unblocked = $unblocked; UPNFixed = $upnFixed; Failed = $failed }
}

# ============================================================================
# BULLETPROOF CHECK (adapted for resource mailboxes)
# ============================================================================
function Invoke-BulletproofMailboxCheck {
    param(
        [string]$Domain,
        [string]$Bearer,
        [string]$Password,
        [int]$MaxRetries = 3
    )

    Write-Log "========== BULLETPROOF FINAL CHECK: $Domain ==========" -Level Info

    $headers = @{ Authorization = "Bearer $Bearer"; "Content-Type" = "application/json" }

    $allPassed = $false
    $attempt = 0
    $finalReport = @{ Passed = 0; Failed = 0; Issues = @() }

    while (-not $allPassed -and $attempt -lt $MaxRetries) {
        $attempt++
        Write-Log "--- Verification Attempt $attempt of $MaxRetries ---" -Level Info

        $mailboxes = Get-Mailbox -ResultSize Unlimited | Where-Object { $_.PrimarySmtpAddress -like "*@$Domain" }
        $total = ($mailboxes | Measure-Object).Count
        $passed = 0
        $fixed = 0
        $issues = @()

        foreach ($mb in $mailboxes) {
            $email = $mb.PrimarySmtpAddress.ToString()
            $mailboxOk = $true
            $mailboxIssues = @()
            $needsFix = $false
            $fixBody = @{}

            # For resource mailboxes, accept RoomMailbox as valid type
            if ($mb.RecipientTypeDetails -ne "RoomMailbox") {
                $mailboxOk = $false
                $mailboxIssues += "NOT RoomMailbox (is $($mb.RecipientTypeDetails))"
                try {
                    Set-Mailbox -Identity $email -Type Room -ErrorAction Stop
                    Write-Log "[$email] Converted to Room mailbox" -Level Success
                    $fixed++
                } catch {
                    Write-Log "[$email] Failed to convert: $($_.Exception.Message)" -Level Error
                }
            }

            $userId = $mb.ExternalDirectoryObjectId

            Remove-OrphanUsersForEmail -Email $email -CorrectUserId $userId -Headers $headers

            try {
                Set-Mailbox -Identity $email -MicrosoftOnlineServicesID $email -ErrorAction SilentlyContinue
                Write-Log "[$email] Set-Mailbox UPN fix applied" -Level Info
            } catch { }

            $user = $null

            if ($userId) {
                try {
                    $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Headers $headers -ErrorAction Stop
                } catch { }
            }

            if (-not $user) {
                try {
                    $filter = [System.Web.HttpUtility]::UrlEncode("mail eq '$email' or proxyAddresses/any(p: p eq 'SMTP:$email')")
                    $searchResult = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users?`$filter=$filter" -Headers $headers -ErrorAction Stop
                    if ($searchResult.value -and $searchResult.value.Count -gt 0) {
                        $user = $searchResult.value[0]
                        $userId = $user.id
                    }
                } catch { }
            }

            if (-not $user) {
                $mailboxOk = $false
                $mailboxIssues += "User not found in Azure AD"
                $issues += "$email`: User not found in Azure AD"
                continue
            }

            if ($user.userPrincipalName -ne $email) {
                $mailboxOk = $false
                $mailboxIssues += "UPN wrong ($($user.userPrincipalName))"
                $needsFix = $true
                $fixBody.userPrincipalName = $email
            }

            $currentUPNPrefix = ($user.userPrincipalName -split '@')[0]
            if ($currentUPNPrefix -match '^[Gg][0-9a-fA-F]{20,}') {
                $mailboxOk = $false
                $mailboxIssues += "UPN is GUID format"
                $needsFix = $true
                $fixBody.userPrincipalName = $email
            }

            if ($user.mail -ne $email) {
                $needsFix = $true
                $fixBody.mail = $email
            }

            $fixBody.accountEnabled = $true
            $fixBody.passwordProfile = @{ forceChangePasswordNextSignIn = $false; password = $Password }
            $needsFix = $true

            if ($needsFix) {
                try {
                    $body = $fixBody | ConvertTo-Json -Depth 3
                    Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$($user.id)" -Method PATCH -Headers $headers -Body $body -ErrorAction Stop | Out-Null
                    Write-Log "[$email] Applied fixes: $($fixBody.Keys -join ', ')" -Level Success
                    $fixed++
                } catch {
                    Write-Log "[$email] Failed to apply fixes: $($_.Exception.Message)" -Level Error
                    $issues += "$email`: Fix failed"
                    $mailboxOk = $false
                }
            }

            # Also ensure calendar processing is disabled
            try {
                $calProc = Get-CalendarProcessing -Identity $email -ErrorAction SilentlyContinue
                if ($calProc -and $calProc.AutomateProcessing -ne "None") {
                    Set-CalendarProcessing -Identity $email -AutomateProcessing None -DeleteComments $false -DeleteSubject $false -RemovePrivateProperty $false -DeleteNonCalendarItems $false -ErrorAction Stop
                    Write-Log "[$email] Calendar auto-processing disabled during check" -Level Success
                }
            } catch {
                Write-Log "[$email] Could not check/fix calendar processing: $($_.Exception.Message)" -Level Warning
            }

            if ($mailboxOk) { $passed++ }
            else { $issues += "$email`: $($mailboxIssues -join ', ')" }
        }

        Write-Log "Attempt $attempt results: $passed/$total passed, $fixed fixed" -Level Info
        if ($passed -eq $total) { $allPassed = $true }
        $finalReport = @{ Total = $total; Passed = $passed; Failed = ($total - $passed); Fixed = $fixed; Issues = $issues }
        break
    }

    $mailboxes = Get-Mailbox -ResultSize Unlimited | Where-Object { $_.PrimarySmtpAddress -like "*@$Domain" }
    $finalPassed = 0
    $finalIssues = @()

    foreach ($mb in $mailboxes) {
        $email = $mb.PrimarySmtpAddress.ToString()
        $userId = $mb.ExternalDirectoryObjectId
        $allGood = $true
        $problemList = @()

        if ($mb.RecipientTypeDetails -ne "RoomMailbox") { $allGood = $false; $problemList += "NotRoom" }

        if ($userId) {
            try {
                $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Headers $headers -ErrorAction Stop
                if ($user.userPrincipalName -ne $email) { $allGood = $false; $problemList += "BadUPN" }
            } catch { $allGood = $false; $problemList += "NoUser" }
        } else { $allGood = $false; $problemList += "NoObjectId" }

        if ($allGood) { $finalPassed++ }
        else { $finalIssues += "$email`: $($problemList -join ',')" }
    }

    $finalTotal = ($mailboxes | Measure-Object).Count
    Write-Log "========== FINAL RESULT: $finalPassed/$finalTotal PASSED ==========" -Level $(if ($finalPassed -eq $finalTotal) { "Success" } else { "Warning" })

    return @{ Total = $finalTotal; Passed = $finalPassed; Failed = ($finalTotal - $finalPassed); Issues = $finalIssues; AllPassed = ($finalPassed -eq $finalTotal) }
}

# ============================================================================
# FAILSAFE (adapted for resource mailboxes)
# ============================================================================
function Invoke-DomainFailsafe {
    param(
        [string]$Domain,
        [string]$Bearer,
        [string]$Password,
        [array]$Names,
        [int]$TargetMailboxes = 99
    )

    Write-Log "========== FAILSAFE CHECK: $Domain ==========" -Level Info

    $headers = @{ Authorization = "Bearer $Bearer"; "Content-Type" = "application/json" }
    $issuesFound = 0
    $issuesFixed = 0
    $orphansDeleted = 0
    $mailboxesCreated = 0

    $mailboxes = Get-Mailbox -ResultSize Unlimited -ErrorAction SilentlyContinue |
                 Where-Object { $_.PrimarySmtpAddress -like "*@$Domain" }
    $currentCount = ($mailboxes | Measure-Object).Count

    Write-Log "Current mailbox count: $currentCount / $TargetMailboxes" -Level Info

    foreach ($mb in $mailboxes) {
        $email = $mb.PrimarySmtpAddress.ToString()
        $userId = $mb.ExternalDirectoryObjectId
        $hasIssue = $false

        try {
            $orphanSearch = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users?`$filter=userPrincipalName eq '$email'" -Headers $headers -ErrorAction SilentlyContinue
            if ($orphanSearch.value) {
                foreach ($orphan in $orphanSearch.value) {
                    if ($orphan.id -ne $userId) {
                        Write-Log "  ORPHAN FOUND for $email - Deleting: $($orphan.id)" -Level Warning
                        try {
                            Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$($orphan.id)" -Method DELETE -Headers $headers -ErrorAction Stop
                            $orphansDeleted++
                            Start-Sleep -Seconds 2
                        } catch {
                            if ($_.Exception.Message -notmatch "404") {
                                Write-Log "  Failed to delete orphan: $($_.Exception.Message)" -Level Error
                            }
                        }
                    }
                }
            }
        } catch { }

        if ($userId) {
            try {
                $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Headers $headers -ErrorAction Stop

                $needsFix = $false
                $fixBody = @{}

                $upnPrefix = ($user.userPrincipalName -split '@')[0]
                if ($upnPrefix -match '^[Gg][0-9a-fA-F]{20,}') {
                    Write-Log "  GUID UPN detected for $email" -Level Warning
                    $fixBody.userPrincipalName = $email
                    $needsFix = $true
                    $hasIssue = $true
                } elseif ($user.userPrincipalName -ne $email) {
                    Write-Log "  UPN mismatch for $email (is: $($user.userPrincipalName))" -Level Warning
                    $fixBody.userPrincipalName = $email
                    $needsFix = $true
                    $hasIssue = $true
                }

                if ($user.accountEnabled -eq $false) {
                    Write-Log "  Account disabled for $email" -Level Warning
                    $fixBody.accountEnabled = $true
                    $needsFix = $true
                    $hasIssue = $true
                }

                if ($user.mail -ne $email) {
                    $fixBody.mail = $email
                    $needsFix = $true
                }

                $fixBody.passwordProfile = @{
                    forceChangePasswordNextSignIn = $false
                    password = $Password
                }
                $needsFix = $true

                if ($needsFix) {
                    try {
                        Set-Mailbox -Identity $email -MicrosoftOnlineServicesID $email -ErrorAction SilentlyContinue

                        $body = $fixBody | ConvertTo-Json -Depth 3
                        Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Method PATCH -Headers $headers -Body $body -ErrorAction Stop | Out-Null

                        if ($hasIssue) {
                            Write-Log "  FIXED: $email" -Level Success
                            $issuesFixed++
                        }
                    } catch {
                        Write-Log "  Failed to fix $email`: $($_.Exception.Message)" -Level Error
                    }
                }

                if ($hasIssue) { $issuesFound++ }

            } catch {
                Write-Log "  Could not get user for $email`: $($_.Exception.Message)" -Level Warning
                $issuesFound++
            }
        } else {
            Write-Log "  No ExternalDirectoryObjectId for $email" -Level Warning
            $issuesFound++
        }

        # Also ensure calendar processing is disabled in failsafe
        try {
            $calProc = Get-CalendarProcessing -Identity $email -ErrorAction SilentlyContinue
            if ($calProc -and $calProc.AutomateProcessing -ne "None") {
                Set-CalendarProcessing -Identity $email -AutomateProcessing None -DeleteComments $false -DeleteSubject $false -RemovePrivateProperty $false -DeleteNonCalendarItems $false -ErrorAction Stop
                Write-Log "  Calendar auto-processing disabled (failsafe): $email" -Level Success
            }
        } catch { }
    }

    # Top up mailboxes if below target
    $currentCount = (Get-Mailbox -ResultSize Unlimited -ErrorAction SilentlyContinue |
                     Where-Object { $_.PrimarySmtpAddress -like "*@$Domain" } |
                     Measure-Object).Count

    if ($currentCount -lt $TargetMailboxes -and $Names -and $Names.Count -gt 0) {
        $deficit = $TargetMailboxes - $currentCount
        Write-Log "Mailbox deficit: $deficit - Will attempt to create more" -Level Warning

        $existingEmails = Get-Mailbox -ResultSize Unlimited -ErrorAction SilentlyContinue |
                          Where-Object { $_.PrimarySmtpAddress -like "*@$Domain" } |
                          ForEach-Object { ($_.PrimarySmtpAddress.ToString() -split '@')[0].ToLower() }

        $availableNames = $Names | Where-Object {
            $_.FirstName.ToLower() -notin $existingEmails -and
            "$($_.FirstName.ToLower()).$($_.FirstName.Substring(0,1).ToLower())" -notin $existingEmails
        }

        $namesToCreate = $availableNames | Select-Object -First $deficit

        if ($namesToCreate.Count -gt 0) {
            Write-Log "Creating $($namesToCreate.Count) additional resource mailboxes..." -Level Info

            $securePassword = ConvertTo-SecureString $Password -AsPlainText -Force

            foreach ($name in $namesToCreate) {
                $firstName = $name.FirstName
                $lastName = $name.LastName
                $email = "$($firstName.ToLower())@$Domain"
                $realDisplayName = "$firstName $lastName"

                # Unique temp name for creation
                $script:resourceCounter++
                $tempDisplayName = "$firstName $lastName $($script:resourceCounter)"

                # Pre-cleanup any orphans
                try {
                    $orphanSearch = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users?`$filter=userPrincipalName eq '$email'" -Headers $headers -ErrorAction SilentlyContinue
                    if ($orphanSearch.value) {
                        foreach ($orphan in $orphanSearch.value) {
                            Write-Log "  Pre-cleanup orphan for $email" -Level Warning
                            Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$($orphan.id)" -Method DELETE -Headers $headers -ErrorAction SilentlyContinue
                            $orphansDeleted++
                            Start-Sleep -Seconds 2
                        }
                        Start-Sleep -Seconds 3
                    }
                } catch { }

                # Create resource mailbox with temp name
                try {
                    New-Mailbox -Room -Name $tempDisplayName -DisplayName $tempDisplayName -PrimarySmtpAddress $email -Password $securePassword -ResetPasswordOnNextLogon $false -ErrorAction Stop | Out-Null
                    Write-Log "  Created resource mailbox: $email" -Level Success
                    $mailboxesCreated++

                    # Wait and fix
                    Start-Sleep -Seconds 3

                    # Rename to real display name
                    try {
                        Set-Mailbox -Identity $email -DisplayName $realDisplayName -Name $realDisplayName -ErrorAction SilentlyContinue
                    } catch { }

                    # Disable calendar
                    try {
                        Set-CalendarProcessing -Identity $email -AutomateProcessing None -DeleteComments $false -DeleteSubject $false -RemovePrivateProperty $false -DeleteNonCalendarItems $false -ErrorAction SilentlyContinue
                    } catch { }

                    $mbx = Get-Mailbox -Identity $email -ErrorAction SilentlyContinue
                    if ($mbx -and $mbx.ExternalDirectoryObjectId) {
                        $newUserId = $mbx.ExternalDirectoryObjectId

                        try {
                            $orphanSearch = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users?`$filter=userPrincipalName eq '$email'" -Headers $headers -ErrorAction SilentlyContinue
                            if ($orphanSearch.value) {
                                foreach ($orphan in $orphanSearch.value) {
                                    if ($orphan.id -ne $newUserId) {
                                        Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$($orphan.id)" -Method DELETE -Headers $headers -ErrorAction SilentlyContinue
                                        $orphansDeleted++
                                        Start-Sleep -Seconds 2
                                    }
                                }
                            }
                        } catch { }

                        Set-Mailbox -Identity $email -MicrosoftOnlineServicesID $email -ErrorAction SilentlyContinue

                        $updateBody = @{
                            accountEnabled = $true
                            userPrincipalName = $email
                            mail = $email
                            displayName = $realDisplayName
                            givenName = $firstName
                            surname = $lastName
                            passwordProfile = @{
                                forceChangePasswordNextSignIn = $false
                                password = $Password
                            }
                        } | ConvertTo-Json -Depth 3

                        Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$newUserId" -Method PATCH -Headers $headers -Body $updateBody -ErrorAction SilentlyContinue | Out-Null
                    }
                } catch {
                    Write-Log "  Failed to create $email`: $($_.Exception.Message)" -Level Error
                }
            }
        } else {
            Write-Log "No available names to create additional mailboxes" -Level Warning
        }
    }

    $finalCount = (Get-Mailbox -ResultSize Unlimited -ErrorAction SilentlyContinue |
                   Where-Object { $_.PrimarySmtpAddress -like "*@$Domain" } |
                   Measure-Object).Count

    Write-Log "========== FAILSAFE COMPLETE: $Domain ==========" -Level Info
    Write-Log "  Issues found: $issuesFound" -Level $(if ($issuesFound -gt 0) { "Warning" } else { "Info" })
    Write-Log "  Issues fixed: $issuesFixed" -Level $(if ($issuesFixed -gt 0) { "Success" } else { "Info" })
    Write-Log "  Orphans deleted: $orphansDeleted" -Level $(if ($orphansDeleted -gt 0) { "Warning" } else { "Info" })
    Write-Log "  Mailboxes created: $mailboxesCreated" -Level $(if ($mailboxesCreated -gt 0) { "Success" } else { "Info" })
    Write-Log "  Final count: $finalCount / $TargetMailboxes" -Level $(if ($finalCount -ge $TargetMailboxes) { "Success" } else { "Warning" })

    return @{
        IssuesFound = $issuesFound
        IssuesFixed = $issuesFixed
        OrphansDeleted = $orphansDeleted
        MailboxesCreated = $mailboxesCreated
        FinalCount = $finalCount
        TargetMet = ($finalCount -ge $TargetMailboxes)
    }
}

# ============================================================================
# DKIM FUNCTIONS
# ============================================================================
function Setup-DomainDKIM {
    param([string]$Domain)

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

function Complete-DKIMSetup {
    param([string]$Domain, [string]$Selector1CNAME, [string]$Selector2CNAME)

    Add-DNSimpleDnsRecord -Domain $Domain -Name "selector1._domainkey" -Type "CNAME" -Content $Selector1CNAME | Out-Null
    Add-DNSimpleDnsRecord -Domain $Domain -Name "selector2._domainkey" -Type "CNAME" -Content $Selector2CNAME | Out-Null

    Write-Log "Waiting 120 seconds for DNS propagation..." -Level Info
    Start-Sleep -Seconds 120

    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            Set-DkimSigningConfig -Identity $Domain -Enabled $true -ErrorAction Stop -WarningAction SilentlyContinue
            Write-Log "DKIM enabled for $Domain" -Level Success
            return $true
        } catch {
            if ($attempt -lt 3) { Start-Sleep -Seconds 90 }
        }
    }
    return $false
}

function Get-ActualMailboxCount {
    param([string]$Domain)
    try {
        $mailboxes = Get-Mailbox -ResultSize Unlimited | Where-Object { $_.PrimarySmtpAddress -like "*@$Domain" }
        return ($mailboxes | Measure-Object).Count
    } catch { return 0 }
}

# ============================================================================
# PROCESS-DOMAIN FUNCTION
# Uses Interim Status (with Both/Microsoft prefixes) for granular pipeline tracking.
# Main Status stays "Processing" throughout, only set to "Completed"/"Failed" at end.
# Action History is appended at each step for audit trail.
# ============================================================================
function Process-Domain {
    param([hashtable]$DomainInfo, [hashtable]$AdminInfo, [string]$Bearer)

    $Domain = $DomainInfo.Domain
    $RecordId = $DomainInfo.RecordId
    $AdminEmail = $AdminInfo.AdminEmail
    $InboxPassword = $DomainInfo.InboxPassword
    $Names = $DomainInfo.Names
    $interimStatus = $DomainInfo.InterimStatus
    $actionHistory = $DomainInfo.ActionHistory
    $targetMailboxes = $DomainInfo.MailboxesTarget
    $tolerance = $DomainInfo.Tolerance
    $minMailboxes = $targetMailboxes - $tolerance

    if ($Names.Count -gt $targetMailboxes) { $Names = $Names[0..($targetMailboxes-1)] }

    $failedSteps = @()
    $errorMessages = @()

    Write-Host ""
    Write-Host "================================================================" -ForegroundColor Magenta
    Write-Host "  Processing Domain (RESOURCE MAILBOX): $Domain" -ForegroundColor Magenta
    Write-Host "  Admin: $AdminEmail | Interim Status: $interimStatus" -ForegroundColor Magenta
    Write-Host "  Target: $targetMailboxes (Min: $minMailboxes, Names: $($Names.Count))" -ForegroundColor Magenta
    Write-Host "================================================================" -ForegroundColor Magenta

    # Interim Status values used for resume logic (ordered pipeline)
    $knownInterimStatuses = @(
        "",
        "Both - New Order",
        "Both - DNS Zone Created",
        "Microsoft - Added to M365",
        "Both - Verification TXT Added",
        "Both - Domain Verified",
        "Microsoft - Email Enabled",
        "Both - DNS Records Added",
        "Microsoft - Exchange Synced",
        "Both - Creating Mailboxes",
        "Microsoft - Mailboxes Created",
        "Microsoft - Configuring Mailboxes",
        "Microsoft - SMTP Enabled",
        "Both - DKIM Complete",
        "Both - Provisioning Complete",
        "Both - Failed"
    )

    if ($null -eq $interimStatus) { $interimStatus = "" }
    if ($interimStatus -notin $knownInterimStatuses) {
        Write-Log "Interim Status '$interimStatus' not a provisioning status - skipping" -Level Warning
        return
    }
    if ($interimStatus -eq "Both - Provisioning Complete") { Write-Log "Already completed, skipping" -Level Warning; return }
    if ($Names.Count -eq 0) {
        $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "FAILED: No names provided for mailbox creation"
        Update-AirtableDomain -RecordId $RecordId -Status "Failed" -InterimStatus "Both - Failed" -StepFailedAt @("Mailbox Creation") -ErrorMessage "No names" -ActionHistory $actionHistory
        return
    }

    $mailboxResults = $null
    $dkimSuccess = $false
    $actualMailboxCount = 0

    # Clear previous errors on fresh run
    Update-AirtableDomain -RecordId $RecordId -ClearFailedSteps -ErrorMessage "" -ClearActionError
    $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Provisioning started for $Domain (admin: $AdminEmail)"

    # STEP 1: DNSimple Zone
    if ($interimStatus -eq "" -or $interimStatus -eq "Both - New Order") {
        Write-Host "  [Both - DNS Zone] Adding zone..." -ForegroundColor Yellow
        if (-not $DryRun) {
            $zoneResult = Add-DNSimpleZone -Domain $Domain
            if (-not $zoneResult.Success) {
                $failedSteps += "DNSimple Zone"
                $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "FAILED: DNS zone creation - $($zoneResult.Error)"
                Update-AirtableDomain -RecordId $RecordId -Status "Failed" -InterimStatus "Both - Failed" -StepFailedAt $failedSteps -ErrorMessage $zoneResult.Error -ActionHistory $actionHistory
                return
            }
        }
        $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "DNS zone created in DNSimple"
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Both - DNS Zone Created" -ActionHistory $actionHistory
        $interimStatus = "Both - DNS Zone Created"
    }

    # STEP 2: M365 Domain Add
    if ($interimStatus -eq "Both - DNS Zone Created") {
        Write-Host "  [Microsoft - Add to M365] Adding to tenant..." -ForegroundColor Yellow
        if (-not $DryRun) {
            $addResult = Add-DomainToM365 -Bearer $Bearer -Domain $Domain
            if (-not $addResult.Success) {
                $failedSteps += "M365 Domain Add"
                $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "FAILED: Add domain to M365 - $($addResult.Error)"
                Update-AirtableDomain -RecordId $RecordId -Status "Failed" -InterimStatus "Both - Failed" -StepFailedAt $failedSteps -ErrorMessage $addResult.Error -ActionHistory $actionHistory
                return
            }
        }
        $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Domain added to M365 tenant"
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Microsoft - Added to M365" -ActionHistory $actionHistory
        $interimStatus = "Microsoft - Added to M365"
    }

    # STEP 3: Verification TXT
    if ($interimStatus -eq "Microsoft - Added to M365") {
        Write-Host "  [Both - Verification TXT] Adding record..." -ForegroundColor Yellow
        if (-not $DryRun) {
            $verificationTxt = Get-DomainVerificationRecord -Bearer $Bearer -Domain $Domain
            if (-not $verificationTxt) {
                $failedSteps += "Verification TXT"
                $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "FAILED: Could not get verification TXT record"
                Update-AirtableDomain -RecordId $RecordId -Status "Failed" -InterimStatus "Both - Failed" -StepFailedAt $failedSteps -ErrorMessage "Could not get verification TXT" -ActionHistory $actionHistory
                return
            }
            Add-DNSimpleDnsRecord -Domain $Domain -Name "" -Type "TXT" -Content $verificationTxt | Out-Null
            Start-Sleep -Seconds 10
        }
        $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Verification TXT record added to DNS"
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Both - Verification TXT Added" -ActionHistory $actionHistory
        $interimStatus = "Both - Verification TXT Added"
    }

    # STEP 4: Domain Verify
    if ($interimStatus -eq "Both - Verification TXT Added") {
        Write-Host "  [Both - Domain Verify] Verifying in M365..." -ForegroundColor Yellow
        if (-not $DryRun) {
            $verified = Verify-M365Domain -Bearer $Bearer -Domain $Domain
            if (-not $verified -and -not (Test-DomainVerified -Bearer $Bearer -Domain $Domain)) {
                $failedSteps += "Domain Verify"
                $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "FAILED: Domain verification failed in M365"
                Update-AirtableDomain -RecordId $RecordId -Status "Failed" -InterimStatus "Both - Failed" -StepFailedAt $failedSteps -ErrorMessage "Verification failed" -ActionHistory $actionHistory
                return
            }
        }
        $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Domain verified in M365"
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Both - Domain Verified" -ActionHistory $actionHistory
        $interimStatus = "Both - Domain Verified"
    }

    # STEP 5: Email Service
    if ($interimStatus -eq "Both - Domain Verified") {
        Write-Host "  [Microsoft - Email Service] Enabling..." -ForegroundColor Yellow
        if (-not $DryRun) { Enable-DomainEmailService -Bearer $Bearer -Domain $Domain | Out-Null }
        $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Microsoft Email service enabled"
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Microsoft - Email Enabled" -ActionHistory $actionHistory
        $interimStatus = "Microsoft - Email Enabled"
    }

    # STEP 6: DNS Records (MX, SPF, DMARC, autodiscover)
    if ($interimStatus -eq "Microsoft - Email Enabled") {
        Write-Host "  [Both - DNS Records] Fetching exact records from Microsoft API..." -ForegroundColor Yellow
        if (-not $DryRun) { Add-M365DnsRecords -Domain $Domain -Bearer $Bearer }
        $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "DNS records added (MX, SPF, DMARC, autodiscover)"
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Both - DNS Records Added" -ActionHistory $actionHistory
        $interimStatus = "Both - DNS Records Added"
    }

    # STEP 7: Exchange Sync
    if ($interimStatus -eq "Both - DNS Records Added") {
        Write-Host "  [Microsoft - Exchange Sync] Waiting..." -ForegroundColor Yellow
        if (-not $DryRun) {
            $synced = Wait-ForExchangeSync -Domain $Domain -MaxWaitSeconds 180
            if (-not $synced) {
                $failedSteps += "Exchange Sync"
                $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "FAILED: Exchange sync timeout (180s)"
                Update-AirtableDomain -RecordId $RecordId -Status "Failed" -InterimStatus "Both - Failed" -StepFailedAt $failedSteps -ErrorMessage "Exchange sync timeout" -ActionHistory $actionHistory
                return
            }
        }
        $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Domain synced to Exchange Online"
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Microsoft - Exchange Synced" -ActionHistory $actionHistory
        $interimStatus = "Microsoft - Exchange Synced"
    }

    # STEP 8: Resource Mailbox Creation
    if ($interimStatus -eq "Microsoft - Exchange Synced" -or $interimStatus -eq "Both - Creating Mailboxes") {
        Write-Host "  [Both - Creating Mailboxes] Creating $($Names.Count) resource mailboxes..." -ForegroundColor Yellow
        $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Creating $($Names.Count) resource mailboxes..."
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Both - Creating Mailboxes" -ActionHistory $actionHistory
        if (-not $DryRun) {
            $mailboxResults = New-ResourceMailboxBulk -Domain $Domain -Names $Names -Password $InboxPassword -Bearer $Bearer
            Start-Sleep -Seconds 5
            $actualMailboxCount = Get-ActualMailboxCount -Domain $Domain
            Update-AirtableDomain -RecordId $RecordId -MailboxesCreated $actualMailboxCount

            if ($actualMailboxCount -lt $minMailboxes) {
                $failedSteps += "Mailbox Creation"
                $errorMessages += "Only $actualMailboxCount of $targetMailboxes (min: $minMailboxes)"
            }
            if ($mailboxResults.Created.Count -eq 0) {
                $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "FAILED: No mailboxes created (0/$targetMailboxes)"
                Update-AirtableDomain -RecordId $RecordId -Status "Failed" -InterimStatus "Both - Failed" -StepFailedAt $failedSteps -ErrorMessage ($errorMessages -join "; ") -ActionHistory $actionHistory
                return
            }
            $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Created $($mailboxResults.Created.Count) mailboxes, $($mailboxResults.Failed.Count) failed"
        } else {
            $mailboxResults = @{ Created = $Names | ForEach-Object { @{ Email = "$($_.FirstName.ToLower())@$Domain"; FirstName = $_.FirstName; LastName = $_.LastName } }; Failed = @() }
            $actualMailboxCount = $Names.Count
        }
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Microsoft - Mailboxes Created" -ActionHistory $actionHistory
        $interimStatus = "Microsoft - Mailboxes Created"
    }

    # STEP 9: Configure Mailboxes (Unblock + Bulletproof Check)
    if ($interimStatus -eq "Microsoft - Mailboxes Created") {
        Write-Host "  [Microsoft - Configuring Mailboxes] Unblocking & verifying..." -ForegroundColor Yellow
        if (-not $DryRun) {
            # Unblock sign-in
            Unblock-DomainMailboxes -Domain $Domain -Bearer $Bearer | Out-Null
            $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Mailboxes unblocked (sign-in enabled)"

            # Bulletproof check
            $bulletproofResult = Invoke-BulletproofMailboxCheck -Domain $Domain -Bearer $Bearer -Password $InboxPassword -MaxRetries 3
            if (-not $bulletproofResult.AllPassed) {
                $failedSteps += "Bulletproof Check"
                $errorMessages += "$($bulletproofResult.Failed)/$($bulletproofResult.Total) failed verification"
            }
            $actualMailboxCount = $bulletproofResult.Passed
            $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Bulletproof check: $($bulletproofResult.Passed)/$($bulletproofResult.Total) passed"
        }
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Microsoft - Configuring Mailboxes" -ActionHistory $actionHistory
        $interimStatus = "Microsoft - Configuring Mailboxes"
    }

    # STEP 10: SMTP Auth
    if ($interimStatus -eq "Microsoft - Configuring Mailboxes") {
        Write-Host "  [Microsoft - SMTP Auth] Enabling..." -ForegroundColor Yellow
        if (-not $DryRun) { Enable-TenantSMTPAuth | Out-Null }
        $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Tenant SMTP AUTH enabled"
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Microsoft - SMTP Enabled" -ActionHistory $actionHistory
        $interimStatus = "Microsoft - SMTP Enabled"
    }

    # STEP 11: DKIM Setup
    if ($interimStatus -eq "Microsoft - SMTP Enabled") {
        Write-Host "  [Both - DKIM Setup] Configuring..." -ForegroundColor Yellow
        if (-not $DryRun) {
            $dkimConfig = Setup-DomainDKIM -Domain $Domain
            if ($dkimConfig.Success) {
                if ($dkimConfig.AlreadyEnabled) {
                    $dkimSuccess = $true
                    $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "DKIM already enabled"
                } else {
                    $dkimSuccess = Complete-DKIMSetup -Domain $Domain -Selector1CNAME $dkimConfig.Selector1CNAME -Selector2CNAME $dkimConfig.Selector2CNAME
                    if ($dkimSuccess) {
                        $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "DKIM configured and enabled"
                    } else {
                        $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "WARNING: DKIM DNS records added but enable failed"
                    }
                }
            }
            if (-not $dkimSuccess) {
                $failedSteps += "DKIM Setup"
                $errorMessages += "DKIM setup failed"
            }
        } else { $dkimSuccess = $true }
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Both - DKIM Complete" -SetDKIM -DKIMEnabled ([bool]$dkimSuccess) -ActionHistory $actionHistory
        $interimStatus = "Both - DKIM Complete"
    }

    # STEP 12: FAILSAFE + Finalize
    if ($interimStatus -eq "Both - DKIM Complete") {
        Write-Host "  [FAILSAFE] Running comprehensive health check..." -ForegroundColor Yellow
        if (-not $DryRun) {
            $failsafeResult = Invoke-DomainFailsafe -Domain $Domain -Bearer $Bearer -Password $InboxPassword -Names $Names -TargetMailboxes $targetMailboxes
            $actualMailboxCount = $failsafeResult.FinalCount

            if ($failsafeResult.OrphansDeleted -gt 0) {
                Write-Log "Failsafe cleaned up $($failsafeResult.OrphansDeleted) orphans" -Level Warning
                $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Failsafe: cleaned $($failsafeResult.OrphansDeleted) orphan users"
            }
            if ($failsafeResult.MailboxesCreated -gt 0) {
                Write-Log "Failsafe created $($failsafeResult.MailboxesCreated) additional mailboxes" -Level Success
                $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Failsafe: created $($failsafeResult.MailboxesCreated) additional mailboxes"
            }
        }

        Write-Host "  [Credentials Save] Saving to Airtable..." -ForegroundColor Yellow

        if ($null -eq $mailboxResults) {
            $mailboxResults = @{ Created = $Names | ForEach-Object { @{ Email = "$($_.FirstName.ToLower())@$Domain"; FirstName = $_.FirstName; LastName = $_.LastName } } }
        }

        $credentialsList = @()
        foreach ($mb in $mailboxResults.Created) {
            $credentialsList += @{ Username = $mb.Email; Password = $InboxPassword; FirstName = $mb.FirstName; LastName = $mb.LastName; Domain = $Domain }
        }
        $credentialsJson = $credentialsList | ConvertTo-Json -Depth 5 -Compress

        if (-not $DryRun) { $actualMailboxCount = Get-ActualMailboxCount -Domain $Domain }

        $canComplete = $true
        if ($actualMailboxCount -lt $minMailboxes) {
            $canComplete = $false
            if ($failedSteps -notcontains "Mailbox Creation") { $failedSteps += "Mailbox Creation"; $errorMessages += "Only $actualMailboxCount mailboxes" }
        }
        if (-not $dkimSuccess) {
            $canComplete = $false
            if ($failedSteps -notcontains "DKIM Setup") { $failedSteps += "DKIM Setup" }
        }

        if ($canComplete) {
            $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "PROVISIONING COMPLETE: $actualMailboxCount resource mailboxes, DKIM enabled"
            Update-AirtableDomain -RecordId $RecordId -Status "Completed" -InterimStatus "Both - Provisioning Complete" -CreatedUserJSON $credentialsJson -ClearFailedSteps -ActionHistory $actionHistory
            Write-Host "  [COMPLETE] $Domain - $actualMailboxCount resource mailboxes, DKIM enabled" -ForegroundColor Green
        } else {
            $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "FAILED: $($failedSteps -join ', ') - $($errorMessages -join '; ')"
            Update-AirtableDomain -RecordId $RecordId -Status "Failed" -InterimStatus "Both - Failed" -StepFailedAt $failedSteps -ErrorMessage ($errorMessages -join "; ") -CreatedUserJSON $credentialsJson -ActionHistory $actionHistory
            Write-Host "  [FAILED] $Domain - Issues: $($failedSteps -join ', ')" -ForegroundColor Red
        }
    }

    Write-Host ""
}

# ============================================================================
# POST-PROVISIONING: FETCH ORDERS FOR ACTIONS
# ============================================================================
function Get-AirtableActionOrders {
    param([string]$InterimStatusFilter)

    Write-Log "Fetching orders with Interim Status = '$InterimStatusFilter'..." -Level Info

    $filterFormula = [System.Web.HttpUtility]::UrlEncode("{Interim Status} = '$InterimStatusFilter'")
    $query = "filterByFormula=$filterFormula"

    $result = Invoke-AirtableApi -Method GET -TableId $AirtableConfig.DomainsTableId -Query $query
    if ($result.Success) {
        $orders = @()
        foreach ($record in $result.Data.records) {
            $namesJson = $record.fields.'Detailed Usernames'
            $names = @()
            if ($namesJson) {
                try { $names = $namesJson | ConvertFrom-Json } catch { }
            }

            $orders += @{
                RecordId         = $record.id
                Domain           = $record.fields.'Domain'
                AdminRecordId    = if ($record.fields.'MS Admins') { $record.fields.'MS Admins'[0] } else { $null }
                InboxPassword    = $record.fields.'Inbox Password'
                Status           = if ($record.fields.'Status') { $record.fields.'Status' } else { "" }
                InterimStatus    = if ($record.fields.'Interim Status') { $record.fields.'Interim Status' } else { "" }
                ActionHistory    = if ($record.fields.'Action History') { $record.fields.'Action History' } else { "" }
                ActionRequest    = if ($record.fields.'Action Request') { $record.fields.'Action Request' } else { "" }
                ActionError      = if ($record.fields.'Action Error') { $record.fields.'Action Error' } else { "" }
                Names            = $names
                CreatedUserJSON  = if ($record.fields.'Created User JSON') { $record.fields.'Created User JSON' } else { "" }
            }
        }
        Write-Log "Found $($orders.Count) orders with Interim Status = '$InterimStatusFilter'" -Level Success
        return $orders
    }
    return @()
}

# ============================================================================
# USERNAME CHANGE WORKER
# Inspection-based: checks real M365 state to skip completed steps.
# Interim Status flow:
#   "Both - Name Change Pending" -> "Both - Name Change Processing" ->
#   "Both - Name Change Complete" (or "Both - Failed")
#
# Action Request JSON format:
#   {
#     "type": "name_change",
#     "changes": [
#       { "oldEmail": "sarah@domain.com", "newFirstName": "Jessica", "newLastName": "Smith", "newUsername": "jessica" }
#     ]
#   }
# ============================================================================
function Process-UsernameChange {
    param(
        [hashtable]$OrderInfo,
        [hashtable]$AdminInfo,
        [string]$Bearer
    )

    $Domain = $OrderInfo.Domain
    $RecordId = $OrderInfo.RecordId
    $actionHistory = $OrderInfo.ActionHistory
    $InboxPassword = $OrderInfo.InboxPassword

    Write-Host ""
    Write-Host "================================================================" -ForegroundColor Yellow
    Write-Host "  USERNAME CHANGE: $Domain" -ForegroundColor Yellow
    Write-Host "================================================================" -ForegroundColor Yellow

    # Parse Action Request JSON
    $actionRequest = $null
    try {
        $actionRequest = $OrderInfo.ActionRequest | ConvertFrom-Json
    } catch {
        $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "FAILED: Could not parse Action Request JSON"
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Both - Failed" -ActionError "Invalid Action Request JSON" -ActionHistory $actionHistory
        return
    }

    if (-not $actionRequest.changes -or $actionRequest.changes.Count -eq 0) {
        $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "FAILED: No changes specified in Action Request"
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Both - Failed" -ActionError "No changes in Action Request" -ActionHistory $actionHistory
        return
    }

    $headers = @{ Authorization = "Bearer $Bearer"; "Content-Type" = "application/json" }
    $changes = $actionRequest.changes
    $totalChanges = $changes.Count
    $successCount = 0
    $skipCount = 0
    $failCount = 0

    $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Name change started: $totalChanges change(s) requested"
    Update-AirtableDomain -RecordId $RecordId -InterimStatus "Both - Name Change Processing" -ActionHistory $actionHistory

    foreach ($change in $changes) {
        $oldEmail = $change.oldEmail
        $newFirstName = $change.newFirstName
        $newLastName = $change.newLastName
        $newUsername = if ($change.newUsername) { $change.newUsername } else { $newFirstName.ToLower() }
        $newEmail = "$newUsername@$Domain"
        $newDisplayName = "$newFirstName $newLastName"

        Write-Log "Processing name change: $oldEmail -> $newEmail ($newDisplayName)" -Level Info

        # INSPECTION: Check current M365 state
        $user = $null
        try {
            $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$oldEmail" -Headers $headers -ErrorAction Stop
        } catch {
            # Maybe already renamed — try new email
            try {
                $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$newEmail" -Headers $headers -ErrorAction Stop
            } catch {
                Write-Log "User not found by old ($oldEmail) or new ($newEmail) email" -Level Error
                $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "FAILED: User not found - $oldEmail / $newEmail"
                $failCount++
                continue
            }
        }

        $userId = $user.id
        $currentUPN = $user.userPrincipalName
        $currentGivenName = $user.givenName
        $currentSurname = $user.surname
        $currentDisplayName = $user.displayName

        # INSPECTION: Check if names already match target
        $namesMatch = ($currentGivenName -eq $newFirstName) -and ($currentSurname -eq $newLastName) -and ($currentDisplayName -eq $newDisplayName)
        $emailMatch = ($currentUPN -eq $newEmail)

        if ($namesMatch -and $emailMatch) {
            Write-Log "Already up to date: $newEmail ($newDisplayName) - skipping" -Level Info
            $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "SKIPPED (already done): $oldEmail -> $newEmail"
            $skipCount++
            continue
        }

        # STEP A: Update names (givenName, surname, displayName) via Graph API
        if (-not $namesMatch) {
            try {
                $nameBody = @{
                    givenName = $newFirstName
                    surname = $newLastName
                    displayName = $newDisplayName
                } | ConvertTo-Json -Depth 3
                Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Method PATCH -Headers $headers -Body $nameBody -ErrorAction Stop | Out-Null
                Write-Log "Updated names: $currentDisplayName -> $newDisplayName" -Level Success

                # Also update via Exchange
                try {
                    Set-Mailbox -Identity $currentUPN -DisplayName $newDisplayName -Name $newDisplayName -ErrorAction SilentlyContinue
                } catch { }
            } catch {
                Write-Log "Failed to update names for $oldEmail: $($_.Exception.Message)" -Level Error
                $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "FAILED: Update names for $oldEmail - $($_.Exception.Message)"
                $failCount++
                continue
            }
        }

        # STEP B: Update UPN/email (this makes old email an alias automatically)
        if (-not $emailMatch) {
            try {
                # Update UPN via Graph
                $upnBody = @{
                    userPrincipalName = $newEmail
                    mail = $newEmail
                } | ConvertTo-Json -Depth 3
                Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Method PATCH -Headers $headers -Body $upnBody -ErrorAction Stop | Out-Null
                Write-Log "Updated UPN: $currentUPN -> $newEmail" -Level Success

                # Also update via Exchange
                Start-Sleep -Seconds 3
                try {
                    Set-Mailbox -Identity $userId -MicrosoftOnlineServicesID $newEmail -ErrorAction SilentlyContinue
                    Set-Mailbox -Identity $userId -PrimarySmtpAddress $newEmail -ErrorAction SilentlyContinue
                } catch { }

                $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Renamed: $oldEmail -> $newEmail ($newDisplayName) [old email retained as alias]"
            } catch {
                Write-Log "Failed to update email for $oldEmail -> $newEmail: $($_.Exception.Message)" -Level Error
                $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "FAILED: Update email $oldEmail -> $newEmail - $($_.Exception.Message)"
                $failCount++
                continue
            }
        } else {
            $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Updated names only: $newEmail ($newDisplayName)"
        }

        $successCount++
        Start-Sleep -Milliseconds 500
    }

    # Update Detailed Usernames JSON to reflect new names
    if ($successCount -gt 0 -or $skipCount -gt 0) {
        try {
            $currentNames = @()
            if ($OrderInfo.Names) { $currentNames = @($OrderInfo.Names) }

            foreach ($change in $changes) {
                $oldEmail = $change.oldEmail
                $oldUsername = ($oldEmail -split '@')[0]

                for ($i = 0; $i -lt $currentNames.Count; $i++) {
                    if ($currentNames[$i].username -eq $oldUsername -or $currentNames[$i].email -eq $oldEmail) {
                        $currentNames[$i] = @{
                            firstName = $change.newFirstName
                            lastName = $change.newLastName
                            username = if ($change.newUsername) { $change.newUsername } else { $change.newFirstName.ToLower() }
                            email = "$( if ($change.newUsername) { $change.newUsername } else { $change.newFirstName.ToLower() } )@$Domain"
                        }
                        break
                    }
                }
            }

            $updatedNamesJson = $currentNames | ConvertTo-Json -Depth 5 -Compress
            $body = @{ fields = @{ 'Detailed Usernames' = $updatedNamesJson } }
            Invoke-AirtableApi -Method PATCH -TableId $AirtableConfig.DomainsTableId -RecordId $RecordId -Body $body | Out-Null
        } catch {
            Write-Log "Failed to update Detailed Usernames: $($_.Exception.Message)" -Level Warning
        }
    }

    # Finalize
    $summary = "Name change complete: $successCount succeeded, $skipCount skipped (already done), $failCount failed out of $totalChanges"
    $actionHistory = Add-HistoryEntry -History $actionHistory -Entry $summary

    if ($failCount -eq 0) {
        # Check if re-upload to sending tool is needed
        if ($actionRequest.reuploadToSendingTool -eq $true) {
            $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Re-upload to sending tool requested (pending)"
            Update-AirtableDomain -RecordId $RecordId -InterimStatus "Both - Re-upload Pending" -ActionHistory $actionHistory -ClearActionError
        } else {
            Update-AirtableDomain -RecordId $RecordId -InterimStatus "Both - Name Change Complete" -ActionHistory $actionHistory -ClearActionError
        }
        Write-Host "  [NAME CHANGE COMPLETE] $Domain - $summary" -ForegroundColor Green
    } else {
        Update-AirtableDomain -RecordId $RecordId -InterimStatus "Both - Failed" -ActionError "$failCount name change(s) failed" -ActionHistory $actionHistory
        Write-Host "  [NAME CHANGE FAILED] $Domain - $summary" -ForegroundColor Red
    }

    Write-Host ""
}

# ============================================================================
# RE-UPLOAD WORKER (placeholder - sends credentials to sending tool API)
# Interim Status flow:
#   "Both - Re-upload Pending" -> "Both - Re-upload Processing" ->
#   "Both - Re-upload Complete" (or "Both - Failed")
# ============================================================================
function Process-Reupload {
    param(
        [hashtable]$OrderInfo,
        [hashtable]$AdminInfo,
        [string]$Bearer
    )

    $Domain = $OrderInfo.Domain
    $RecordId = $OrderInfo.RecordId
    $actionHistory = $OrderInfo.ActionHistory

    Write-Host ""
    Write-Host "================================================================" -ForegroundColor Cyan
    Write-Host "  RE-UPLOAD TO SENDING TOOL: $Domain" -ForegroundColor Cyan
    Write-Host "================================================================" -ForegroundColor Cyan

    $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Re-upload to sending tool started"
    Update-AirtableDomain -RecordId $RecordId -InterimStatus "Both - Re-upload Processing" -ActionHistory $actionHistory

    # TODO: Implement sending tool API integration (Instantly, Smartlead, PlusVibe)
    # This will:
    # 1. Read Created User JSON for current credentials
    # 2. Call sending tool API to update/re-upload accounts
    # 3. Update status on completion

    $actionHistory = Add-HistoryEntry -History $actionHistory -Entry "Re-upload to sending tool: NOT YET IMPLEMENTED (requires SendingToolClient)"
    Update-AirtableDomain -RecordId $RecordId -InterimStatus "Both - Re-upload Pending" -ActionHistory $actionHistory -ActionError "Re-upload not yet implemented"

    Write-Host "  [RE-UPLOAD] Not yet implemented - requires SendingToolClient integration" -ForegroundColor Yellow
    Write-Host ""
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

Add-Type -AssemblyName System.Web

if (-not (Get-Module -ListAvailable -Name ExchangeOnlineManagement)) {
    Write-Host "Installing ExchangeOnlineManagement module..." -ForegroundColor Yellow
    Install-Module -Name ExchangeOnlineManagement -Force -AllowClobber -Scope CurrentUser
}
Import-Module ExchangeOnlineManagement -ErrorAction SilentlyContinue

if (-not (Test-Path $OutputFolder)) { New-Item -ItemType Directory -Path $OutputFolder -Force | Out-Null }

Write-Host ""
Write-Host "=========================================================================" -ForegroundColor Cyan
Write-Host "     M365 BULK DOMAIN & RESOURCE MAILBOX CREATION (AIRTABLE)" -ForegroundColor Cyan
Write-Host "=========================================================================" -ForegroundColor Cyan
if ($DryRun) { Write-Host "                      *** DRY RUN MODE ***" -ForegroundColor Yellow }
Write-Host ""

$admins = Get-AirtableAdmins

# ---- PHASE 1: New Order Provisioning (Status = "Processing") ----
$domains = Get-AirtableDomains

# ---- PHASE 2: Post-Provisioning Actions ----
$nameChangeOrders = Get-AirtableActionOrders -InterimStatusFilter "Both - Name Change Pending"
$reuploadOrders = Get-AirtableActionOrders -InterimStatusFilter "Both - Re-upload Pending"

$totalNewOrders = $domains.Count
$totalNameChanges = $nameChangeOrders.Count
$totalReuploads = $reuploadOrders.Count
$totalWork = $totalNewOrders + $totalNameChanges + $totalReuploads

if ($totalWork -eq 0) {
    Write-Host "No work to process (no new orders, name changes, or re-uploads)." -ForegroundColor Yellow
    exit 0
}

if ($MaxDomains -gt 0 -and $domains.Count -gt $MaxDomains) {
    $domains = $domains[0..($MaxDomains-1)]
}

$totalMailboxes = ($domains | ForEach-Object { [Math]::Min($_.Names.Count, $_.MailboxesTarget) } | Measure-Object -Sum).Sum
Write-Host "New orders to provision: $totalNewOrders ($totalMailboxes mailboxes)" -ForegroundColor White
Write-Host "Name changes to process: $totalNameChanges" -ForegroundColor White
Write-Host "Re-uploads to process: $totalReuploads" -ForegroundColor White
Write-Host ""

# Combine all orders that need admin/tenant access
$allOrders = @()
foreach ($d in $domains) { $allOrders += @{ Order = $d; Type = "provision" } }
foreach ($d in $nameChangeOrders) { $allOrders += @{ Order = $d; Type = "name_change" } }
foreach ($d in $reuploadOrders) { $allOrders += @{ Order = $d; Type = "reupload" } }

# Group by admin
$ordersByAdmin = @{}
foreach ($item in $allOrders) {
    $adminId = $item.Order.AdminRecordId
    if (-not $adminId) { Write-Log "Order $($item.Order.Domain) has no linked admin, skipping" -Level Warning; continue }
    if (-not $ordersByAdmin.ContainsKey($adminId)) { $ordersByAdmin[$adminId] = @() }
    $ordersByAdmin[$adminId] += $item
}

$processedAdmins = @{}
$lockedAdmins = @()

# Initialize resource counter for unique display names
$script:resourceCounter = Get-Random -Minimum 10000 -Maximum 99999

foreach ($adminId in $ordersByAdmin.Keys) {
    $adminInfo = $admins[$adminId]
    if (-not $adminInfo) { Write-Log "Admin record $adminId not found" -Level Error; continue }

    $adminEmail = $adminInfo.AdminEmail
    $adminPassword = $adminInfo.AdminPassword

    if (-not $processedAdmins.ContainsKey($adminEmail)) {
        Write-Host ""
        Write-Host "########################################################################" -ForegroundColor Blue
        Write-Host "  TENANT: $adminEmail" -ForegroundColor Blue
        Write-Host "########################################################################" -ForegroundColor Blue

        if (-not $DryRun) {
            $lockAcquired = Request-AdminLock -RecordId $adminId -AdminEmail $adminEmail
            if (-not $lockAcquired) {
                Write-Log "Skipping tenant - locked by another process" -Level Warning
                continue
            }
            $lockedAdmins += @{ RecordId = $adminId; AdminEmail = $adminEmail }
        }

        $adminDomainPart = ($adminEmail -split '@')[1]
        $tenantId = Get-TenantIdFromDomain -Domain $adminDomainPart
        if (-not $tenantId) {
            Write-Log "Could not get tenant ID" -Level Error
            Update-AirtableAdmin -RecordId $adminId -TenantStatus "Failed"
            if (-not $DryRun) { Release-AdminLock -RecordId $adminId -AdminEmail $adminEmail }
            $lockedAdmins = $lockedAdmins | Where-Object { $_.RecordId -ne $adminId }
            continue
        }
        Write-Log "Tenant ID: $tenantId" -Level Success
        Update-AirtableAdmin -RecordId $adminId -TenantId $tenantId

        if (-not $DryRun) {
            $graphToken = Get-ROPCToken -TenantId $tenantId -ClientId $AzureCliPublicClientId -Username $adminEmail -Password $adminPassword
            if (-not $graphToken) {
                Write-Log "Failed to get Graph token (MFA?)" -Level Error
                Update-AirtableAdmin -RecordId $adminId -TenantStatus "MFA Enabled"
                Release-AdminLock -RecordId $adminId -AdminEmail $adminEmail
                $lockedAdmins = $lockedAdmins | Where-Object { $_.RecordId -ne $adminId }
                continue
            }
            Write-Log "Got Graph token" -Level Success
            Update-AirtableAdmin -RecordId $adminId -TenantStatus "Active" -UpdateLastTokenSuccess

            $consentResult = Configure-TenantUserConsent -Bearer $graphToken -TenantId $tenantId -AdminEmail $adminEmail -AdminPassword $adminPassword
            Update-AirtableAdmin -RecordId $adminId -SetUserConsent -UserConsentConfigured ([bool]$consentResult)

            if (-not (Connect-ToExchangeOnline -AdminEmail $adminEmail -AdminPassword $adminPassword)) {
                Update-AirtableAdmin -RecordId $adminId -TenantStatus "Failed"
                Release-AdminLock -RecordId $adminId -AdminEmail $adminEmail
                $lockedAdmins = $lockedAdmins | Where-Object { $_.RecordId -ne $adminId }
                continue
            }
        } else { $graphToken = "DRY_RUN_TOKEN" }

        $processedAdmins[$adminEmail] = @{ TenantId = $tenantId; GraphToken = $graphToken; AdminId = $adminId }
    } else {
        $graphToken = $processedAdmins[$adminEmail].GraphToken
    }

    # Process all orders for this admin, dispatching by type
    foreach ($item in $ordersByAdmin[$adminId]) {
        switch ($item.Type) {
            "provision" {
                Process-Domain -DomainInfo $item.Order -AdminInfo $adminInfo -Bearer $graphToken
            }
            "name_change" {
                Process-UsernameChange -OrderInfo $item.Order -AdminInfo $adminInfo -Bearer $graphToken
            }
            "reupload" {
                Process-Reupload -OrderInfo $item.Order -AdminInfo $adminInfo -Bearer $graphToken
            }
        }
        Start-Sleep -Milliseconds $DelayBetweenDomainsMs
    }

    if (-not $DryRun -and $lockedAdmins.RecordId -contains $adminId) {
        Release-AdminLock -RecordId $adminId -AdminEmail $adminEmail
        $lockedAdmins = $lockedAdmins | Where-Object { $_.RecordId -ne $adminId }
    }
}

foreach ($locked in $lockedAdmins) {
    Release-AdminLock -RecordId $locked.RecordId -AdminEmail $locked.AdminEmail
}

if (-not $DryRun) {
    Write-Log "Disconnecting from Exchange Online..." -Level Info
    Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue
}

Write-Host ""
Write-Host "=========================================================================" -ForegroundColor Green
Write-Host "              PROCESSING COMPLETE" -ForegroundColor Green
Write-Host "=========================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Summary:" -ForegroundColor White
Write-Host "  New orders provisioned: $totalNewOrders" -ForegroundColor White
Write-Host "  Name changes processed: $totalNameChanges" -ForegroundColor White
Write-Host "  Re-uploads processed:   $totalReuploads" -ForegroundColor White
Write-Host ""
Write-Host "Check Airtable for status. Action History field has full audit trail." -ForegroundColor White
Write-Host ""
