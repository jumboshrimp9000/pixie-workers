. (Join-Path $PSScriptRoot "config.ps1")

$script:RecoveryAzureCliPublicClientId = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
$script:RecoveryDomainBlocklists = @(
    @{ Name = "Spamhaus DBL"; Zone = "dbl.spamhaus.org"; Ignore = @("127.255.255.252", "127.255.255.254", "127.255.255.255", "127.0.1.255") },
    @{ Name = "SURBL"; Zone = "multi.surbl.org"; Ignore = @() },
    @{ Name = "URIBL"; Zone = "multi.uribl.com"; Ignore = @("127.0.0.1") },
    @{ Name = "Abusix Domain Blocklist"; Zone = $(if ($env:ABUSIX_DOMAIN_BLOCKLIST_ZONE) { $env:ABUSIX_DOMAIN_BLOCKLIST_ZONE } elseif ($env:ABUSIX_API_KEY) { "$($env:ABUSIX_API_KEY).dblack.mail.abusix.zone" } else { "" }); Ignore = @() },
    @{ Name = "Invaluement ivmURI"; Zone = $(if ($env:INVALUEMENT_IVMURI_ZONE) { $env:INVALUEMENT_IVMURI_ZONE } else { "ivmuri.dnsbl.invaluement.com" }); Ignore = @() }
)

function Ensure-RecoveryExchangeModule {
    if (-not (Get-Module -ListAvailable -Name ExchangeOnlineManagement)) {
        Write-Host "Installing ExchangeOnlineManagement module..." -ForegroundColor Yellow
        Install-Module -Name ExchangeOnlineManagement -Force -AllowClobber -Scope CurrentUser
    }
    Import-Module ExchangeOnlineManagement -ErrorAction SilentlyContinue
}

function Get-RecoveryStepMap {
    param([object]$ActionRecord)

    $map = @{}
    $result = $null
    if ($ActionRecord -and $ActionRecord.result) {
        $result = $ActionRecord.result
        if ($result -is [string]) {
            try { $result = $result | ConvertFrom-Json -Depth 20 } catch { $result = $null }
        }
    }

    foreach ($step in @($result.steps)) {
        if (-not $step) { continue }
        $name = [string]$step.step
        if (-not $name) { continue }
        $map[$name] = [ordered]@{
            step = $name
            status = [string]$step.status
            startedAt = $step.startedAt
            completedAt = $step.completedAt
            attempt = $step.attempt
            details = $step.details
            error = $step.error
        }
    }

    return $map
}

function Save-RecoveryProgress {
    param(
        [string]$ActionId,
        [string]$ActionType,
        [string]$Domain,
        [hashtable]$StepMap,
        [hashtable]$Summary
    )

    $steps = @(
        $StepMap.Values |
            Sort-Object -Property @{
                Expression = {
                    try { [DateTime]::Parse([string]$_.startedAt) } catch { [DateTime]::MinValue }
                }
            }
    )

    $result = [ordered]@{
        checkpoint_version = 1
        type = $ActionType
        domain = $Domain
        action_id = $ActionId
        steps = $steps
        summary = $Summary
        lastUpdated = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    }

    Update-ActionResult -ActionId $ActionId -Result $result
}

function Start-RecoveryStep {
    param(
        [string]$ActionId,
        [string]$ActionType,
        [string]$Domain,
        [string]$StepName,
        [hashtable]$StepMap,
        [hashtable]$Summary,
        [int]$Attempt = 1
    )

    if ($StepMap.ContainsKey($StepName) -and [string]$StepMap[$StepName].status -eq "completed") {
        return $StepMap[$StepName]
    }

    if (-not $StepMap.ContainsKey($StepName)) {
        $StepMap[$StepName] = [ordered]@{ step = $StepName }
    }

    $step = $StepMap[$StepName]
    $step.status = "in_progress"
    if (-not $step.startedAt) {
        $step.startedAt = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    }
    $step.completedAt = $null
    $step.error = $null
    $step.attempt = $Attempt
    Save-RecoveryProgress -ActionId $ActionId -ActionType $ActionType -Domain $Domain -StepMap $StepMap -Summary $Summary
    return $step
}

function Complete-RecoveryStep {
    param(
        [string]$ActionId,
        [string]$ActionType,
        [string]$Domain,
        [hashtable]$StepMap,
        [hashtable]$Summary,
        [string]$StepName,
        [hashtable]$Details = $null
    )

    if (-not $StepMap.ContainsKey($StepName)) {
        $StepMap[$StepName] = [ordered]@{ step = $StepName; startedAt = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ") }
    }
    $step = $StepMap[$StepName]
    $step.status = "completed"
    $step.completedAt = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    if ($Details) { $step.details = $Details }
    $step.error = $null
    Save-RecoveryProgress -ActionId $ActionId -ActionType $ActionType -Domain $Domain -StepMap $StepMap -Summary $Summary
}

function Fail-RecoveryStep {
    param(
        [string]$ActionId,
        [string]$ActionType,
        [string]$Domain,
        [hashtable]$StepMap,
        [hashtable]$Summary,
        [string]$StepName,
        [string]$ErrorMessage
    )

    if (-not $StepMap.ContainsKey($StepName)) {
        $StepMap[$StepName] = [ordered]@{ step = $StepName; startedAt = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ") }
    }
    $step = $StepMap[$StepName]
    $step.status = "failed"
    $step.completedAt = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    $step.error = $ErrorMessage
    Save-RecoveryProgress -ActionId $ActionId -ActionType $ActionType -Domain $Domain -StepMap $StepMap -Summary $Summary
}

function Invoke-RecoveryGraphRequest {
    param(
        [string]$Method,
        [string]$Url,
        [string]$Bearer,
        [object]$Body = $null
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
    if ($null -ne $Body) {
        $params.Body = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 20 }
    }
    Invoke-RestMethod @params
}

function Get-RecoveryTenantIdFromDomain {
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

function Get-RecoveryROPCToken {
    param(
        [string]$TenantId,
        [string]$Username,
        [string]$Password,
        [string]$ScopeString = "https://graph.microsoft.com/.default"
    )

    $tokenUrl = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
    $body = @{
        grant_type = "password"
        client_id = $script:RecoveryAzureCliPublicClientId
        scope = $ScopeString
        username = $Username
        password = $Password
    }

    try {
        $response = Invoke-RestMethod -Method POST -Uri $tokenUrl -Body $body -ContentType "application/x-www-form-urlencoded" -TimeoutSec 30 -ErrorAction Stop
        return $response.access_token
    } catch {
        Write-Log "ROPC token failed: $($_.Exception.Message)" -Level Error
        return $null
    }
}

function Connect-RecoveryExchangeOnline {
    param([string]$Email, [string]$Password)

    $securePwd = ConvertTo-SecureString $Password -AsPlainText -Force
    $creds = New-Object System.Management.Automation.PSCredential($Email, $securePwd)
    Connect-ExchangeOnline -Credential $creds -ShowBanner:$false -ErrorAction Stop
}

function Get-RecoveryPoolRow {
    param([string]$RecoveryPoolId)
    $result = Invoke-SupabaseApi -Method GET -Table "recovery_pool" -Query "id=eq.$RecoveryPoolId&limit=1&select=*"
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
}

function Update-RecoveryPool {
    param(
        [string]$RecoveryPoolId,
        [hashtable]$Fields
    )
    $Fields.updated_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    Invoke-SupabaseApi -Method PATCH -Table "recovery_pool" -Query "id=eq.$RecoveryPoolId" -Body $Fields | Out-Null
}

function Get-RecoveryTenant {
    param([string]$RecoveryTenantId)
    if (-not $RecoveryTenantId) { return $null }
    $result = Invoke-SupabaseApi -Method GET -Table "recovery_tenants" -Query "id=eq.$RecoveryTenantId&limit=1&select=*"
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
}

function Pick-RecoveryTenantWithCapacity {
    $result = Invoke-SupabaseRpc -FunctionName "pick_recovery_tenant_with_capacity"
    if ($result.Success -and $result.Data) {
        $rows = @($result.Data)
        if ($rows.Count -gt 0) { return $rows[0] }
    }
    return $null
}

function Get-RecoveryActionPayloadValue {
    param(
        [object]$Action,
        [string]$Key
    )

    if (-not $Action -or -not $Action.payload) { return $null }
    $payload = $Action.payload
    if ($payload -is [string]) {
        try { $payload = $payload | ConvertFrom-Json -Depth 20 } catch { $payload = $null }
    }
    if (-not $payload) { return $null }
    return $payload.$Key
}

function Get-RecoveryActiveDomainInboxes {
    param([string]$DomainId)

    $result = Invoke-SupabaseApi -Method GET -Table "inboxes" -Query "domain_id=eq.$DomainId&status=eq.active&order=created_at.asc&select=*"
    if ($result.Success) { return @($result.Data) }
    return @()
}

function Get-RecoveryDomainCredentialAssignments {
    param([string]$DomainId)

    $result = Invoke-SupabaseApi -Method GET -Table "domain_credentials" -Query "domain_id=eq.$DomainId&select=credential_id"
    if ($result.Success) { return @($result.Data) }
    return @()
}

function Get-RecoverySendingToolUploadActions {
    param([string]$DomainId)

    $result = Invoke-SupabaseApi -Method GET -Table "actions" -Query "domain_id=eq.$DomainId&type=eq.reupload_inboxes&order=created_at.desc&limit=20&select=*"
    if ($result.Success) { return @($result.Data) }
    return @()
}

function Test-RecoveryUploadActionMatchesReactivation {
    param([object]$Action, [string]$RecoveryActionId)

    if (-not $Action -or -not $Action.payload -or -not $RecoveryActionId) { return $false }
    $payload = $Action.payload
    if ($payload -is [string]) {
        try { $payload = $payload | ConvertFrom-Json -Depth 20 } catch { $payload = $null }
    }
    if (-not $payload) { return $false }
    return ([string]$payload.recovery_reactivate_action_id -eq $RecoveryActionId)
}

function New-RecoverySendingToolUploadAction {
    param(
        [object]$DomainRecord,
        [string]$RecoveryActionId,
        [int]$ExpectedActiveInboxCount,
        [object]$SendingToolSettings = $null
    )

    $payload = @{
        domain = $DomainRecord.domain
        source = "microsoft_recovery_reactivate"
        recovery_reactivate_action_id = $RecoveryActionId
        expected_active_inboxes = $ExpectedActiveInboxCount
    }

    if ($SendingToolSettings) {
        $payload.sending_tool_settings = $SendingToolSettings
    }

    $body = @{
        customer_id = $DomainRecord.customer_id
        domain_id = $DomainRecord.id
        type = "reupload_inboxes"
        status = "pending"
        attempts = 0
        max_attempts = 8
        payload = $payload
    }

    $result = Invoke-SupabaseApi -Method POST -Table "actions" -Body $body
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
}

function Ensure-RecoverySendingToolUploadAction {
    param(
        [object]$DomainRecord,
        [string]$RecoveryActionId,
        [int]$ExpectedActiveInboxCount,
        [object]$SendingToolSettings = $null
    )

    $credentialAssignments = @(Get-RecoveryDomainCredentialAssignments -DomainId $DomainRecord.id)
    if ($credentialAssignments.Count -eq 0) {
        return @{
            Success = $false
            Blocked = $true
            Reason = "No saved sending-tool credential is assigned to this reactivated domain"
        }
    }

    $uploadActions = @(Get-RecoverySendingToolUploadActions -DomainId $DomainRecord.id)
    $matchingActions = @($uploadActions | Where-Object { Test-RecoveryUploadActionMatchesReactivation -Action $_ -RecoveryActionId $RecoveryActionId })

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

    $newAction = New-RecoverySendingToolUploadAction -DomainRecord $DomainRecord -RecoveryActionId $RecoveryActionId -ExpectedActiveInboxCount $ExpectedActiveInboxCount -SendingToolSettings $SendingToolSettings
    if (-not $newAction) {
        return @{
            Success = $false
            Blocked = $false
            Reason = "Failed to enqueue reupload_inboxes action for recovery reactivation"
        }
    }

    return @{ Success = $true; UploadAction = $newAction; Created = $true }
}

function Test-RecoverySendingToolUploadValidation {
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
    if ($result -is [string]) {
        try { $result = $result | ConvertFrom-Json -Depth 20 } catch { $result = $null }
    }
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

function Add-RecoveryActionLog {
    param(
        [object]$Action,
        [string]$DomainId,
        [string]$CustomerId,
        [string]$EventType,
        [string]$Severity,
        [string]$Message,
        [hashtable]$Metadata = $null
    )

    $level = switch ($Severity) {
        "error" { "Error" }
        "warn" { "Warning" }
        default { "Info" }
    }
    Write-Log $Message -Level $level
    Add-ActionLog -ActionId $Action.id -DomainId $DomainId -CustomerId $CustomerId -EventType $EventType -Severity $Severity -Message $Message -Metadata $Metadata
}

function Get-RecoveryCloudflareHeaders {
    if ($env:CLOUDFLARE_GLOBAL_KEY -and $env:CLOUDFLARE_EMAIL) {
        return @{
            "X-Auth-Key" = $env:CLOUDFLARE_GLOBAL_KEY
            "X-Auth-Email" = $env:CLOUDFLARE_EMAIL
            "Content-Type" = "application/json"
        }
    }

    return @{
        "Authorization" = "Bearer $($env:CLOUDFLARE_API_TOKEN)"
        "Content-Type" = "application/json"
    }
}

function Get-CloudflareDnsRecords {
    param([string]$ZoneId)

    $headers = Get-RecoveryCloudflareHeaders
    $records = @()
    $page = 1
    do {
        $response = Invoke-RestMethod -Method GET -Uri "https://api.cloudflare.com/client/v4/zones/$ZoneId/dns_records?page=$page&per_page=100" -Headers $headers -UserAgent "pixie-worker/1.0" -TimeoutSec 30 -ErrorAction Stop
        $records += @($response.result)
        $page++
    } while ($response.result_info -and [int]$response.result_info.page -lt [int]$response.result_info.total_pages)
    return $records
}

function Get-RecoveryCloudflareZoneStatus {
    param([string]$ZoneId)
    if (-not $ZoneId) { return $null }
    $headers = Get-RecoveryCloudflareHeaders
    try {
        $response = Invoke-RestMethod -Method GET -Uri "https://api.cloudflare.com/client/v4/zones/$ZoneId" -Headers $headers -UserAgent "pixie-worker/1.0" -TimeoutSec 30 -ErrorAction Stop
        return [string]$response.result.status
    } catch {
        return $null
    }
}

function Remove-CloudflareDnsRecord {
    param(
        [string]$ZoneId,
        [string]$RecordId
    )
    $headers = Get-RecoveryCloudflareHeaders
    Invoke-RestMethod -Method DELETE -Uri "https://api.cloudflare.com/client/v4/zones/$ZoneId/dns_records/$RecordId" -Headers $headers -UserAgent "pixie-worker/1.0" -TimeoutSec 30 -ErrorAction Stop | Out-Null
}

function Add-RecoveryCloudflareDnsRecord {
    param(
        [string]$ZoneId,
        [string]$Type,
        [string]$Name,
        [string]$Content,
        [int]$TTL = 3600,
        [int]$Priority = -1
    )

    $headers = Get-RecoveryCloudflareHeaders
    $body = @{ type = $Type; name = $Name; content = $Content; ttl = $TTL }
    if ($Priority -ge 0 -and $Type -eq "MX") { $body.priority = $Priority }

    try {
        Invoke-RestMethod -Method POST -Uri "https://api.cloudflare.com/client/v4/zones/$ZoneId/dns_records" -Headers $headers -Body ($body | ConvertTo-Json -Depth 5 -Compress) -UserAgent "pixie-worker/1.0" -TimeoutSec 30 -ErrorAction Stop | Out-Null
        return @{ Success = $true }
    } catch {
        $detail = [string]$_.ErrorDetails.Message
        $statusCode = $null
        if ($_.Exception.Response) { $statusCode = [int]$_.Exception.Response.StatusCode }
        if ($detail -match "already exists|same type|Record already" -or $statusCode -eq 409) {
            return @{ Success = $true; AlreadyExists = $true }
        }
        return @{ Success = $false; Error = $_.Exception.Message }
    }
}

function Set-RecoveryDnsRecords {
    param(
        [string]$ZoneId,
        [string]$Domain,
        [string]$Bearer = $null
    )

    # Primary path: pull the records MS itself wants for this specific domain from
    # /domains/{domain}/serviceConfigurationRecords. Matches the pattern used by
    # Part2 ("Add-M365DnsRecords"). Requires a Graph bearer for the tenant that now
    # owns the domain. Falls back to the hyphen-substituted form if Graph is
    # unreachable (e.g. transient auth issue).
    if ($Bearer) {
        try {
            $headers = @{ Authorization = "Bearer $Bearer" }
            $response = Invoke-RestMethod -Method GET -Uri "https://graph.microsoft.com/v1.0/domains/$Domain/serviceConfigurationRecords" -Headers $headers -TimeoutSec 30 -ErrorAction Stop
            foreach ($rec in @($response.value)) {
                switch ([string]$rec.recordType) {
                    "Mx" {
                        Add-RecoveryCloudflareDnsRecord -ZoneId $ZoneId -Type "MX" -Name "@" -Content ([string]$rec.mailExchange) -Priority ([int]$rec.preference) | Out-Null
                    }
                    "Txt" {
                        if ([string]$rec.text -match "spf") {
                            Add-RecoveryCloudflareDnsRecord -ZoneId $ZoneId -Type "TXT" -Name "@" -Content ([string]$rec.text) | Out-Null
                        }
                    }
                    "CName" {
                        $cnameName = [string]$rec.label -replace "\.$Domain$", ""
                        if ($cnameName) {
                            Add-RecoveryCloudflareDnsRecord -ZoneId $ZoneId -Type "CNAME" -Name $cnameName -Content ([string]$rec.canonicalName) | Out-Null
                        }
                    }
                }
            }
            Add-RecoveryCloudflareDnsRecord -ZoneId $ZoneId -Type "TXT" -Name "_dmarc" -Content "v=DMARC1; p=none" | Out-Null
            return
        } catch {
            Write-Log "Failed to pull serviceConfigurationRecords from Graph for $Domain, falling back to hyphen-form: $($_.Exception.Message)" -Level Warning
        }
    }

    $mxHost = $Domain -replace '\.', '-'
    Add-RecoveryCloudflareDnsRecord -ZoneId $ZoneId -Type "MX" -Name "@" -Content "$mxHost.mail.protection.outlook.com" -Priority 0 | Out-Null
    Add-RecoveryCloudflareDnsRecord -ZoneId $ZoneId -Type "TXT" -Name "@" -Content "v=spf1 include:spf.protection.outlook.com -all" | Out-Null
    Add-RecoveryCloudflareDnsRecord -ZoneId $ZoneId -Type "TXT" -Name "_dmarc" -Content "v=DMARC1; p=none" | Out-Null
    Add-RecoveryCloudflareDnsRecord -ZoneId $ZoneId -Type "CNAME" -Name "autodiscover" -Content "autodiscover.outlook.com" | Out-Null
}

function Remove-RecoveryManagedDnsRecords {
    param(
        [string]$ZoneId,
        [string]$Domain
    )

    $records = @(Get-CloudflareDnsRecords -ZoneId $ZoneId)
    $domainName = $Domain.ToLower()
    foreach ($record in $records) {
        if (-not $record -or -not $record.id) { continue }
        $name = [string]$record.name
        $content = [string]$record.content
        $type = [string]$record.type
        $normalizedName = $name.ToLower()

        $shouldDelete = $false
        if ($type -eq "MX" -and ($normalizedName -eq $domainName -or $normalizedName -eq "@")) { $shouldDelete = $true }
        if ($type -eq "TXT" -and ($normalizedName -eq $domainName -or $normalizedName -eq "@")) {
            if ($content -match "spf1" -or $content -match "^MS=") { $shouldDelete = $true }
        }
        if ($type -eq "CNAME" -and ($normalizedName -eq "autodiscover.$domainName" -or $normalizedName -eq "selector1._domainkey.$domainName" -or $normalizedName -eq "selector2._domainkey.$domainName")) {
            $shouldDelete = $true
        }

        if (-not $shouldDelete) { continue }
        try { Remove-CloudflareDnsRecord -ZoneId $ZoneId -RecordId $record.id } catch { }
    }
}

function Wait-RecoveryExchangeSync {
    param(
        [string]$Domain,
        [int]$MaxWaitSeconds = 600
    )

    $elapsed = 0
    while ($elapsed -lt $MaxWaitSeconds) {
        $accepted = Get-AcceptedDomain -Identity $Domain -ErrorAction SilentlyContinue
        if ($accepted) { return $true }
        Start-Sleep -Seconds 15
        $elapsed += 15
    }
    return $false
}

function Add-RecoveryDomainToM365 {
    param(
        [string]$Bearer,
        [string]$Domain
    )

    try {
        Invoke-RecoveryGraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer | Out-Null
        return @{ Success = $true; AlreadyExists = $true }
    } catch { }

    try {
        Invoke-RecoveryGraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/domains" -Bearer $Bearer -Body @{ id = $Domain } | Out-Null
        return @{ Success = $true }
    } catch {
        return @{ Success = $false; Error = $_.Exception.Message }
    }
}

function Get-RecoveryDomainVerificationRecord {
    param(
        [string]$Bearer,
        [string]$Domain
    )
    $records = Invoke-RecoveryGraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$Domain/verificationDnsRecords" -Bearer $Bearer
    foreach ($record in @($records.value)) {
        if ([string]$record.recordType -eq "Txt") { return [string]$record.text }
    }
    return $null
}

function Verify-RecoveryDomain {
    param(
        [string]$Bearer,
        [string]$Domain,
        [int]$MaxAttempts = 20,
        [int]$WaitSeconds = 30
    )
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            $result = Invoke-RecoveryGraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/domains/$Domain/verify" -Bearer $Bearer
            if ($result.isVerified) { return $true }
        } catch {
            try {
                $existing = Invoke-RecoveryGraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer
                if ($existing.isVerified) { return $true }
            } catch { }
        }
        if ($attempt -lt $MaxAttempts) { Start-Sleep -Seconds $WaitSeconds }
    }
    return $false
}

function Enable-RecoveryDomainEmailService {
    param(
        [string]$Bearer,
        [string]$Domain
    )
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            $current = Invoke-RecoveryGraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer
            $services = @($current.supportedServices)
            if ($services -contains "Email") { return $true }
            $updated = @($services + @("Email") | Select-Object -Unique)
            Invoke-RecoveryGraphRequest -Method PATCH -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer -Body @{ supportedServices = $updated } | Out-Null
            return $true
        } catch {
            if ($attempt -lt 3) { Start-Sleep -Seconds 5 }
        }
    }
    return $false
}

function Get-GraphUsersByDomain {
    param(
        [string]$Bearer,
        [string]$Domain
    )
    $users = New-Object System.Collections.Generic.List[object]
    $suffix = "@$($Domain.ToLower())"
    $url = "https://graph.microsoft.com/v1.0/users?`$select=id,userPrincipalName,mail&`$top=999"
    while ($url) {
        $response = Invoke-RecoveryGraphRequest -Method GET -Url $url -Bearer $Bearer
        foreach ($user in @($response.value)) {
            if (-not $user) { continue }
            $upn = [string]$user.userPrincipalName
            $mail = [string]$user.mail
            if (($upn -and $upn.ToLower().EndsWith($suffix)) -or ($mail -and $mail.ToLower().EndsWith($suffix))) {
                $users.Add($user) | Out-Null
            }
        }
        $url = [string]$response.'@odata.nextLink'
        if (-not $url) { break }
    }
    return $users.ToArray()
}

function Invoke-RecoveryGraphDeletePermanent {
    param(
        [string]$Bearer,
        [string]$DeletedItemId
    )
    try {
        Invoke-RecoveryGraphRequest -Method DELETE -Url "https://graph.microsoft.com/v1.0/directory/deletedItems/$DeletedItemId" -Bearer $Bearer | Out-Null
    } catch { }
}

function Remove-RecoveryGraphUsersByDomain {
    param(
        [string]$Bearer,
        [string]$Domain,
        [string]$AdminEmail = ""
    )
    $count = 0
    foreach ($user in @(Get-GraphUsersByDomain -Bearer $Bearer -Domain $Domain)) {
        if (-not $user.id) { continue }
        $upn = [string]$user.userPrincipalName
        if ($AdminEmail -and $upn.Trim().ToLower() -eq $AdminEmail.Trim().ToLower()) { continue }
        try {
            Invoke-RecoveryGraphRequest -Method DELETE -Url "https://graph.microsoft.com/v1.0/users/$($user.id)" -Bearer $Bearer | Out-Null
            Invoke-RecoveryGraphDeletePermanent -Bearer $Bearer -DeletedItemId $user.id
            $count++
        } catch { }
    }
    return $count
}

function Remove-RecoveryMailboxesByDomain {
    param(
        [string]$Domain,
        [string]$Bearer
    )
    $suffix = "@$($Domain.ToLower())"
    $count = 0
    $mailboxes = @(Get-Mailbox -ResultSize Unlimited -ErrorAction SilentlyContinue | Where-Object {
        $_.PrimarySmtpAddress -and $_.PrimarySmtpAddress.ToString().ToLower().EndsWith($suffix)
    })
    foreach ($mailbox in $mailboxes) {
        try {
            $deletedItemId = [string]$mailbox.ExternalDirectoryObjectId
            Remove-Mailbox -Identity $mailbox.Identity -Confirm:$false -ErrorAction Stop
            if ($deletedItemId) { Invoke-RecoveryGraphDeletePermanent -Bearer $Bearer -DeletedItemId $deletedItemId }
            $count++
        } catch { }
    }
    return $count
}

function Remove-RecoveryAcceptedDomainFromExchange {
    param([string]$Domain)
    try {
        $accepted = Get-AcceptedDomain -Identity $Domain -ErrorAction SilentlyContinue
        if ($accepted) { Remove-AcceptedDomain -Identity $Domain -Confirm:$false -ErrorAction Stop }
        return $true
    } catch {
        return $false
    }
}

function Remove-RecoveryDomainFromGraphWithRetry {
    param(
        [string]$Bearer,
        [string]$Domain,
        [int]$MaxAttempts = 3
    )

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            Invoke-RecoveryGraphRequest -Method DELETE -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer | Out-Null
            return @{ Success = $true; Attempts = $attempt }
        } catch {
            $message = [string]$_.Exception.Message
            if ($message -match "ResourceNotFound|404|Request_ResourceNotFound") {
                return @{ Success = $true; Attempts = $attempt; AlreadyRemoved = $true }
            }
            if ($attempt -lt $MaxAttempts -and $message -match "400|referenced|reference|in use") {
                Start-Sleep -Seconds (10 * $attempt)
                continue
            }
            return @{ Success = $false; Attempts = $attempt; Error = $message }
        }
    }

    return @{ Success = $false; Attempts = $MaxAttempts; Error = "still referenced" }
}

function Remove-OrphanUsersForEmail {
    param([string]$Email, [string]$CorrectUserId, [hashtable]$Headers)
    $orphansDeleted = 0
    try {
        $searchByUPN = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users?`$filter=userPrincipalName eq '$Email'" -Headers $Headers -ErrorAction SilentlyContinue
        if ($searchByUPN.value) {
            foreach ($user in $searchByUPN.value) {
                if ($user.id -ne $CorrectUserId) {
                    try {
                        Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$($user.id)" -Method DELETE -Headers $Headers -ErrorAction Stop
                        $orphansDeleted++
                        Start-Sleep -Seconds 2
                    } catch { }
                }
            }
        }
    } catch { }
    return $orphansDeleted
}

function New-RecoveryRoomMailboxBulk {
    param(
        [string]$Domain,
        [array]$Inboxes,
        [string]$Password,
        [string]$Bearer
    )

    $results = @{ Created = @(); Failed = @() }
    $securePassword = ConvertTo-SecureString $Password -AsPlainText -Force
    $headers = @{ Authorization = "Bearer $Bearer"; "Content-Type" = "application/json" }

    $skuResponse = $null
    try { $skuResponse = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/subscribedSkus" -Headers $headers -ErrorAction SilentlyContinue } catch { }
    $licenseSkuId = if ($skuResponse -and $skuResponse.value) { $skuResponse.value[0].skuId } else { $null }

    $script:resourceCounter = Get-Random -Minimum 10000 -Maximum 99999
    foreach ($inbox in $Inboxes) {
        $firstName = [string]$inbox.first_name
        $lastName = [string]$inbox.last_name
        $username = [string]$inbox.username
        $email = "$username@$Domain"
        $realDisplayName = "$firstName $lastName".Trim()
        if (-not $realDisplayName) { $realDisplayName = $username }
        $script:resourceCounter++
        $tempDisplayName = "$realDisplayName $($script:resourceCounter)"

        $existing = Get-Mailbox -Identity $email -ErrorAction SilentlyContinue
        if ($existing) {
            $results.Created += @{
                InboxId = $inbox.id
                Email = $email
                FirstName = $firstName
                LastName = $lastName
                DisplayName = $realDisplayName
                AlreadyExisted = $true
                ExternalId = [string]$existing.ExternalDirectoryObjectId
            }
            continue
        }

        $directSuccess = $false
        try {
            New-Mailbox -Room -Name $tempDisplayName -DisplayName $tempDisplayName -PrimarySmtpAddress $email -Password $securePassword -ResetPasswordOnNextLogon $false -ErrorAction Stop | Out-Null
            $directSuccess = $true
        } catch { }

        if (-not $directSuccess -and $licenseSkuId) {
            $newUserId = $null
            $licenseAssigned = $false
            try {
                $existingUser = $null
                try { $existingUser = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$email" -Headers $headers -ErrorAction Stop } catch { }
                if ($existingUser) {
                    Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$($existingUser.id)" -Method DELETE -Headers $headers -ErrorAction Stop
                    Start-Sleep -Seconds 10
                }

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
                $licBody = @{ addLicenses = @(@{ skuId = $licenseSkuId }); removeLicenses = @() } | ConvertTo-Json -Depth 5
                Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$newUserId/assignLicense" -Method POST -Headers $headers -Body $licBody | Out-Null
                $licenseAssigned = $true

                $mailboxReady = $false
                for ($waitAttempt = 1; $waitAttempt -le 3; $waitAttempt++) {
                    Start-Sleep -Seconds 45
                    $mbx = Get-Mailbox -Identity $email -ErrorAction SilentlyContinue
                    if ($mbx) { $mailboxReady = $true; break }
                }
                if (-not $mailboxReady) { throw "Mailbox not provisioned after fallback creation" }

                Set-Mailbox -Identity $email -Type Room -ErrorAction Stop

                $remBody = @{ addLicenses = @(); removeLicenses = @($licenseSkuId) } | ConvertTo-Json -Depth 5
                Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$newUserId/assignLicense" -Method POST -Headers $headers -Body $remBody | Out-Null
                $licenseAssigned = $false
                $directSuccess = $true
            } catch {
                $results.Failed += @{ InboxId = $inbox.id; Email = $email; Error = $_.Exception.Message }
            } finally {
                if ($licenseAssigned -and $newUserId) {
                    try {
                        $remBody = @{ addLicenses = @(); removeLicenses = @($licenseSkuId) } | ConvertTo-Json -Depth 5
                        Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$newUserId/assignLicense" -Method POST -Headers $headers -Body $remBody | Out-Null
                    } catch { }
                }
            }
        }

        if (-not $directSuccess) { continue }

        Start-Sleep -Seconds 5
        $mailbox = Get-Mailbox -Identity $email -ErrorAction SilentlyContinue
        $results.Created += @{
            InboxId = $inbox.id
            Email = $email
            FirstName = $firstName
            LastName = $lastName
            DisplayName = $realDisplayName
            AlreadyExisted = $false
            ExternalId = if ($mailbox) { [string]$mailbox.ExternalDirectoryObjectId } else { $null }
        }
    }

    if ($results.Created.Count -gt 0) {
        foreach ($mb in $results.Created) {
            $email = $mb.Email
            $realDisplayName = $mb.DisplayName
            try {
                Set-Mailbox -Identity $email -DisplayName $realDisplayName -Name $realDisplayName -ErrorAction Stop
            } catch { }
            try {
                Set-CalendarProcessing -Identity $email -AutomateProcessing None -DeleteComments $false -DeleteSubject $false -RemovePrivateProperty $false -DeleteNonCalendarItems $false -ErrorAction Stop -WarningAction SilentlyContinue
            } catch { }
        }
    }

    return $results
}

function Enable-RecoveryTenantSMTPAuth {
    try {
        $tc = Get-TransportConfig
        if (-not $tc.SmtpClientAuthenticationDisabled) { return $true }
        Set-TransportConfig -SmtpClientAuthenticationDisabled $false
        return $true
    } catch {
        return $false
    }
}

function Setup-RecoveryDomainDKIM {
    param([string]$Domain)
    $dk = $null
    try { $dk = Get-DkimSigningConfig -Identity $Domain -ErrorAction Stop -WarningAction SilentlyContinue } catch { }
    if ($dk -and (-not $dk.Selector1CNAME -or -not $dk.Selector2CNAME)) {
        try { Remove-DkimSigningConfig -Identity $Domain -Confirm:$false -ErrorAction Stop; $dk = $null; Start-Sleep -Seconds 3 } catch { }
    }
    if (-not $dk) {
        try { New-DkimSigningConfig -DomainName $Domain -Enabled $false -ErrorAction Stop -WarningAction SilentlyContinue | Out-Null } catch { }
        Start-Sleep -Seconds 5
    }
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try { $dk = Get-DkimSigningConfig -Identity $Domain -ErrorAction Stop -WarningAction SilentlyContinue } catch { $dk = $null }
        if ($dk -and $dk.Selector1CNAME -and $dk.Selector2CNAME) {
            return @{
                Success = $true
                Selector1CNAME = [string]$dk.Selector1CNAME
                Selector2CNAME = [string]$dk.Selector2CNAME
                AlreadyEnabled = [bool]$dk.Enabled
            }
        }
        Start-Sleep -Seconds 10
    }
    return @{ Success = $false; Error = "DKIM selectors not available" }
}

function Complete-RecoveryDKIMSetup {
    param(
        [string]$Domain,
        [string]$ZoneId,
        [string]$Selector1CNAME,
        [string]$Selector2CNAME
    )
    Add-RecoveryCloudflareDnsRecord -ZoneId $ZoneId -Type "CNAME" -Name "selector1._domainkey" -Content $Selector1CNAME | Out-Null
    Add-RecoveryCloudflareDnsRecord -ZoneId $ZoneId -Type "CNAME" -Name "selector2._domainkey" -Content $Selector2CNAME | Out-Null
    for ($attempt = 1; $attempt -le 5; $attempt++) {
        try {
            Set-DkimSigningConfig -Identity $Domain -Enabled $true -ErrorAction Stop -WarningAction SilentlyContinue
            return $true
        } catch {
            if ($attempt -lt 5) { Start-Sleep -Seconds (60 * $attempt) }
        }
    }
    return $false
}

function Add-RecoveryMailboxToInstantly {
    param(
        [string]$Email,
        [string]$Password
    )
    $body = @{
        email = $Email
        password = $Password
        smtp_host = "smtp.office365.com"
        smtp_port = 587
        imap_host = "outlook.office365.com"
        imap_port = 993
        type = "microsoft"
    }
    $response = Invoke-RestMethod -Method POST -Uri "https://api.instantly.ai/api/v2/accounts" -Headers @{ Authorization = "Bearer $($env:INSTANTLY_RECOVERY_API_KEY)" } -Body ($body | ConvertTo-Json -Depth 10) -ContentType "application/json" -TimeoutSec 30 -ErrorAction Stop
    foreach ($candidate in @($response.id, $response.account_id, $response.accountId, $response.email, $Email)) {
        if ($null -ne $candidate -and [string]$candidate) { return [string]$candidate }
    }
    return [string]$Email
}

function Enable-RecoveryInstantlyWarmup {
    param([string]$Email)
    $payloads = @(
        @{ emails = @($Email) },
        @{ account_emails = @($Email) },
        @{ accounts = @($Email) },
        @{ emails = @($Email); enabled = $true }
    )
    foreach ($payload in $payloads) {
        try {
            Invoke-RestMethod -Method POST -Uri "https://api.instantly.ai/api/v2/accounts/warmup/enable" -Headers @{ Authorization = "Bearer $($env:INSTANTLY_RECOVERY_API_KEY)" } -Body ($payload | ConvertTo-Json -Depth 10) -ContentType "application/json" -TimeoutSec 30 -ErrorAction Stop | Out-Null
            return $true
        } catch { }
    }
    return $false
}

function Remove-RecoveryInstantlyAccount {
    param([string]$InstantlyAccountId)
    if (-not $InstantlyAccountId) { return }
    try {
        Invoke-RestMethod -Method DELETE -Uri "https://api.instantly.ai/api/v2/accounts/$([System.Uri]::EscapeDataString($InstantlyAccountId))" -Headers @{ Authorization = "Bearer $($env:INSTANTLY_RECOVERY_API_KEY)" } -TimeoutSec 30 -ErrorAction Stop | Out-Null
    } catch {
        try {
            Invoke-RestMethod -Method DELETE -Uri "https://api.instantly.ai/api/v2/accounts/$([System.Uri]::EscapeDataString($InstantlyAccountId.ToLower()))" -Headers @{ Authorization = "Bearer $($env:INSTANTLY_RECOVERY_API_KEY)" } -TimeoutSec 30 -ErrorAction Stop | Out-Null
        } catch { }
    }
}

function Get-RecoveryInstantlyAccount {
    param([string]$InstantlyAccountId)
    $response = Invoke-RestMethod -Method GET -Uri "https://api.instantly.ai/api/v2/accounts/$([System.Uri]::EscapeDataString($InstantlyAccountId))" -Headers @{ Authorization = "Bearer $($env:INSTANTLY_RECOVERY_API_KEY)" } -TimeoutSec 30 -ErrorAction Stop
    if ($response.account) { return $response.account }
    return $response
}

function Get-RecoveryHealthScoreFromInstantlyAccount {
    param([object]$Account)
    foreach ($propertyName in @("health_score", "healthScore", "score")) {
        $value = $Account.$propertyName
        if ($null -ne $value -and "$value" -ne "") { return [double]$value }
    }
    if ($Account.health -and $Account.health.score) { return [double]$Account.health.score }
    if ($Account.warmup -and $Account.warmup.health_score) { return [double]$Account.warmup.health_score }
    throw "Instantly account response did not include a health score"
}

function Get-RecoveryHealthLight {
    param([double]$Score)
    if ($Score -lt 50) { return "red" }
    if ($Score -lt 75) { return "yellow" }
    return "green"
}

function Test-RecoveryDnsblListings {
    param([string]$Domain)
    $listings = @()
    foreach ($item in $script:RecoveryDomainBlocklists) {
        $zone = [string]$item.Zone
        if (-not $zone) { continue }
        try {
            $job = Start-Job -ScriptBlock {
                param($LookupName, $IgnoreCodes)
                try {
                    $records = @(Resolve-DnsName -Name $LookupName -Type A -QuickTimeout -ErrorAction Stop)
                    foreach ($record in $records) {
                        $ip = [string]$record.IPAddress
                        if ($ip -and (-not (@($IgnoreCodes) -contains $ip))) {
                            return $true
                        }
                    }
                    return $false
                } catch {
                    return $false
                }
            } -ArgumentList "$Domain.$zone", @($item.Ignore)
            $finished = Wait-Job -Job $job -Timeout 2
            if ($finished) {
                $listed = Receive-Job -Job $job
                if ($listed) {
                    $listings += @{ zone = [string]$item.Name }
                }
            }
            Remove-Job -Job $job -Force | Out-Null
        } catch { }
    }
    return $listings
}

function Invoke-RecoveryReadyEmail {
    param([string]$RecoveryPoolId)
    $baseUrl = [string]$env:PIXIE_APP_API_BASE_URL
    if (-not $baseUrl) { $baseUrl = "https://app.simpleinboxes.com/api/v1" }
    $uri = "$($baseUrl.TrimEnd('/'))/internal/recovery/fire-ready-email"
    $body = @{ recoveryPoolId = $RecoveryPoolId } | ConvertTo-Json -Depth 5
    Invoke-RestMethod -Method POST -Uri $uri -Headers @{ "X-Cron-Secret" = $env:CRON_SECRET } -Body $body -ContentType "application/json" -TimeoutSec 30 -ErrorAction Stop | Out-Null
}

function Remove-RecoveryTenantCapacityCount {
    param([object]$TenantRow)
    if (-not $TenantRow -or -not $TenantRow.id) { return }
    $current = 0
    if ($null -ne $TenantRow.current_domain_count -and "$($TenantRow.current_domain_count)" -ne "") {
        $current = [int]$TenantRow.current_domain_count
    }
    Invoke-SupabaseApi -Method PATCH -Table "recovery_tenants" -Query "id=eq.$($TenantRow.id)" -Body @{
        current_domain_count = [Math]::Max(0, $current - 1)
        updated_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    } | Out-Null
}

function Get-RecoveryPoolZoneId {
    param([object]$RecoveryPoolRow)
    if ($RecoveryPoolRow.cloudflare_zone_id) { return [string]$RecoveryPoolRow.cloudflare_zone_id }
    $previousConfig = $RecoveryPoolRow.previous_config
    if ($previousConfig -is [string]) {
        try { $previousConfig = $previousConfig | ConvertFrom-Json -Depth 20 } catch { $previousConfig = $null }
    }
    if ($previousConfig) {
        $zoneId = [string]$previousConfig.cloudflare_zone_id
        if ($zoneId) { return $zoneId }
    }
    if ($RecoveryPoolRow.original_domain_id) {
        $domain = Get-Domain -DomainId $RecoveryPoolRow.original_domain_id
        if ($domain -and $domain.cloudflare_zone_id) { return [string]$domain.cloudflare_zone_id }
    }
    return $null
}

function Remove-RecoveryPoolOriginalState {
    param([object]$RecoveryPoolRow)
    if ($RecoveryPoolRow.original_domain_id) {
        Invoke-SupabaseApi -Method DELETE -Table "inboxes" -Query "domain_id=eq.$($RecoveryPoolRow.original_domain_id)" | Out-Null
        Invoke-SupabaseApi -Method DELETE -Table "domains" -Query "id=eq.$($RecoveryPoolRow.original_domain_id)" | Out-Null
    }
}

function Remove-RecoveryPoolRow {
    param([string]$RecoveryPoolId)
    Invoke-SupabaseApi -Method DELETE -Table "recovery_pool" -Query "id=eq.$RecoveryPoolId" | Out-Null
}
