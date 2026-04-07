<#
.SYNOPSIS
    Shared configuration and Supabase helper functions for the provisioning pipeline.
.DESCRIPTION
    Provides Supabase REST API wrappers that replace Airtable in the original script.
    All pipeline scripts dot-source this file.
#>

# ============================================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================================
$envFile = Join-Path $PSScriptRoot ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
            $key = $matches[1].Trim()
            $val = $matches[2].Trim()
            [System.Environment]::SetEnvironmentVariable($key, $val, "Process")
        }
    }
}

$SupabaseConfig = @{
    Url            = $env:SUPABASE_URL
    ServiceRoleKey = $env:SUPABASE_SERVICE_ROLE_KEY
}

$CloudflareConfig = @{
    ApiToken  = $env:CLOUDFLARE_API_TOKEN
    AccountId = $env:CLOUDFLARE_ACCOUNT_ID
}

$DynadotConfig = @{
    ApiKey = $env:DYNADOT_API_KEY
}

$AzureCliPublicClientId = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"

# ============================================================================
# LOGGING
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
# SUPABASE REST API
# ============================================================================
function Invoke-SupabaseApi {
    param(
        [string]$Method,
        [string]$Table,
        [string]$Query = "",
        [object]$Body = $null,
        [hashtable]$ExtraHeaders = @{}
    )

    $headers = @{
        "apikey"        = $SupabaseConfig.ServiceRoleKey
        "Authorization" = "Bearer $($SupabaseConfig.ServiceRoleKey)"
        "Content-Type"  = "application/json"
        "Prefer"        = "return=representation"
    }
    foreach ($key in $ExtraHeaders.Keys) { $headers[$key] = $ExtraHeaders[$key] }

    $url = "$($SupabaseConfig.Url)/rest/v1/$Table"
    if ($Query) { $url += "?$Query" }

    $params = @{
        Method      = $Method
        Uri         = $url
        Headers     = $headers
        UserAgent   = "pixie-worker/1.0 (PowerShell)"
        TimeoutSec  = 60
        ErrorAction = "Stop"
    }

    if ($Body) {
        $params.Body = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 10 -Compress }
    }

    try {
        $response = Invoke-RestMethod @params
        return @{ Success = $true; Data = $response }
    } catch {
        $errorMsg = $_.Exception.Message
        if ($_.ErrorDetails.Message) {
            try { $errorMsg = ($_.ErrorDetails.Message | ConvertFrom-Json).message } catch { $errorMsg = $_.ErrorDetails.Message }
        }
        Write-Log "Supabase API error ($Table): $errorMsg" -Level Error
        return @{ Success = $false; Error = $errorMsg }
    }
}

function Invoke-SupabaseRpc {
    param(
        [string]$FunctionName,
        [object]$Body = $null,
        [hashtable]$ExtraHeaders = @{}
    )

    $headers = @{
        "apikey"        = $SupabaseConfig.ServiceRoleKey
        "Authorization" = "Bearer $($SupabaseConfig.ServiceRoleKey)"
        "Content-Type"  = "application/json"
        "Prefer"        = "return=representation"
    }
    foreach ($key in $ExtraHeaders.Keys) { $headers[$key] = $ExtraHeaders[$key] }

    $url = "$($SupabaseConfig.Url)/rest/v1/rpc/$FunctionName"
    $params = @{
        Method      = "POST"
        Uri         = $url
        Headers     = $headers
        UserAgent   = "pixie-worker/1.0 (PowerShell)"
        TimeoutSec  = 60
        ErrorAction = "Stop"
    }

    if ($Body) {
        $params.Body = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 10 -Compress }
    }

    try {
        $response = Invoke-RestMethod @params
        return @{ Success = $true; Data = $response }
    } catch {
        $errorMsg = $_.Exception.Message
        if ($_.ErrorDetails.Message) {
            try { $errorMsg = ($_.ErrorDetails.Message | ConvertFrom-Json).message } catch { $errorMsg = $_.ErrorDetails.Message }
        }
        Write-Log "Supabase RPC error ($FunctionName): $errorMsg" -Level Error
        return @{ Success = $false; Error = $errorMsg }
    }
}

# ============================================================================
# SUPABASE TABLE HELPERS
# ============================================================================

function Get-PendingActions {
    param(
        [string[]]$ActionTypes = @("provision_inbox"),
        [int]$Limit = 10
    )

    $validTypes = @($ActionTypes | Where-Object { $_ -and $_.Trim() } | ForEach-Object { $_.Trim() })
    if ($validTypes.Count -eq 0) { return @() }

    $encodedTypes = ($validTypes -join ",")
    $query = "type=in.($encodedTypes)&status=in.(pending,in_progress)&order=created_at.asc&limit=$Limit"
    $result = Invoke-SupabaseApi -Method GET -Table "actions" -Query $query
    if (-not $result.Success) { return @() }

    $now = Get-Date
    return @($result.Data | Where-Object {
        $attempts = if ($_.attempts -ne $null) { [int]$_.attempts } else { 0 }
        $maxAttempts = if ($_.max_attempts -ne $null) { [int]$_.max_attempts } else { 3 }
        if ($attempts -ge $maxAttempts) { return $false }

        $nextRetryAt = $null
        if ($_.next_retry_at) {
            try { $nextRetryAt = [DateTime]::Parse($_.next_retry_at).ToUniversalTime() } catch { $nextRetryAt = $null }
        }

        if ($nextRetryAt -and $nextRetryAt -gt $now.ToUniversalTime()) {
            return $false
        }

        return $true
    })
}

function Claim-Action {
    param([object]$Action)

    if (-not $Action -or -not $Action.id) { return $null }
    $attempts = if ($Action.attempts -ne $null) { [int]$Action.attempts } else { 0 }
    $body = @{
        status = "in_progress"
        attempts = $attempts + 1
        started_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
        error = $null
    }

    $result = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$($Action.id)&attempts=eq.$attempts" -Body $body
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
}

function Update-ActionStatus {
    param(
        [string]$ActionId,
        [string]$Status,
        [string]$Error = $null,
        [object]$Result = $null,
        [string]$NextRetryAt = $null
    )

    $body = @{ status = $Status; updated_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ") }
    if ($Status -eq "in_progress") { $body.started_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ") }
    if ($Status -eq "completed") { $body.completed_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ") }
    if ($Error) { $body.error = $Error }
    if ($Result) { $body.result = $Result }
    if ($PSBoundParameters.ContainsKey("NextRetryAt")) { $body.next_retry_at = $NextRetryAt }

    Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$ActionId" -Body $body | Out-Null
}

function Fail-Action {
    param(
        [object]$Action,
        [string]$ErrorMessage,
        [int]$DefaultMaxRetries = 5
    )

    if (-not $Action -or -not $Action.id) { return }

    $attempts = if ($Action.attempts -ne $null) { [int]$Action.attempts } else { 1 }
    $maxRetries = if ($Action.max_attempts -ne $null) { [int]$Action.max_attempts } else { $DefaultMaxRetries }
    $isFinal = $attempts -ge $maxRetries
    $delaySeconds = [Math]::Min([Math]::Pow(2, [Math]::Max(0, $attempts - 1)), 300)
    $nextRetryAt = if ($isFinal) { $null } else { (Get-Date).AddSeconds($delaySeconds).ToString("yyyy-MM-ddTHH:mm:ssZ") }

    $body = @{
        status = if ($isFinal) { "failed" } else { "pending" }
        error = if ($ErrorMessage) { $ErrorMessage.Substring(0, [Math]::Min(4000, $ErrorMessage.Length)) } else { "Unknown error" }
        next_retry_at = $nextRetryAt
        updated_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    }

    Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$($Action.id)" -Body $body | Out-Null
}

function Update-ActionResult {
    param(
        [string]$ActionId,
        [object]$Result
    )
    $body = @{ result = $Result; updated_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ") }
    Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$ActionId" -Body $body | Out-Null
}

function Get-Action {
    param([string]$ActionId)
    $result = Invoke-SupabaseApi -Method GET -Table "actions" -Query "id=eq.$ActionId&limit=1&select=*"
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
}

function Get-Domain {
    param([string]$DomainId)
    $result = Invoke-SupabaseApi -Method GET -Table "domains" -Query "id=eq.$DomainId&select=*"
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
}

function Get-DomainInboxes {
    param([string]$DomainId, [string]$Status = "pending")
    $result = Invoke-SupabaseApi -Method GET -Table "inboxes" -Query "domain_id=eq.$DomainId&status=eq.$Status&select=*"
    if ($result.Success) { return $result.Data }
    return @()
}

function Update-Domain {
    param(
        [string]$DomainId,
        [hashtable]$Fields
    )
    $Fields.updated_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    Invoke-SupabaseApi -Method PATCH -Table "domains" -Query "id=eq.$DomainId" -Body $Fields | Out-Null
}

function Update-Inbox {
    param(
        [string]$InboxId,
        [hashtable]$Fields
    )
    $Fields.updated_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    Invoke-SupabaseApi -Method PATCH -Table "inboxes" -Query "id=eq.$InboxId" -Body $Fields | Out-Null
}

function Get-InboxesByIds {
    param(
        [string]$DomainId,
        [string[]]$InboxIds
    )

    $validIds = @($InboxIds | Where-Object { $_ -and $_.Trim() } | ForEach-Object { $_.Trim() })
    if ($validIds.Count -eq 0) { return @() }
    $encoded = ($validIds -join ",")
    $result = Invoke-SupabaseApi -Method GET -Table "inboxes" -Query "domain_id=eq.$DomainId&id=in.($encoded)&order=created_at.asc&select=*"
    if ($result.Success) { return $result.Data }
    return @()
}

function Get-AvailableAdmin {
    param([string]$Provider = "microsoft")

    # Round-robin: pick the admin with lowest usage_count
    $query = "provider=eq.$Provider&active=eq.true&order=usage_count.asc&limit=1"
    $result = Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query $query
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
}

function Test-ActiveAdminExists {
    param([string]$Provider = "microsoft")

    $query = "provider=eq.$Provider&active=eq.true&select=id&limit=1"
    $result = Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query $query
    return ($result.Success -and $result.Data -and $result.Data.Count -gt 0)
}

function Acquire-MicrosoftAdminLock {
    param(
        [string]$ActionId,
        [string]$DomainId,
        [string]$PreferredAdminId = $null,
        [int]$LeaseSeconds = 7200
    )

    $body = @{
        p_action_id = $ActionId
        p_domain_id = $DomainId
        p_lease_seconds = $LeaseSeconds
    }
    if ($PreferredAdminId) { $body.p_preferred_admin_id = $PreferredAdminId }

    $result = Invoke-SupabaseRpc -FunctionName "acquire_microsoft_admin_lock" -Body $body
    if ($result.Success -and $result.Data) {
        $rows = @($result.Data)
        if ($rows.Count -gt 0) { return $rows[0] }
    }
    return $null
}

function Refresh-MicrosoftAdminLock {
    param(
        [string]$ActionId,
        [int]$LeaseSeconds = 7200
    )

    $body = @{
        p_action_id = $ActionId
        p_lease_seconds = $LeaseSeconds
    }

    $result = Invoke-SupabaseRpc -FunctionName "refresh_microsoft_admin_lock" -Body $body
    if ($result.Success -and $result.Data) {
        $rows = @($result.Data)
        if ($rows.Count -gt 0) { return $rows[0] }
    }
    return $null
}

function Release-MicrosoftAdminLock {
    param([string]$ActionId)

    if (-not $ActionId) { return 0 }
    $result = Invoke-SupabaseRpc -FunctionName "release_microsoft_admin_lock" -Body @{ p_action_id = $ActionId }
    if ($result.Success -and $null -ne $result.Data) { return [int]$result.Data }
    return 0
}

function Ensure-DomainAdminAssignment {
    param(
        [string]$DomainId,
        [string]$AdminCredId
    )

    if (-not $DomainId -or -not $AdminCredId) { return }

    $existing = Invoke-SupabaseApi -Method GET -Table "domain_admin_assignments" -Query "domain_id=eq.$DomainId&admin_cred_id=eq.$AdminCredId&order=assigned_at.desc&limit=1&select=id"
    if ($existing.Success -and $existing.Data -and $existing.Data.Count -gt 0) { return }

    New-DomainAdminAssignment -DomainId $DomainId -AdminCredId $AdminCredId
}

function Requeue-ActionWithoutPenalty {
    param(
        [object]$Action,
        [string]$Reason,
        [int]$DelaySeconds = 60
    )

    if (-not $Action -or -not $Action.id) { return }

    $currentAttempts = if ($Action.attempts -ne $null) { [int]$Action.attempts } else { 1 }
    $restoredAttempts = [Math]::Max(0, $currentAttempts - 1)
    $nextRetryAt = (Get-Date).AddSeconds($DelaySeconds).ToString("yyyy-MM-ddTHH:mm:ssZ")
    $body = @{
        status = "pending"
        error = if ($Reason) { $Reason.Substring(0, [Math]::Min(4000, $Reason.Length)) } else { "Pending retry" }
        next_retry_at = $nextRetryAt
        attempts = $restoredAttempts
        started_at = $null
        completed_at = $null
        updated_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    }

    Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$($Action.id)" -Body $body | Out-Null
}

function Update-AdminUsage {
    param(
        [string]$AdminId,
        [int]$InboxCount
    )
    # First get current usage
    $result = Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query "id=eq.$AdminId&select=usage_count"
    if ($result.Success -and $result.Data.Count -gt 0) {
        $current = if ($result.Data[0].usage_count) { [int]$result.Data[0].usage_count } else { 0 }
        $body = @{
            usage_count = $current + $InboxCount
            last_used = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
        }
        Invoke-SupabaseApi -Method PATCH -Table "admin_credentials" -Query "id=eq.$AdminId" -Body $body | Out-Null
    }
}

function New-DomainAdminAssignment {
    param(
        [string]$DomainId,
        [string]$AdminCredId
    )
    $body = @{ domain_id = $DomainId; admin_cred_id = $AdminCredId }
    Invoke-SupabaseApi -Method POST -Table "domain_admin_assignments" -Body $body | Out-Null
}

function Get-AssignedAdmin {
    param([string]$DomainId)

    $assignmentResult = Invoke-SupabaseApi -Method GET -Table "domain_admin_assignments" -Query "domain_id=eq.$DomainId&order=assigned_at.desc&limit=1&select=*"
    if (-not $assignmentResult.Success -or -not $assignmentResult.Data -or $assignmentResult.Data.Count -eq 0) {
        return $null
    }

    $adminCredId = $assignmentResult.Data[0].admin_cred_id
    if (-not $adminCredId) { return $null }

    $adminResult = Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query "id=eq.$adminCredId&limit=1&select=*"
    if ($adminResult.Success -and $adminResult.Data -and $adminResult.Data.Count -gt 0) {
        return $adminResult.Data[0]
    }

    return $null
}

function Add-ActionLog {
    param(
        [string]$ActionId,
        [string]$DomainId,
        [string]$CustomerId,
        [string]$EventType,
        [string]$Severity = "info",
        [string]$Message,
        [object]$Metadata = $null
    )
    $body = @{
        action_id  = $ActionId
        domain_id  = $DomainId
        customer_id = $CustomerId
        event_type = $EventType
        severity   = $Severity
        message    = $Message
    }
    if ($Metadata) { $body.metadata = $Metadata }
    Invoke-SupabaseApi -Method POST -Table "action_logs" -Body $body | Out-Null
}

function Get-MutationRequest {
    param([string]$RequestId)
    $result = Invoke-SupabaseApi -Method GET -Table "domain_mutation_requests" -Query "id=eq.$RequestId&limit=1&select=*"
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
}

function Update-MutationRequest {
    param(
        [string]$RequestId,
        [hashtable]$Fields
    )
    $Fields.updated_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    Invoke-SupabaseApi -Method PATCH -Table "domain_mutation_requests" -Query "id=eq.$RequestId" -Body $Fields | Out-Null
}

function Get-MutationItems {
    param([string]$RequestId)
    $result = Invoke-SupabaseApi -Method GET -Table "domain_mutation_items" -Query "request_id=eq.$RequestId&order=sort_order.asc&select=*"
    if ($result.Success) { return $result.Data }
    return @()
}

function Update-MutationItem {
    param(
        [string]$ItemId,
        [hashtable]$Fields
    )
    $Fields.updated_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    Invoke-SupabaseApi -Method PATCH -Table "domain_mutation_items" -Query "id=eq.$ItemId" -Body $Fields | Out-Null
}

function Add-MutationEvent {
    param(
        [hashtable]$Fields
    )

    $cleanFields = @{}
    foreach ($key in $Fields.Keys) {
        $value = $Fields[$key]
        if ($null -eq $value) { continue }
        if ($value -is [string] -and -not $value.Trim()) { continue }
        $cleanFields[$key] = $value
    }

    Invoke-SupabaseApi -Method POST -Table "domain_mutation_events" -Body $cleanFields | Out-Null
}

function Upsert-InboxEmailAlias {
    param(
        [string]$InboxId,
        [string]$Email,
        [string]$Status = "active",
        [string]$Source = "mutation",
        [string]$ProviderAliasId = $null
    )

    $cleanEmail = if ($Email) { $Email.Trim().ToLower() } else { "" }
    if (-not $cleanEmail) { return }

    $payload = @{
        inbox_id = $InboxId
        email = $cleanEmail
        source = $Source
        status = $Status
        provider_alias_id = $ProviderAliasId
    }

    $existing = Invoke-SupabaseApi -Method GET -Table "inbox_email_aliases" -Query "email=eq.$cleanEmail&limit=1&select=id"
    if ($existing.Success -and $existing.Data -and $existing.Data.Count -gt 0 -and $existing.Data[0].id) {
        Invoke-SupabaseApi -Method PATCH -Table "inbox_email_aliases" -Query "id=eq.$($existing.Data[0].id)" -Body $payload | Out-Null
        return
    }

    Invoke-SupabaseApi -Method POST -Table "inbox_email_aliases" -Body $payload | Out-Null
}

function Refresh-MutationSubmission {
    param([string]$SubmissionId)

    $result = Invoke-SupabaseApi -Method GET -Table "domain_mutation_requests" -Query "submission_id=eq.$SubmissionId&order=requested_at.asc&select=id,status,current_step,last_error,started_at,completed_at,failed_at"
    if (-not $result.Success -or -not $result.Data -or $result.Data.Count -eq 0) { return $null }

    $rows = @($result.Data)
    $statuses = @($rows | ForEach-Object { [string]($_.status) })

    $aggregateStatus = "queued"
    if (($statuses | Where-Object { $_ -ne "completed" }).Count -eq 0) {
        $aggregateStatus = "completed"
    } elseif ($statuses -contains "processing") {
        $aggregateStatus = "processing"
    } elseif ($statuses -contains "queued") {
        $aggregateStatus = "queued"
    } elseif (($statuses | Where-Object { $_ -ne "cancelled" }).Count -eq 0) {
        $aggregateStatus = "cancelled"
    } elseif (($statuses -contains "failed") -and ($statuses -contains "completed")) {
        $aggregateStatus = "partially_completed"
    } elseif ($statuses -contains "failed") {
        $aggregateStatus = "failed"
    } elseif ($statuses -contains "needs_attention") {
        $aggregateStatus = "processing"
    }

    $startedValues = @($rows | Where-Object { $_.started_at } | ForEach-Object { [string]$_.started_at })
    $completedValues = @($rows | Where-Object { $_.completed_at } | ForEach-Object { [string]$_.completed_at })
    $failedValues = @($rows | Where-Object { $_.failed_at } | ForEach-Object { [string]$_.failed_at })

    $payload = @{
        status = $aggregateStatus
        request_count = $rows.Count
        current_step = switch ($aggregateStatus) {
            "completed" { "completed" }
            "failed" { "failed" }
            "cancelled" { "cancelled" }
            "partially_completed" { "partially_completed" }
            "queued" { "queued" }
            default {
                $activeRequest = $rows | Where-Object { $_.status -in @("processing", "needs_attention", "queued") } | Select-Object -First 1
                if ($activeRequest -and $activeRequest.current_step) { [string]$activeRequest.current_step } else { $aggregateStatus }
            }
        }
        updated_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    }

    if ($startedValues.Count -gt 0) { $payload.started_at = ($startedValues | Sort-Object | Select-Object -First 1) }
    if ($aggregateStatus -eq "completed" -and $completedValues.Count -gt 0) {
        $payload.completed_at = ($completedValues | Sort-Object | Select-Object -Last 1)
        $payload.failed_at = $null
        $payload.last_error = $null
    } elseif (($aggregateStatus -eq "failed" -or $aggregateStatus -eq "partially_completed") -and $failedValues.Count -gt 0) {
        $payload.failed_at = ($failedValues | Sort-Object | Select-Object -Last 1)
        $latestError = $rows | Where-Object { $_.last_error } | Select-Object -First 1
        $payload.last_error = if ($latestError) { [string]$latestError.last_error } else { $null }
    } else {
        $payload.last_error = $null
    }

    $updateResult = Invoke-SupabaseApi -Method PATCH -Table "inbox_mutation_submissions" -Query "id=eq.$SubmissionId" -Body $payload
    if ($updateResult.Success -and $updateResult.Data -and $updateResult.Data.Count -gt 0) { return $updateResult.Data[0] }
    return $null
}

# History helper (builds string for domain.action_history)
function Add-HistoryEntry {
    param([string]$History, [string]$Entry)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $newLine = "[$timestamp] $Entry"
    if ($History) { return "$History`n$newLine" } else { return $newLine }
}
