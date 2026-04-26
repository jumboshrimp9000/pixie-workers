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

if (-not $global:ActionLeaseFences) {
    $global:ActionLeaseFences = @{}
}

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

function Get-WorkerActionLeaseSeconds {
    $leaseSeconds = 600
    if ($env:WORKER_ACTION_LEASE_SECONDS) {
        try { $leaseSeconds = [Math]::Max(30, [int][double]$env:WORKER_ACTION_LEASE_SECONDS) } catch { $leaseSeconds = 600 }
    }
    return $leaseSeconds
}

function Get-WorkerActionHeartbeatSeconds {
    $configured = 120
    if ($env:WORKER_ACTION_HEARTBEAT_SECONDS) {
        try { $configured = [Math]::Max(5, [int][double]$env:WORKER_ACTION_HEARTBEAT_SECONDS) } catch { $configured = 120 }
    }
    return [Math]::Max(5, [Math]::Min([int]([double](Get-WorkerActionLeaseSeconds) / 3), $configured))
}

function Register-ActionLeaseFence {
    param([object]$Action)

    if (-not $Action -or -not $Action.id) { return }
    $actionId = [string]$Action.id
    $existing = if ($global:ActionLeaseFences.ContainsKey($actionId)) { $global:ActionLeaseFences[$actionId] } else { $null }
    $global:ActionLeaseFences[$actionId] = [pscustomobject]@{
        ActionId      = $actionId
        Attempts      = if ($Action.attempts -ne $null) { [int]$Action.attempts } else { 0 }
        StartedAt     = if ($Action.started_at) { [string]$Action.started_at } else { $null }
        Timer         = if ($existing) { $existing.Timer } else { $null }
        Subscription  = if ($existing) { $existing.Subscription } else { $null }
        SourceId      = if ($existing) { $existing.SourceId } else { "action-lease-$actionId" }
    }
}

function Get-ActionLeaseFence {
    param(
        [string]$ActionId,
        [object]$Action = $null
    )

    if ($ActionId -and $global:ActionLeaseFences.ContainsKey($ActionId)) {
        return $global:ActionLeaseFences[$ActionId]
    }

    if ($Action -and $Action.id) {
        $actionKey = [string]$Action.id
        if ($global:ActionLeaseFences.ContainsKey($actionKey)) {
            return $global:ActionLeaseFences[$actionKey]
        }
        return [pscustomobject]@{
            ActionId  = $actionKey
            Attempts  = if ($Action.attempts -ne $null) { [int]$Action.attempts } else { 0 }
            StartedAt = if ($Action.started_at) { [string]$Action.started_at } else { $null }
        }
    }

    return $null
}

function Get-ActionFenceQuery {
    param(
        [string]$ActionId,
        [object]$Action = $null,
        [switch]$RequireFence
    )

    $query = "id=eq.$ActionId"
    $fence = Get-ActionLeaseFence -ActionId $ActionId -Action $Action
    if (-not $RequireFence) { return $query }
    if (-not $fence) {
        Write-Log "Missing lease fence for action $ActionId; refusing fenced action update" -Level Warning
        return "$query&status=eq.__missing_lease_fence__"
    }

    $query += "&status=eq.in_progress&attempts=eq.$($fence.Attempts)"
    if ($fence.StartedAt) {
        $query += "&started_at=eq.$([uri]::EscapeDataString([string]$fence.StartedAt))"
    } else {
        $query += "&started_at=is.null"
    }
    return $query
}

function Start-ActionLeaseHeartbeat {
    param([object]$Action)

    if (-not $Action -or -not $Action.id) { return $null }
    Register-ActionLeaseFence -Action $Action
    $actionId = [string]$Action.id
    $fence = $global:ActionLeaseFences[$actionId]
    if ($fence.Timer) { return $fence }

    $timer = [System.Timers.Timer]::new((Get-WorkerActionHeartbeatSeconds) * 1000)
    $timer.AutoReset = $true
    $sourceId = "action-lease-$actionId"
    $messageData = @{
        ActionId = $actionId
        BaseUrl = $SupabaseConfig.Url
        ServiceRoleKey = $SupabaseConfig.ServiceRoleKey
    }
    $subscription = Register-ObjectEvent -InputObject $timer -EventName Elapsed -SourceIdentifier $sourceId -MessageData $messageData -Action {
        $data = $Event.MessageData
        $actionId = [string]$data.ActionId
        if (-not $global:ActionLeaseFences -or -not $global:ActionLeaseFences.ContainsKey($actionId)) { return }

        $fence = $global:ActionLeaseFences[$actionId]
        $nextStartedAt = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        $startedFilter = if ($fence.StartedAt) { "started_at=eq.$([uri]::EscapeDataString([string]$fence.StartedAt))" } else { "started_at=is.null" }
        $query = "id=eq.$actionId&status=eq.in_progress&attempts=eq.$($fence.Attempts)&$startedFilter"
        $headers = @{
            "apikey"        = $data.ServiceRoleKey
            "Authorization" = "Bearer $($data.ServiceRoleKey)"
            "Content-Type"  = "application/json"
            "Prefer"        = "return=representation"
        }
        $body = @{ started_at = $nextStartedAt } | ConvertTo-Json -Depth 4 -Compress
        try {
            $rows = Invoke-RestMethod -Method PATCH -Uri "$($data.BaseUrl)/rest/v1/actions?$query" -Headers $headers -Body $body -TimeoutSec 60 -ErrorAction Stop
            $rowList = @($rows)
            if ($rowList.Count -gt 0) {
                $fence.StartedAt = if ($rowList[0].started_at) { [string]$rowList[0].started_at } else { $nextStartedAt }
            } else {
                Write-Host "[WARN] Action lease heartbeat lost fence for $actionId" -ForegroundColor Yellow
                $Event.Sender.Stop()
            }
        } catch {
            Write-Host "[WARN] Action lease heartbeat failed for $actionId`: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }

    $fence.Timer = $timer
    $fence.Subscription = $subscription
    $fence.SourceId = $sourceId
    $timer.Start()
    return $fence
}

function Stop-ActionLeaseHeartbeat {
    param([object]$Action)

    if (-not $Action -or -not $Action.id) { return }
    $actionId = [string]$Action.id
    if (-not $global:ActionLeaseFences.ContainsKey($actionId)) { return }

    $fence = $global:ActionLeaseFences[$actionId]
    if ($fence.Timer) {
        $fence.Timer.Stop()
        $fence.Timer.Dispose()
    }
    if ($fence.SourceId) {
        try { Unregister-Event -SourceIdentifier $fence.SourceId -ErrorAction SilentlyContinue } catch { }
    }
    if ($fence.Subscription) {
        try { Remove-Job -Id $fence.Subscription.Id -Force -ErrorAction SilentlyContinue } catch { }
    }
    $global:ActionLeaseFences.Remove($actionId) | Out-Null
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
    $pendingQuery = "type=in.($encodedTypes)&status=eq.pending&order=created_at.asc&limit=$Limit"
    $pendingResult = Invoke-SupabaseApi -Method GET -Table "actions" -Query $pendingQuery
    if (-not $pendingResult.Success) { return @() }

    $leaseSeconds = Get-WorkerActionLeaseSeconds
    $reclaimBefore = (Get-Date).ToUniversalTime().AddSeconds(-$leaseSeconds).ToString("yyyy-MM-ddTHH:mm:ssZ")
    $staleQuery = "type=in.($encodedTypes)&status=eq.in_progress&or=(started_at.is.null,started_at.lte.$reclaimBefore)&order=created_at.asc&limit=$Limit"
    $staleResult = Invoke-SupabaseApi -Method GET -Table "actions" -Query $staleQuery
    if (-not $staleResult.Success) { return @() }

    $now = Get-Date
    return @(@($pendingResult.Data) + @($staleResult.Data) | Sort-Object created_at | Where-Object {
        $attempts = if ($_.attempts -ne $null) { [int]$_.attempts } else { 0 }
        $maxAttempts = if ($_.max_attempts -ne $null) { [int]$_.max_attempts } else { 3 }
        if ($attempts -ge $maxAttempts) { return $false }

        if ($_.status -eq "pending") {
            $nextRetryAt = $null
            if ($_.next_retry_at) {
                try { $nextRetryAt = [DateTime]::Parse($_.next_retry_at).ToUniversalTime() } catch { $nextRetryAt = $null }
            }

            if ($nextRetryAt -and $nextRetryAt -gt $now.ToUniversalTime()) {
                return $false
            }
        }

        return $true
    } | Select-Object -First $Limit)
}

function Claim-Action {
    param([object]$Action)

    if (-not $Action -or -not $Action.id) { return $null }
    $attempts = if ($Action.attempts -ne $null) { [int]$Action.attempts } else { 0 }
    $status = if ($Action.status) { [string]$Action.status } else { "pending" }
    $body = @{
        status = "in_progress"
        attempts = $attempts + 1
        started_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        error = $null
        next_retry_at = $null
    }

    $query = "id=eq.$($Action.id)&attempts=eq.$attempts&status=eq.$status"
    if ($status -eq "in_progress") {
        $leaseSeconds = Get-WorkerActionLeaseSeconds
        $reclaimBefore = (Get-Date).ToUniversalTime().AddSeconds(-$leaseSeconds).ToString("yyyy-MM-ddTHH:mm:ssZ")
        if ($Action.started_at) {
            $query += "&started_at=lte.$reclaimBefore"
        } else {
            $query += "&started_at=is.null"
        }
    }

    $result = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query $query -Body $body
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) {
        Register-ActionLeaseFence -Action $result.Data[0]
        return $result.Data[0]
    }
    return $null
}

function Update-ActionStatus {
    param(
        [string]$ActionId,
        [string]$Status,
        [string]$Error = $null,
        [object]$Result = $null,
        [string]$NextRetryAt = $null,
        [object]$Action = $null
    )

    $now = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $body = @{ status = $Status; updated_at = $now }
    if ($Status -eq "in_progress") { $body.started_at = $now }
    if ($Status -eq "completed") { $body.completed_at = $now }
    if ($Error) { $body.error = $Error }
    if ($Result) { $body.result = $Result }
    if ($PSBoundParameters.ContainsKey("NextRetryAt")) { $body.next_retry_at = $NextRetryAt }

    $query = Get-ActionFenceQuery -ActionId $ActionId -Action $Action -RequireFence
    $result = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query $query -Body $body
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) {
        if ($Status -eq "in_progress") { Register-ActionLeaseFence -Action $result.Data[0] }
        return $true
    } elseif ($result.Success) {
        Write-Log "Skipped action $ActionId status update to $Status because the lease fence no longer matched" -Level Warning
    }
    return $false
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
    $nextRetryAt = if ($isFinal) { $null } else { (Get-Date).ToUniversalTime().AddSeconds($delaySeconds).ToString("yyyy-MM-ddTHH:mm:ssZ") }

    $body = @{
        status = if ($isFinal) { "failed" } else { "pending" }
        error = if ($ErrorMessage) { $ErrorMessage.Substring(0, [Math]::Min(4000, $ErrorMessage.Length)) } else { "Unknown error" }
        next_retry_at = $nextRetryAt
        started_at = if ($isFinal) { $Action.started_at } else { $null }
        updated_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    }

    $result = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query (Get-ActionFenceQuery -ActionId ([string]$Action.id) -Action $Action -RequireFence) -Body $body
    if ($result.Success -and (-not $result.Data -or $result.Data.Count -eq 0)) {
        Write-Log "Skipped failure update for action $($Action.id) because the lease fence no longer matched" -Level Warning
    }
    return ($result.Success -and $result.Data -and $result.Data.Count -gt 0)
}

function Update-ActionResult {
    param(
        [string]$ActionId,
        [object]$Result
    )
    $body = @{ result = $Result; updated_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ") }
    $result = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query (Get-ActionFenceQuery -ActionId $ActionId -RequireFence) -Body $body
    if ($result.Success -and (-not $result.Data -or $result.Data.Count -eq 0)) {
        Write-Log "Skipped result update for action $ActionId because the lease fence no longer matched" -Level Warning
    }
    return ($result.Success -and $result.Data -and $result.Data.Count -gt 0)
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
        [int]$LeaseSeconds = 21600
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
        [int]$LeaseSeconds = 21600
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
    $nextRetryAt = (Get-Date).ToUniversalTime().AddSeconds($DelaySeconds).ToString("yyyy-MM-ddTHH:mm:ssZ")
    $body = @{
        status = "pending"
        error = if ($Reason) { $Reason.Substring(0, [Math]::Min(4000, $Reason.Length)) } else { "Pending retry" }
        next_retry_at = $nextRetryAt
        attempts = $restoredAttempts
        started_at = $null
        completed_at = $null
        updated_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    }

    $result = Invoke-SupabaseApi -Method PATCH -Table "actions" -Query (Get-ActionFenceQuery -ActionId ([string]$Action.id) -Action $Action -RequireFence) -Body $body
    if ($result.Success -and (-not $result.Data -or $result.Data.Count -eq 0)) {
        Write-Log "Skipped requeue for action $($Action.id) because the lease fence no longer matched" -Level Warning
    }
    return ($result.Success -and $result.Data -and $result.Data.Count -gt 0)
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
