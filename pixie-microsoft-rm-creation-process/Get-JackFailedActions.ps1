param(
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
    [switch]$IncludeHistorical,
    [switch]$Json
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "config.ps1")

function Normalize-DomainName {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

function Invoke-FailedActionsApiRows {
    param([string]$Table, [string]$Query, [string]$Label)
    $attempt = 0
    while ($true) {
        $attempt += 1
        $result = Invoke-SupabaseApi -Method GET -Table $Table -Query $Query
        if ($result.Success) { return @($result.Data) }
        if ($attempt -ge 3) {
            $errorText = if ($result.Error) { [string]$result.Error } else { "unknown Supabase API error" }
            throw "$Label failed after $attempt attempt(s): $errorText"
        }
        Start-Sleep -Seconds (2 * $attempt)
    }
}

$plan = @(Import-Csv -Path $PlanCsv)
$names = @($plan | ForEach-Object { Normalize-DomainName ([string]$_.domain) } | Where-Object { $_ } | Sort-Object -Unique)
$domains = @(Invoke-FailedActionsApiRows -Table "domains" -Query "select=id,domain,status,interim_status&limit=5000" -Label "domains" | Where-Object { $names -contains (Normalize-DomainName ([string]$_.domain)) })
$domainById = @{}
foreach ($domain in $domains) { $domainById[[string]$domain.id] = $domain }
$ids = @($domains | ForEach-Object { [string]$_.id })

$actions = @()
for ($i = 0; $i -lt $ids.Count; $i += 8) {
    $last = [Math]::Min($i + 7, $ids.Count - 1)
    $chunk = @($ids[$i..$last])
    $query = "domain_id=in.($($chunk -join ','))&select=id,domain_id,type,status,error,attempts,payload,result,updated_at,next_retry_at&limit=20000"
    $actions += @(Invoke-FailedActionsApiRows -Table "actions" -Query $query -Label "actions chunk")
}

$replacementProvisionIds = @{}
foreach ($action in @($actions)) {
    if (
        [string]$action.type -eq "provision_inbox" -and
        $action.payload -and
        [string]$action.payload.source -eq "jack_threshold_tenant_migration"
    ) {
        $replacementProvisionIds[[string]$action.id] = $true
    }
}

$completedReplacementUploadDomainIds = @{}
foreach ($action in @($actions)) {
    if (
        [string]$action.type -eq "reupload_inboxes" -and
        [string]$action.status -eq "completed" -and
        $action.payload -and
        $replacementProvisionIds.ContainsKey([string]$action.payload.provision_action_id)
    ) {
        $completedReplacementUploadDomainIds[[string]$action.domain_id] = $true
    }
}

$failedActions = @($actions | Where-Object { [string]$_.status -eq "failed" })
if (-not $IncludeHistorical) {
    $failedActions = @($failedActions | Where-Object {
        if ($completedReplacementUploadDomainIds.ContainsKey([string]$_.domain_id)) { return $false }
        if (-not $_.payload) { return $false }
        if ([string]$_.payload.source -eq "jack_threshold_tenant_migration") { return $true }
        if (
            [string]$_.type -eq "reupload_inboxes" -and
            $_.payload.provision_action_id -and
            $replacementProvisionIds.ContainsKey([string]$_.payload.provision_action_id)
        ) { return $true }
        return $false
    })
}

$rows = @($failedActions | ForEach-Object {
    $domain = $domainById[[string]$_.domain_id]
    $errorText = [string]$_.error
    $source = if ($_.payload -and $_.payload.source) { [string]$_.payload.source } else { "" }
    [pscustomobject]@{
        domain = [string]$domain.domain
        domain_status = [string]$domain.status
        interim_status = [string]$domain.interim_status
        type = [string]$_.type
        source = $source
        id = [string]$_.id
        attempts = [int]$_.attempts
        error = if ($errorText.Length -gt 700) { $errorText.Substring(0, 700) } else { $errorText }
        updated_at = [string]$_.updated_at
        next_retry_at = [string]$_.next_retry_at
    }
})

if ($Json) {
    $rows | ConvertTo-Json -Depth 6
} else {
    $rows | Format-Table -AutoSize
}
