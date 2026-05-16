<#
.SYNOPSIS
    Builds the final Simple Inboxes admin-pool move plan from precheck and threshold proof CSVs.
.DESCRIPTION
    This is the gatekeeper report between "candidate admins were checked" and
    "candidate admins are allowed to be inserted into the SI database".

    Expected flow:
      1. Run the read-only precheck: login, license, no custom domains, DB threshold flag.
      2. Run the active threshold proof with Test-JackDestinationTenantActiveThreshold.ps1.
      3. Run this script to produce move_stage values:
         - blocked
         - precleared
         - threshold_cleared
         - ready_to_insert

    Only ready_to_insert rows should be imported into admin_credentials.
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$PrecheckCsv,
    [string]$ThresholdCsv = "",
    [string]$OutputCsv = "",
    [string]$OutputSummaryJson = "",
    [string]$LogDir = (Join-Path $PSScriptRoot "logs")
)

$ErrorActionPreference = "Stop"

function Normalize-AdminEmail {
    param([object]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

function ConvertTo-Bool {
    param([object]$Value)
    $text = ([string]$Value).Trim().ToLowerInvariant()
    return ($text -in @("true", "1", "yes", "y"))
}

function ConvertTo-Int {
    param([object]$Value)
    $text = ([string]$Value).Trim()
    $number = 0
    if ([int]::TryParse($text, [ref]$number)) { return $number }
    return 0
}

function Get-PrecheckFailures {
    param([object]$Row)

    $failures = New-Object System.Collections.Generic.List[string]
    if (-not (ConvertTo-Bool $Row.login_ok)) { $failures.Add("login_failed") | Out-Null }
    if (-not (ConvertTo-Bool $Row.licensed)) { $failures.Add("no_license") | Out-Null }
    if ((ConvertTo-Int $Row.custom_domain_count) -gt 0) { $failures.Add("has_custom_domains") | Out-Null }
    if (-not (ConvertTo-Bool $Row.no_threshold_error)) { $failures.Add("db_threshold_flag") | Out-Null }
    if (ConvertTo-Bool $Row.already_in_si_db) { $failures.Add("already_in_si_db") | Out-Null }
    return @($failures.ToArray())
}

if (-not (Test-Path $PrecheckCsv)) { throw "Precheck CSV not found: $PrecheckCsv" }
if ($ThresholdCsv -and -not (Test-Path $ThresholdCsv)) { throw "Threshold CSV not found: $ThresholdCsv" }
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Force -Path $LogDir | Out-Null }

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
if (-not $OutputCsv) { $OutputCsv = Join-Path $LogDir "si-admin-candidate-move-plan-$stamp.csv" }
if (-not $OutputSummaryJson) { $OutputSummaryJson = [System.IO.Path]::ChangeExtension($OutputCsv, ".summary.json") }

$precheckRows = @(Import-Csv -Path $PrecheckCsv)
$thresholdByEmail = @{}

if ($ThresholdCsv) {
    foreach ($row in @(Import-Csv -Path $ThresholdCsv)) {
        $email = Normalize-AdminEmail $row.admin_email
        if (-not $email) { $email = Normalize-AdminEmail $row.email }
        if ($email) { $thresholdByEmail[$email] = $row }
    }
}

$planRows = New-Object System.Collections.Generic.List[object]
foreach ($row in $precheckRows) {
    $email = Normalize-AdminEmail $row.email
    if (-not $email) { continue }

    $preFailures = @(Get-PrecheckFailures -Row $row)
    $precheckStatus = if ($preFailures.Count -eq 0) { "precleared" } else { "blocked" }
    $thresholdStatus = if ($ThresholdCsv) { "missing_threshold_proof" } else { "not_run" }
    $thresholdMethod = ""
    $thresholdEvidence = ""
    $thresholdError = ""
    $moveStage = $precheckStatus

    if ($preFailures.Count -eq 0 -and $ThresholdCsv) {
        $threshold = $thresholdByEmail[$email]
        if (-not $threshold) {
            $thresholdStatus = "missing_threshold_proof"
            $moveStage = "precleared"
        } elseif (([string]$threshold.result).Trim().ToLowerInvariant() -eq "clean") {
            $thresholdStatus = "threshold_cleared"
            $moveStage = "ready_to_insert"
            $thresholdMethod = [string]$threshold.method
        } elseif (([string]$threshold.result).Trim().ToLowerInvariant() -eq "threshold") {
            $thresholdStatus = "threshold_blocked"
            $moveStage = "blocked"
            $thresholdMethod = [string]$threshold.method
            $thresholdEvidence = [string]$threshold.evidence
        } else {
            $thresholdStatus = "threshold_unknown"
            $moveStage = "precleared"
            $thresholdMethod = [string]$threshold.method
            $thresholdError = [string]$threshold.error
        }
    }

    $planRows.Add([pscustomobject]@{
        email = $email
        precheck_status = $precheckStatus
        precheck_failures = ($preFailures -join ";")
        threshold_status = $thresholdStatus
        threshold_method = $thresholdMethod
        move_stage = $moveStage
        eligible_for_insert = [string]($moveStage -eq "ready_to_insert")
        login_ok = [string](ConvertTo-Bool $row.login_ok)
        licensed = [string](ConvertTo-Bool $row.licensed)
        custom_domain_count = [string](ConvertTo-Int $row.custom_domain_count)
        custom_domains = [string]$row.custom_domains
        no_threshold_error = [string](ConvertTo-Bool $row.no_threshold_error)
        already_in_si_db = [string](ConvertTo-Bool $row.already_in_si_db)
        threshold_evidence = $thresholdEvidence
        threshold_error = $thresholdError
    }) | Out-Null
}

$plan = @($planRows.ToArray() | Sort-Object email)
$plan | Export-Csv -Path $OutputCsv -NoTypeInformation

$summary = [ordered]@{
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    precheck_csv = (Resolve-Path $PrecheckCsv).Path
    threshold_csv = if ($ThresholdCsv) { (Resolve-Path $ThresholdCsv).Path } else { "" }
    output_csv = $OutputCsv
    candidate_count = $plan.Count
    blocked_count = @($plan | Where-Object { $_.move_stage -eq "blocked" }).Count
    precleared_count = @($plan | Where-Object { $_.move_stage -eq "precleared" }).Count
    threshold_cleared_count = @($plan | Where-Object { $_.threshold_status -eq "threshold_cleared" }).Count
    ready_to_insert_count = @($plan | Where-Object { $_.move_stage -eq "ready_to_insert" }).Count
    threshold_unknown_count = @($plan | Where-Object { $_.threshold_status -eq "threshold_unknown" }).Count
    missing_threshold_proof_count = @($plan | Where-Object { $_.threshold_status -eq "missing_threshold_proof" }).Count
}

($summary | ConvertTo-Json -Depth 6) | Set-Content -Path $OutputSummaryJson -Encoding UTF8
$summary | ConvertTo-Json -Depth 6
