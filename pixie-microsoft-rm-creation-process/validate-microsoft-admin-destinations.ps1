<#
.SYNOPSIS
    Validates Microsoft admin tenants before using them as migration destinations.
.DESCRIPTION
    One-off guardrail for moving domains away from thresholded tenants. It reads
    candidate admin emails, loads credentials from SimpleInboxes, connects to
    Exchange Online, checks tenant message trace for threshold evidence, and
    exports a non-secret validation report. It does not modify tenants or DB rows.
#>

param(
    [string]$InputCsv = "",
    [string]$AdminEmail = "",
    [int]$Limit = 10,
    [int]$Skip = 0,
    [int]$HoursBack = 96,
    [int]$MaxTracePages = 10,
    [int]$MaxFailedDetailsToInspect = 50,
    [switch]$UseMessageTraceV2,
    [string]$OutputCsv = "",
    [string]$LogDir = (Join-Path $PSScriptRoot "logs")
)

$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "config.ps1")

$ThresholdPattern = "5\.7\.705|5\.7\.708|tenant has exceeded threshold|exceeded threshold|Access denied.*threshold"

function Normalize-AdminEmail {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

function Get-EmailDomain {
    param([string]$Email)
    $value = ([string]$Email).Trim().ToLowerInvariant()
    $at = $value.LastIndexOf("@")
    if ($at -lt 0 -or $at -eq ($value.Length - 1)) { return "" }
    return $value.Substring($at + 1)
}

function Get-TraceReceived {
    param([object]$Trace)
    if (-not $Trace) { return $null }
    if ($null -ne $Trace.Received -and ([string]$Trace.Received).Trim()) { return [datetime]$Trace.Received }
    if ($null -ne $Trace.ReceivedTime -and ([string]$Trace.ReceivedTime).Trim()) { return [datetime]$Trace.ReceivedTime }
    return $null
}

function ConvertTo-TraceText {
    param([object]$Object)
    if (-not $Object) { return "" }
    $values = New-Object System.Collections.Generic.List[string]
    foreach ($property in $Object.PSObject.Properties) {
        if ($null -ne $property.Value) { $values.Add(([string]$property.Value)) | Out-Null }
    }
    return ($values -join "`n")
}

function ConvertTo-EvidenceSnippet {
    param([string]$Text)
    $value = (([string]$Text) -replace "\s+", " ").Trim()
    if ($value.Length -gt 700) { return $value.Substring(0, 700) }
    return $value
}

function Read-CandidateEmails {
    $emails = New-Object System.Collections.Generic.List[string]
    if ($AdminEmail) {
        $emails.Add((Normalize-AdminEmail $AdminEmail)) | Out-Null
        return @($emails.ToArray())
    }

    if ($InputCsv) {
        if (-not (Test-Path $InputCsv)) { throw "InputCsv not found: $InputCsv" }
        $rows = @(Import-Csv -Path $InputCsv)
        foreach ($row in $rows) {
            $value = ""
            foreach ($name in @("admin", "email", "AdminEmail", "proposed_destination_admin")) {
                $prop = $row.PSObject.Properties | Where-Object { $_.Name -eq $name } | Select-Object -First 1
                if ($prop -and $prop.Value) { $value = [string]$prop.Value; break }
            }
            $email = Normalize-AdminEmail $value
            if ($email) { $emails.Add($email) | Out-Null }
        }
    } else {
        $query = "provider=eq.microsoft&status=eq.Active&active=eq.true&locked_by_action_id=is.null&order=usage_count.asc,email.asc&select=id,email&limit=1000"
        $result = Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query $query
        if (-not $result.Success) { throw "Failed to load candidate admins: $($result.Error)" }
        foreach ($row in @($result.Data)) {
            $email = Normalize-AdminEmail ([string]$row.email)
            if ($email) { $emails.Add($email) | Out-Null }
        }
    }

    $unique = @($emails.ToArray() | Select-Object -Unique)
    if ($Skip -gt 0) { $unique = @($unique | Select-Object -Skip $Skip) }
    if ($Limit -gt 0) { $unique = @($unique | Select-Object -First $Limit) }
    return $unique
}

function Get-AdminByEmail {
    param([string]$Email)
    $encoded = [uri]::EscapeDataString($Email)
    $result = Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query "provider=eq.microsoft&email=eq.$encoded&select=*&limit=1"
    if ($result.Success -and $result.Data -and $result.Data.Count -gt 0) { return $result.Data[0] }
    return $null
}

function Get-AssignedDomainCount {
    param([string]$AdminId)
    if (-not $AdminId) { return 0 }
    $result = Invoke-SupabaseApi -Method GET -Table "domain_admin_assignments" -Query "admin_cred_id=eq.$AdminId&select=domain_id&limit=5000"
    if ($result.Success) { return @($result.Data).Count }
    return 0
}

function Get-PagedMessageTraces {
    param([datetime]$StartDate, [datetime]$EndDate)

    $script:LastTracePagesFetched = 0
    $script:LastTraceComplete = $true
    $script:LastTraceOldestAtUtc = ""
    $seen = @{}
    $traces = New-Object System.Collections.ArrayList

    if ($UseMessageTraceV2 -and (Get-Command Get-MessageTraceV2 -ErrorAction SilentlyContinue)) {
        $pageEndDate = $EndDate
        for ($page = 1; $page -le [Math]::Max(1, $MaxTracePages); $page += 1) {
            if ($pageEndDate -le $StartDate) { break }
            $pageRows = @(Get-MessageTraceV2 -StartDate $StartDate -EndDate $pageEndDate -ResultSize 1000 -ErrorAction Stop)
            $script:LastTracePagesFetched = $page
            if ($pageRows.Count -eq 0) { break }
            foreach ($row in $pageRows) {
                $received = Get-TraceReceived $row
                $key = "$($row.MessageTraceId)|$($row.RecipientAddress)|$($row.SenderAddress)|$received"
                if (-not $seen.ContainsKey($key)) {
                    $seen[$key] = $true
                    $traces.Add($row) | Out-Null
                }
            }
            $oldest = @($pageRows | Sort-Object @{ Expression = { Get-TraceReceived $_ }; Ascending = $true } | Select-Object -First 1)
            if ($oldest.Count -eq 0) { break }
            $oldestAt = Get-TraceReceived $oldest[0]
            if (-not $oldestAt) { break }
            $script:LastTraceOldestAtUtc = $oldestAt.ToUniversalTime().ToString("o")
            if ($pageRows.Count -lt 1000 -or $oldestAt -le $StartDate) { break }
            $pageEndDate = $oldestAt.AddMilliseconds(-1)
            if ($page -eq [Math]::Max(1, $MaxTracePages)) { $script:LastTraceComplete = $false }
        }
        return @($traces.ToArray())
    }

    for ($page = 1; $page -le [Math]::Max(1, $MaxTracePages); $page += 1) {
        $pageRows = @(Get-MessageTrace -StartDate $StartDate -EndDate $EndDate -PageSize 1000 -Page $page -WarningAction SilentlyContinue -ErrorAction Stop)
        $script:LastTracePagesFetched = $page
        if ($pageRows.Count -eq 0) { break }
        foreach ($row in $pageRows) {
            $received = Get-TraceReceived $row
            $key = "$($row.MessageTraceId)|$($row.RecipientAddress)|$($row.SenderAddress)|$received"
            if (-not $seen.ContainsKey($key)) {
                $seen[$key] = $true
                $traces.Add($row) | Out-Null
            }
        }
        if ($pageRows.Count -lt 1000) { break }
        if ($page -eq [Math]::Max(1, $MaxTracePages)) { $script:LastTraceComplete = $false }
    }
    return @($traces.ToArray())
}

function Get-TraceDetails {
    param([object]$Trace, [datetime]$StartDate, [datetime]$EndDate)
    if (-not $Trace.MessageTraceId -or -not $Trace.RecipientAddress) { return @() }
    try {
        if (Get-Command Get-MessageTraceDetailV2 -ErrorAction SilentlyContinue) {
            return @(Get-MessageTraceDetailV2 -MessageTraceId $Trace.MessageTraceId -RecipientAddress $Trace.RecipientAddress -StartDate $StartDate -EndDate $EndDate -ErrorAction Stop)
        }
        return @(Get-MessageTraceDetail -MessageTraceId $Trace.MessageTraceId -RecipientAddress $Trace.RecipientAddress -ErrorAction Stop)
    } catch {
        return @()
    }
}

function Test-TraceHasThreshold {
    param([object]$Trace, [datetime]$StartDate, [datetime]$EndDate)
    $traceText = ConvertTo-TraceText $Trace
    $detailText = ""
    if ($traceText -notmatch $ThresholdPattern) {
        $details = @(Get-TraceDetails -Trace $Trace -StartDate $StartDate -EndDate $EndDate)
        $detailText = (($details | ForEach-Object { ConvertTo-TraceText $_ }) -join "`n")
    }
    $combined = "$traceText`n$detailText"
    return [pscustomobject]@{ HasThreshold = ($combined -match $ThresholdPattern); Evidence = ConvertTo-EvidenceSnippet $combined }
}

function Test-NormalExternalOutbound {
    param([object]$Trace, [hashtable]$AcceptedDomains)
    $sender = ([string]$Trace.SenderAddress).Trim().ToLowerInvariant()
    if (-not $sender -or $sender -notmatch "@") { return $false }
    if ($sender.StartsWith("microsoftexchange") -or $sender.StartsWith("postmaster@")) { return $false }
    $recipientDomain = Get-EmailDomain ([string]$Trace.RecipientAddress)
    if (-not $recipientDomain) { return $false }
    if ($AcceptedDomains.ContainsKey($recipientDomain)) { return $false }
    return $true
}

function Validate-Admin {
    param([object]$Admin, [datetime]$StartDate, [datetime]$EndDate)

    $assignedDomainCount = Get-AssignedDomainCount -AdminId ([string]$Admin.id)
    $locked = [bool]([string]$Admin.locked_by_action_id)

    $securePwd = ConvertTo-SecureString ([string]$Admin.password) -AsPlainText -Force
    $creds = New-Object System.Management.Automation.PSCredential(([string]$Admin.email), $securePwd)

    Connect-ExchangeOnline -Credential $creds -ShowBanner:$false -ErrorAction Stop
    try {
        $accepted = @{}
        foreach ($domain in @(Get-AcceptedDomain -ErrorAction Stop)) {
            $name = ([string]$domain.DomainName).Trim().ToLowerInvariant()
            if ($name) { $accepted[$name] = $true }
        }
        $traces = @(Get-PagedMessageTraces -StartDate $StartDate -EndDate $EndDate)
        $outbound = @($traces | Where-Object { Test-NormalExternalOutbound -Trace $_ -AcceptedDomains $accepted } | Sort-Object @{ Expression = { Get-TraceReceived $_ }; Ascending = $false })
        $failed = @($outbound | Where-Object { ([string]$_.Status).Trim() -eq "Failed" })

        $detailsChecked = 0
        $thresholdTrace = $null
        $thresholdCheck = $null
        foreach ($trace in @($failed | Sort-Object @{ Expression = { Get-TraceReceived $_ }; Ascending = $false })) {
            if ($detailsChecked -ge [Math]::Max(1, $MaxFailedDetailsToInspect)) { break }
            $detailsChecked += 1
            $check = Test-TraceHasThreshold -Trace $trace -StartDate $StartDate -EndDate $EndDate
            if ($check.HasThreshold) {
                $thresholdTrace = $trace
                $thresholdCheck = $check
                break
            }
        }

        $validation = if ($thresholdTrace) {
            "threshold_evidence_found"
        } elseif ($outbound.Count -eq 0) {
            "auth_ok_no_recent_external_outbound"
        } elseif ($failed.Count -eq $outbound.Count) {
            "all_recent_external_outbound_failed_no_threshold_evidence"
        } else {
            "auth_ok_no_threshold_evidence"
        }

        return [pscustomobject]@{
            AdminEmail = Normalize-AdminEmail ([string]$Admin.email)
            AdminId = [string]$Admin.id
            DbStatus = [string]$Admin.status
            DbActive = [string]$Admin.active
            AssignedDomainCount = $assignedDomainCount
            Locked = $locked
            AcceptedDomainCount = $accepted.Count
            TraceRowsFetched = $traces.Count
            TracePagesFetched = $script:LastTracePagesFetched
            TraceComplete = $script:LastTraceComplete
            OldestTraceFetchedAtUtc = $script:LastTraceOldestAtUtc
            ExternalOutboundCount = $outbound.Count
            ExternalFailedCount = $failed.Count
            DetailsChecked = $detailsChecked
            Validation = $validation
            ThresholdAtUtc = if ($thresholdTrace) { (Get-TraceReceived $thresholdTrace).ToUniversalTime().ToString("o") } else { "" }
            ThresholdSender = if ($thresholdTrace) { [string]$thresholdTrace.SenderAddress } else { "" }
            ThresholdRecipient = if ($thresholdTrace) { [string]$thresholdTrace.RecipientAddress } else { "" }
            Evidence = if ($thresholdCheck) { [string]$thresholdCheck.Evidence } else { "" }
            Error = ""
        }
    } finally {
        Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
    }
}

if (-not (Get-Module -ListAvailable -Name ExchangeOnlineManagement)) {
    Install-Module -Name ExchangeOnlineManagement -Force -AllowClobber -Scope CurrentUser
}
Import-Module ExchangeOnlineManagement -ErrorAction Stop

if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}

$startDate = (Get-Date).ToUniversalTime().AddHours(-1 * [Math]::Max(1, $HoursBack))
$endDate = (Get-Date).ToUniversalTime()
$emails = @(Read-CandidateEmails)

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
if (-not $OutputCsv) {
    $OutputCsv = Join-Path $LogDir "microsoft-admin-destination-validation-$stamp.csv"
}

Write-Log "Validating $($emails.Count) Microsoft destination admin(s) from $startDate to $endDate" -Level Info

$results = New-Object System.Collections.Generic.List[object]
$index = 0
foreach ($email in $emails) {
    $index += 1
    Write-Log "[$index/$($emails.Count)] Validating destination admin $email" -Level Info
    $admin = Get-AdminByEmail -Email $email
    if (-not $admin) {
        $results.Add([pscustomobject]@{
            AdminEmail = $email; AdminId = ""; DbStatus = ""; DbActive = ""; AssignedDomainCount = ""; Locked = ""; AcceptedDomainCount = "";
            TraceRowsFetched = 0; TracePagesFetched = 0; TraceComplete = $false; OldestTraceFetchedAtUtc = ""; ExternalOutboundCount = 0; ExternalFailedCount = 0; DetailsChecked = 0;
            Validation = "missing_from_db"; ThresholdAtUtc = ""; ThresholdSender = ""; ThresholdRecipient = ""; Evidence = ""; Error = "Admin not found in SI"
        }) | Out-Null
        continue
    }

    try {
        $result = Validate-Admin -Admin $admin -StartDate $startDate -EndDate $endDate
        $results.Add($result) | Out-Null
        $results | Export-Csv -Path $OutputCsv -NoTypeInformation
        $level = if ($result.Validation -eq "threshold_evidence_found") { "Error" } elseif ($result.Validation -eq "all_recent_external_outbound_failed_no_threshold_evidence") { "Warning" } else { "Success" }
        Write-Log "[$index/$($emails.Count)] $email -> $($result.Validation), outbound=$($result.ExternalOutboundCount), failed=$($result.ExternalFailedCount), acceptedDomains=$($result.AcceptedDomainCount)" -Level $level
    } catch {
        $message = "$($_.Exception.Message)"
        if ($_.ScriptStackTrace) { $message = "$message`n$($_.ScriptStackTrace)" }
        $results.Add([pscustomobject]@{
            AdminEmail = $email; AdminId = [string]$admin.id; DbStatus = [string]$admin.status; DbActive = [string]$admin.active; AssignedDomainCount = ""; Locked = "";
            AcceptedDomainCount = ""; TraceRowsFetched = 0; TracePagesFetched = 0; TraceComplete = $false; OldestTraceFetchedAtUtc = ""; ExternalOutboundCount = 0; ExternalFailedCount = 0; DetailsChecked = 0;
            Validation = "error"; ThresholdAtUtc = ""; ThresholdSender = ""; ThresholdRecipient = ""; Evidence = ""; Error = $message
        }) | Out-Null
        $results | Export-Csv -Path $OutputCsv -NoTypeInformation
        Write-Log "[$index/$($emails.Count)] $email validation failed: $message" -Level Error
    }
}

$results | Export-Csv -Path $OutputCsv -NoTypeInformation
$summary = $results | Group-Object Validation | ForEach-Object { "$($_.Name)=$($_.Count)" }
Write-Log "Destination validation complete: $($summary -join ', ') Output=$OutputCsv" -Level Success
