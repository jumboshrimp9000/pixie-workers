<#
.SYNOPSIS
    Finds the earliest observed Jack/ProfitPath tenant threshold bounce in message trace.
.DESCRIPTION
    This is a one-off operational checker for the Jack Azure migration incident.
    It loads the batch domain -> Microsoft admin mapping from Simple Inboxes,
    filters message trace to outbound mail from domains assigned to each admin,
    and inspects failed-message trace detail until it finds the first
    provider-side threshold proof, usually:

        550 5.7.705 Service unavailable. Access denied, tenant has exceeded threshold

    The result is an observed onset, not necessarily the true Microsoft-side
    start time if the threshold began before the requested trace window.
#>

param(
    [string]$BatchId = "ffa54dd8-3fd1-4336-8e06-6f73872432f4",
    [int]$HoursBack = 24,
    [int]$SkipTenants = 0,
    [int]$LimitTenants = 0,
    [int]$MaxTracePages = 25,
    [int]$MaxFailedDetailsToInspect = 100,
    [string]$AdminEmail = "",
    [string]$InputCsv = "",
    [string]$InputCsvStatus = "Yes,Likely",
    [string]$StartDateUtc = "",
    [string]$EndDateUtc = "",
    [switch]$UseMessageTraceV2,
    [string]$OutputCsv = "",
    [string]$LogDir = (Join-Path $PSScriptRoot "logs")
)

. (Join-Path $PSScriptRoot "config.ps1")

$ThresholdPattern = "5\.7\.705|5\.7\.708|tenant has exceeded threshold|exceeded threshold|Access denied.*threshold"

function Normalize-Domain {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant().Replace("https://", "").Replace("http://", "").TrimEnd("/")
}

function Get-EmailDomain {
    param([string]$Email)
    $value = ([string]$Email).Trim().ToLowerInvariant()
    $at = $value.LastIndexOf("@")
    if ($at -lt 0 -or $at -eq ($value.Length - 1)) { return "" }
    return $value.Substring($at + 1)
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

function Get-TraceReceived {
    param([object]$Trace)
    if (-not $Trace) { return $null }
    if ($null -ne $Trace.Received -and ([string]$Trace.Received).Trim()) {
        return [datetime]$Trace.Received
    }
    if ($null -ne $Trace.ReceivedTime -and ([string]$Trace.ReceivedTime).Trim()) {
        return [datetime]$Trace.ReceivedTime
    }
    return $null
}

function ConvertTo-EvidenceSnippet {
    param([string]$Text)
    $value = (([string]$Text) -replace "\s+", " ").Trim()
    if ($value.Length -gt 900) { return $value.Substring(0, 900) }
    return $value
}

function Get-BatchTenantTargets {
    param([string]$BatchId)

    $domainsResult = Invoke-SupabaseApi -Method GET -Table "domains" -Query "order_batch_id=eq.$BatchId&select=id,domain,customer_id,provider,status,interim_status&limit=1000"
    if (-not $domainsResult.Success) { throw "Failed to load batch domains: $($domainsResult.Error)" }

    $domains = @($domainsResult.Data)
    if ($domains.Count -eq 0) { throw "No domains found for batch $BatchId" }

    $ids = @($domains | ForEach-Object { [string]$_.id } | Where-Object { $_ })
    $assignments = @()
    $chunkSize = 75
    for ($offset = 0; $offset -lt $ids.Count; $offset += $chunkSize) {
        $chunk = @($ids | Select-Object -Skip $offset -First $chunkSize)
        $query = "domain_id=in.($($chunk -join ','))&select=domain_id,admin_cred_id,admin_credentials(id,email,password)&limit=1000"
        $result = Invoke-SupabaseApi -Method GET -Table "domain_admin_assignments" -Query $query
        if ($result.Success) { $assignments += @($result.Data) }
    }

    $domainById = @{}
    foreach ($domain in $domains) {
        $domainById[[string]$domain.id] = $domain
    }

    $groups = @{}
    foreach ($assignment in $assignments) {
        $domainId = [string]$assignment.domain_id
        if (-not $domainById.ContainsKey($domainId)) { continue }
        $domain = $domainById[$domainId]
        $admin = $assignment.admin_credentials
        $adminEmail = ([string]$admin.email).Trim().ToLowerInvariant()
        $adminPassword = [string]$admin.password
        if (-not $adminEmail -or -not $adminPassword) { continue }

        if (-not $groups.ContainsKey($adminEmail)) {
            $groups[$adminEmail] = [ordered]@{
                AdminEmail = $adminEmail
                AdminPassword = $adminPassword
                AdminCredentialId = [string]$admin.id
                Domains = New-Object System.Collections.Generic.List[string]
                DomainIds = New-Object System.Collections.Generic.List[string]
            }
        }
        $normalizedDomain = Normalize-Domain ([string]$domain.domain)
        if ($normalizedDomain) { $groups[$adminEmail].Domains.Add($normalizedDomain) | Out-Null }
        $groups[$adminEmail].DomainIds.Add($domainId) | Out-Null
    }

    return @($groups.Values | ForEach-Object {
        [pscustomobject]@{
            AdminEmail = $_.AdminEmail
            AdminPassword = $_.AdminPassword
            AdminCredentialId = $_.AdminCredentialId
            Domains = @($_.Domains | Select-Object -Unique)
            DomainIds = @($_.DomainIds | Select-Object -Unique)
        }
    } | Sort-Object AdminEmail)
}

function Get-RecentMessageTraces {
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
            if ($page -eq [Math]::Max(1, $MaxTracePages)) {
                $script:LastTraceComplete = $false
            }
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
        $oldest = @($pageRows | Sort-Object @{ Expression = { Get-TraceReceived $_ }; Ascending = $true } | Select-Object -First 1)
        if ($oldest.Count -gt 0) {
            $oldestAt = Get-TraceReceived $oldest[0]
            if ($oldestAt) { $script:LastTraceOldestAtUtc = $oldestAt.ToUniversalTime().ToString("o") }
        }
        if ($pageRows.Count -lt 1000) { break }
        if ($page -eq [Math]::Max(1, $MaxTracePages)) {
            $script:LastTraceComplete = $false
        }
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
    param(
        [object]$Trace,
        [datetime]$StartDate,
        [datetime]$EndDate
    )

    $traceText = ConvertTo-TraceText $Trace
    $detailText = ""
    $details = @()
    if ($traceText -notmatch $ThresholdPattern) {
        $details = @(Get-TraceDetails -Trace $Trace -StartDate $StartDate -EndDate $EndDate)
        $detailText = (($details | ForEach-Object { ConvertTo-TraceText $_ }) -join "`n")
    }

    $combined = "$traceText`n$detailText"
    return [pscustomobject]@{
        HasThreshold = ($combined -match $ThresholdPattern)
        DetailCount = $details.Count
        Evidence = ConvertTo-EvidenceSnippet $combined
    }
}

function Get-TenantThresholdOnset {
    param([object]$Target, [datetime]$StartDate, [datetime]$EndDate)

    $domainSet = @{}
    foreach ($domain in @($Target.Domains)) {
        $normalized = Normalize-Domain $domain
        if ($normalized) { $domainSet[$normalized] = $true }
    }

    $securePwd = ConvertTo-SecureString ([string]$Target.AdminPassword) -AsPlainText -Force
    $creds = New-Object System.Management.Automation.PSCredential(([string]$Target.AdminEmail), $securePwd)
    Connect-ExchangeOnline -Credential $creds -ShowBanner:$false -ErrorAction Stop

    try {
        $recentTraces = @(Get-RecentMessageTraces -StartDate $StartDate -EndDate $EndDate)
        $outbound = @($recentTraces | Where-Object {
            $sender = ([string]$_.SenderAddress).Trim().ToLowerInvariant()
            $senderDomain = Get-EmailDomain $sender
            $sender -and $domainSet.ContainsKey($senderDomain)
        } | Sort-Object @{ Expression = { Get-TraceReceived $_ }; Ascending = $true })

        $failedOutbound = @($outbound | Where-Object { ([string]$_.Status).Trim() -eq "Failed" })
        $firstOutbound = if ($outbound.Count -gt 0) { $outbound[0] } else { $null }
        $lastOutbound = if ($outbound.Count -gt 0) { $outbound[$outbound.Count - 1] } else { $null }

        $detailsChecked = 0
        $earliestThresholdTrace = $null
        $earliestThresholdCheck = $null
        foreach ($trace in $failedOutbound) {
            if ($detailsChecked -ge [Math]::Max(1, $MaxFailedDetailsToInspect)) { break }
            $detailsChecked += 1
            $check = Test-TraceHasThreshold -Trace $trace -StartDate $StartDate -EndDate $EndDate
            if ($check.HasThreshold) {
                $earliestThresholdTrace = $trace
                $earliestThresholdCheck = $check
                break
            }
        }

        $latestCleanBefore = $null
        if ($earliestThresholdTrace) {
            $thresholdAt = Get-TraceReceived $earliestThresholdTrace
            $latestCleanBefore = @($outbound | Where-Object {
                $received = Get-TraceReceived $_
                $received -and $thresholdAt -and $received -lt $thresholdAt -and ([string]$_.Status).Trim() -ne "Failed"
            } | Sort-Object @{ Expression = { Get-TraceReceived $_ }; Ascending = $false } | Select-Object -First 1)
            if ($latestCleanBefore.Count -gt 0) { $latestCleanBefore = $latestCleanBefore[0] } else { $latestCleanBefore = $null }
        }

        $allOutboundFailed = ($outbound.Count -gt 0 -and $failedOutbound.Count -eq $outbound.Count)
        $observed = "No"
        $method = ""
        $conclusion = "No threshold proof found in the returned outbound trace window."
        if ($earliestThresholdTrace) {
            $observed = "Yes"
            $method = "EarliestFailedTraceDetail"
            $thresholdAtText = (Get-TraceReceived $earliestThresholdTrace).ToUniversalTime().ToString("o")
            if ($latestCleanBefore) {
                $cleanAtText = (Get-TraceReceived $latestCleanBefore).ToUniversalTime().ToString("o")
                $conclusion = "Threshold first observed at $thresholdAtText; latest clean outbound before that was $cleanAtText."
            } else {
                $conclusion = "Threshold was already active by $thresholdAtText; no clean outbound found earlier in this trace window."
            }
        } elseif ($allOutboundFailed) {
            $observed = "Likely"
            $method = "AllReturnedOutboundFailedNoDetailProof"
            $conclusion = "All returned outbound traces failed, but no threshold detail proof was found within inspected failed rows."
        }

        return [pscustomobject]@{
            AdminEmail = $Target.AdminEmail
            DomainCount = @($Target.Domains).Count
            Domains = (@($Target.Domains) -join ",")
            TraceWindowHours = $HoursBack
            TracePagesFetched = $script:LastTracePagesFetched
            TraceComplete = $script:LastTraceComplete
            OldestTraceFetchedAtUtc = $script:LastTraceOldestAtUtc
            FirstOutboundInWindowAtUtc = if ($firstOutbound) { (Get-TraceReceived $firstOutbound).ToUniversalTime().ToString("o") } else { "" }
            LastOutboundInWindowAtUtc = if ($lastOutbound) { (Get-TraceReceived $lastOutbound).ToUniversalTime().ToString("o") } else { "" }
            OutboundTraceCount = $outbound.Count
            FailedOutboundTraceCount = $failedOutbound.Count
            DetailsChecked = $detailsChecked
            ThresholdObserved = $observed
            DetectionMethod = $method
            EarliestThresholdAtUtc = if ($earliestThresholdTrace) { (Get-TraceReceived $earliestThresholdTrace).ToUniversalTime().ToString("o") } else { "" }
            EarliestThresholdSender = if ($earliestThresholdTrace) { [string]$earliestThresholdTrace.SenderAddress } else { "" }
            EarliestThresholdRecipient = if ($earliestThresholdTrace) { [string]$earliestThresholdTrace.RecipientAddress } else { "" }
            EarliestThresholdStatus = if ($earliestThresholdTrace) { [string]$earliestThresholdTrace.Status } else { "" }
            EarliestThresholdSubject = if ($earliestThresholdTrace) { [string]$earliestThresholdTrace.Subject } else { "" }
            LatestCleanBeforeThresholdAtUtc = if ($latestCleanBefore) { (Get-TraceReceived $latestCleanBefore).ToUniversalTime().ToString("o") } else { "" }
            LatestCleanBeforeThresholdSender = if ($latestCleanBefore) { [string]$latestCleanBefore.SenderAddress } else { "" }
            LatestCleanBeforeThresholdRecipient = if ($latestCleanBefore) { [string]$latestCleanBefore.RecipientAddress } else { "" }
            LatestCleanBeforeThresholdStatus = if ($latestCleanBefore) { [string]$latestCleanBefore.Status } else { "" }
            OnsetConclusion = $conclusion
            Evidence = if ($earliestThresholdCheck) { [string]$earliestThresholdCheck.Evidence } else { "" }
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

$targets = @(Get-BatchTenantTargets -BatchId $BatchId)
if ($AdminEmail) {
    $normalizedAdminEmail = ([string]$AdminEmail).Trim().ToLowerInvariant()
    $targets = @($targets | Where-Object { ([string]$_.AdminEmail).Trim().ToLowerInvariant() -eq $normalizedAdminEmail })
    if ($targets.Count -eq 0) { throw "Admin $AdminEmail was not found in batch $BatchId assignments." }
}
if ($InputCsv) {
    if (-not (Test-Path $InputCsv)) { throw "Input CSV not found: $InputCsv" }
    $acceptedStatuses = @(([string]$InputCsvStatus).Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    $inputRows = @(Import-Csv -Path $InputCsv)
    $adminSet = @{}
    foreach ($row in $inputRows) {
        $status = ([string]$row.HasThreshold).Trim()
        if ($acceptedStatuses.Count -gt 0 -and $acceptedStatuses -notcontains $status) { continue }
        $admin = ([string]$row.AdminEmail).Trim().ToLowerInvariant()
        if ($admin) { $adminSet[$admin] = $true }
    }
    $targets = @($targets | Where-Object { $adminSet.ContainsKey(([string]$_.AdminEmail).Trim().ToLowerInvariant()) })
    if ($targets.Count -eq 0) { throw "No admins matched InputCsv=$InputCsv with HasThreshold in $InputCsvStatus" }
}
if ($SkipTenants -gt 0) { $targets = @($targets | Select-Object -Skip $SkipTenants) }
if ($LimitTenants -gt 0) { $targets = @($targets | Select-Object -First $LimitTenants) }

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
if (-not $OutputCsv) {
    $OutputCsv = Join-Path $LogDir "jack-tenant-threshold-onset-$stamp.csv"
}

if ($StartDateUtc -or $EndDateUtc) {
    if (-not $StartDateUtc -or -not $EndDateUtc) {
        throw "StartDateUtc and EndDateUtc must be provided together."
    }
    $startDate = ([datetime]::Parse($StartDateUtc)).ToUniversalTime()
    $endDate = ([datetime]::Parse($EndDateUtc)).ToUniversalTime()
    if ($endDate -le $startDate) { throw "EndDateUtc must be after StartDateUtc." }
} else {
    $startDate = (Get-Date).ToUniversalTime().AddHours(-1 * [Math]::Max(1, $HoursBack))
    $endDate = (Get-Date).ToUniversalTime()
}

Write-Log "Checking threshold onset for $($targets.Count) Jack tenant admin(s) from $startDate to $endDate" -Level Info

$results = New-Object System.Collections.Generic.List[object]
$index = 0
foreach ($target in $targets) {
    $index += 1
    Write-Log "[$index/$($targets.Count)] Onset trace: $($target.AdminEmail) ($(@($target.Domains).Count) Jack domain(s))" -Level Info
    try {
        $result = Get-TenantThresholdOnset -Target $target -StartDate $startDate -EndDate $endDate
        $results.Add($result) | Out-Null
        $results | Export-Csv -Path $OutputCsv -NoTypeInformation
        $level = if ($result.ThresholdObserved -eq "Yes") { "Error" } elseif ($result.ThresholdObserved -eq "Likely") { "Warning" } else { "Success" }
        Write-Log "[$index/$($targets.Count)] $($target.AdminEmail): observed=$($result.ThresholdObserved), first=$($result.EarliestThresholdAtUtc), checked=$($result.DetailsChecked)" -Level $level
    } catch {
        $message = "$($_.Exception.Message)"
        if ($_.ScriptStackTrace) { $message = "$message`n$($_.ScriptStackTrace)" }
        $results.Add([pscustomobject]@{
            AdminEmail = $target.AdminEmail
            DomainCount = @($target.Domains).Count
            Domains = (@($target.Domains) -join ",")
            TraceWindowHours = $HoursBack
            TracePagesFetched = 0
            TraceComplete = $false
            OldestTraceFetchedAtUtc = ""
            FirstOutboundInWindowAtUtc = ""
            LastOutboundInWindowAtUtc = ""
            OutboundTraceCount = 0
            FailedOutboundTraceCount = 0
            DetailsChecked = 0
            ThresholdObserved = "Unknown"
            DetectionMethod = ""
            EarliestThresholdAtUtc = ""
            EarliestThresholdSender = ""
            EarliestThresholdRecipient = ""
            EarliestThresholdStatus = ""
            EarliestThresholdSubject = ""
            LatestCleanBeforeThresholdAtUtc = ""
            LatestCleanBeforeThresholdSender = ""
            LatestCleanBeforeThresholdRecipient = ""
            LatestCleanBeforeThresholdStatus = ""
            OnsetConclusion = ""
            Evidence = ""
            Error = $message
        }) | Out-Null
        $results | Export-Csv -Path $OutputCsv -NoTypeInformation
        Write-Log "[$index/$($targets.Count)] $($target.AdminEmail): $message" -Level Error
    }
}

$results | Export-Csv -Path $OutputCsv -NoTypeInformation

$yes = @($results | Where-Object { $_.ThresholdObserved -eq "Yes" }).Count
$likely = @($results | Where-Object { $_.ThresholdObserved -eq "Likely" }).Count
$no = @($results | Where-Object { $_.ThresholdObserved -eq "No" }).Count
$unknown = @($results | Where-Object { $_.ThresholdObserved -eq "Unknown" }).Count
Write-Log "Jack tenant threshold onset complete. Yes=$yes Likely=$likely No=$no Unknown=$unknown Output=$OutputCsv" -Level Success
