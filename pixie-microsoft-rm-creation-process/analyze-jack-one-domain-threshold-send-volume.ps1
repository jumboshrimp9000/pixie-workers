<#
.SYNOPSIS
    Analyzes outbound send volume around threshold onset for Jack one-domain tenants.
.DESCRIPTION
    One-off operational analysis for the Jack/ProfitPath Azure threshold incident.
    The script loads the Jack batch admin/domain mapping from Simple Inboxes,
    limits to threshold-confirmed admins with exactly one assigned Jack domain,
    pulls Exchange message trace rows for that tenant, and exports per-tenant
    send counts around the first observed threshold bounce.
#>

param(
    [string]$BatchId = "ffa54dd8-3fd1-4336-8e06-6f73872432f4",
    [string]$ThresholdCsv = (Join-Path $PSScriptRoot "logs/jack-tenant-threshold-hard-confirm-227.csv"),
    [string]$ThresholdRescanCsv = (Join-Path $PSScriptRoot "logs/jack-tenant-threshold-hard-confirm-nonyes-rescan.csv"),
    [int]$SkipTenants = 0,
    [int]$LimitTenants = 0,
    [string]$AdminEmail = "",
    [string]$StartDateUtc = "2026-05-09T00:00:00Z",
    [string]$EndDateUtc = "",
    [int]$MaxTracePages = 20,
    [int]$MaxFailedDetailsToInspect = 300,
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
    if ($value.Length -gt 900) { return $value.Substring(0, 900) }
    return $value
}

function Get-ThresholdConfirmedAdmins {
    $set = @{}
    foreach ($path in @($ThresholdCsv, $ThresholdRescanCsv)) {
        if (-not $path -or -not (Test-Path $path)) { continue }
        foreach ($row in @(Import-Csv -Path $path)) {
            $admin = ([string]$row.AdminEmail).Trim().ToLowerInvariant()
            $status = ([string]$row.HasThreshold).Trim()
            if ($admin -and $status -eq "Yes") { $set[$admin] = $true }
        }
    }
    return $set
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
        $oldest = @($pageRows | Sort-Object @{ Expression = { Get-TraceReceived $_ }; Ascending = $true } | Select-Object -First 1)
        if ($oldest.Count -gt 0) {
            $oldestAt = Get-TraceReceived $oldest[0]
            if ($oldestAt) { $script:LastTraceOldestAtUtc = $oldestAt.ToUniversalTime().ToString("o") }
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
    $detailCount = 0
    if ($traceText -notmatch $ThresholdPattern) {
        $details = @(Get-TraceDetails -Trace $Trace -StartDate $StartDate -EndDate $EndDate)
        $detailCount = $details.Count
        $detailText = (($details | ForEach-Object { ConvertTo-TraceText $_ }) -join "`n")
    }

    $combined = "$traceText`n$detailText"
    return [pscustomobject]@{
        HasThreshold = ($combined -match $ThresholdPattern)
        DetailCount = $detailCount
        Evidence = ConvertTo-EvidenceSnippet $combined
    }
}

function Count-Between {
    param(
        [object[]]$Rows,
        [datetime]$Start,
        [datetime]$End,
        [string]$Status = ""
    )

    return @($Rows | Where-Object {
        $received = Get-TraceReceived $_
        if (-not $received) { return $false }
        $ok = ($received -ge $Start -and $received -lt $End)
        if (-not $ok) { return $false }
        if ($Status) { return (([string]$_.Status).Trim() -eq $Status) }
        return $true
    }).Count
}

function Count-Before {
    param(
        [object[]]$Rows,
        [datetime]$End,
        [string]$Status = ""
    )

    return @($Rows | Where-Object {
        $received = Get-TraceReceived $_
        if (-not $received) { return $false }
        $ok = ($received -lt $End)
        if (-not $ok) { return $false }
        if ($Status) { return (([string]$_.Status).Trim() -eq $Status) }
        return $true
    }).Count
}

function Get-AcceptedDomainSet {
    $set = @{}
    foreach ($domain in @(Get-AcceptedDomain -ErrorAction Stop)) {
        $name = Normalize-Domain ([string]$domain.DomainName)
        if ($name) { $set[$name] = $true }
    }
    return $set
}

function Test-TenantOutboundTrace {
    param(
        [object]$Trace,
        [hashtable]$AcceptedDomainSet
    )
    $sender = ([string]$Trace.SenderAddress).Trim().ToLowerInvariant()
    if (-not $sender -or $sender -notmatch "@") { return $false }
    if ($sender.StartsWith("microsoftexchange")) { return $false }
    if ($sender.StartsWith("postmaster@")) { return $false }
    if ($AcceptedDomainSet) {
        $recipientDomain = Get-EmailDomain ([string]$Trace.RecipientAddress)
        if (-not $recipientDomain) { return $false }
        if ($AcceptedDomainSet.ContainsKey($recipientDomain)) { return $false }
    }
    return $true
}

function Convert-DayCounts {
    param([object[]]$Rows)

    $counts = @{}
    foreach ($row in @($Rows)) {
        $received = Get-TraceReceived $row
        if (-not $received) { continue }
        $key = $received.ToUniversalTime().ToString("yyyy-MM-dd")
        if (-not $counts.ContainsKey($key)) { $counts[$key] = 0 }
        $counts[$key] += 1
    }
    return (($counts.GetEnumerator() | Sort-Object Name | ForEach-Object { "$($_.Name)=$($_.Value)" }) -join ";")
}

function Convert-DomainCounts {
    param([object[]]$Rows, [int]$Top = 12)

    $counts = @{}
    foreach ($row in @($Rows)) {
        $domain = Get-EmailDomain ([string]$row.SenderAddress)
        if (-not $domain) { continue }
        if (-not $counts.ContainsKey($domain)) { $counts[$domain] = 0 }
        $counts[$domain] += 1
    }
    return (($counts.GetEnumerator() |
        Sort-Object @{ Expression = { $_.Value }; Descending = $true }, @{ Expression = { $_.Name }; Ascending = $true } |
        Select-Object -First ([Math]::Max(1, $Top)) |
        ForEach-Object { "$($_.Name)=$($_.Value)" }) -join ";")
}

function Find-FirstThresholdTrace {
    param(
        [object[]]$Rows,
        [datetime]$StartDate,
        [datetime]$EndDate,
        [int]$MaxDetails
    )

    $detailsChecked = 0
    foreach ($trace in @($Rows | Where-Object { ([string]$_.Status).Trim() -eq "Failed" } | Sort-Object @{ Expression = { Get-TraceReceived $_ }; Ascending = $true })) {
        if ($detailsChecked -ge [Math]::Max(1, $MaxDetails)) { break }
        $detailsChecked += 1
        $check = Test-TraceHasThreshold -Trace $trace -StartDate $StartDate -EndDate $EndDate
        if ($check.HasThreshold) {
            return [pscustomobject]@{
                Trace = $trace
                Check = $check
                DetailsChecked = $detailsChecked
            }
        }
    }

    return [pscustomobject]@{
        Trace = $null
        Check = $null
        DetailsChecked = $detailsChecked
    }
}

function Get-LatestCleanBefore {
    param([object[]]$Rows, $Before)

    if (-not $Before) { return $null }
    $beforeAt = ([datetime]$Before).ToUniversalTime()
    $matches = @($Rows | Where-Object {
        $received = Get-TraceReceived $_
        $received -and $received.ToUniversalTime() -lt $beforeAt -and ([string]$_.Status).Trim() -ne "Failed"
    } | Sort-Object @{ Expression = { Get-TraceReceived $_ }; Ascending = $false } | Select-Object -First 1)
    if ($matches.Count -gt 0) { return $matches[0] }
    return $null
}

function Get-WindowMetrics {
    param([object[]]$Rows, $FirstThresholdAt)

    if (-not $FirstThresholdAt) {
        return [pscustomobject]@{
            Sends24hBefore = ""
            Delivered24hBefore = ""
            Failed24hBefore = ""
            SendsPreviousUtcDay = ""
            DeliveredPreviousUtcDay = ""
            FailedPreviousUtcDay = ""
            SendsThresholdUtcDayBefore = ""
            DeliveredThresholdUtcDayBefore = ""
            FailedThresholdUtcDayBefore = ""
            SendsBeforeInFetchedWindow = ""
            DeliveredBeforeInFetchedWindow = ""
            FailedBeforeInFetchedWindow = ""
        }
    }

    $firstThresholdDate = ([datetime]$FirstThresholdAt).ToUniversalTime()
    $thresholdDayStart = $firstThresholdDate.Date
    $previousDayStart = $firstThresholdDate.Date.AddDays(-1)
    $previousDayEnd = $firstThresholdDate.Date
    $twentyFourHourStart = $firstThresholdDate.AddHours(-24)

    return [pscustomobject]@{
        Sends24hBefore = Count-Between -Rows $Rows -Start $twentyFourHourStart -End $firstThresholdDate
        Delivered24hBefore = Count-Between -Rows $Rows -Start $twentyFourHourStart -End $firstThresholdDate -Status "Delivered"
        Failed24hBefore = Count-Between -Rows $Rows -Start $twentyFourHourStart -End $firstThresholdDate -Status "Failed"
        SendsPreviousUtcDay = Count-Between -Rows $Rows -Start $previousDayStart -End $previousDayEnd
        DeliveredPreviousUtcDay = Count-Between -Rows $Rows -Start $previousDayStart -End $previousDayEnd -Status "Delivered"
        FailedPreviousUtcDay = Count-Between -Rows $Rows -Start $previousDayStart -End $previousDayEnd -Status "Failed"
        SendsThresholdUtcDayBefore = Count-Between -Rows $Rows -Start $thresholdDayStart -End $firstThresholdDate
        DeliveredThresholdUtcDayBefore = Count-Between -Rows $Rows -Start $thresholdDayStart -End $firstThresholdDate -Status "Delivered"
        FailedThresholdUtcDayBefore = Count-Between -Rows $Rows -Start $thresholdDayStart -End $firstThresholdDate -Status "Failed"
        SendsBeforeInFetchedWindow = Count-Before -Rows $Rows -End $firstThresholdDate
        DeliveredBeforeInFetchedWindow = Count-Before -Rows $Rows -End $firstThresholdDate -Status "Delivered"
        FailedBeforeInFetchedWindow = Count-Before -Rows $Rows -End $firstThresholdDate -Status "Failed"
    }
}

function Analyze-Tenant {
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
        $acceptedDomainSet = Get-AcceptedDomainSet
        foreach ($domain in @($domainSet.Keys)) {
            if ($domain) { $acceptedDomainSet[$domain] = $true }
        }
        $acceptedDomains = (($acceptedDomainSet.Keys | Sort-Object) -join ";")

        $allTraces = @(Get-PagedMessageTraces -StartDate $StartDate -EndDate $EndDate)
        $tenantOutbound = @($allTraces | Where-Object { Test-TenantOutboundTrace -Trace $_ -AcceptedDomainSet $acceptedDomainSet } | Sort-Object @{ Expression = { Get-TraceReceived $_ }; Ascending = $true })
        $tenantFailedOutbound = @($tenantOutbound | Where-Object { ([string]$_.Status).Trim() -eq "Failed" })
        $tenantDeliveredOutbound = @($tenantOutbound | Where-Object { ([string]$_.Status).Trim() -eq "Delivered" })

        $outbound = @($allTraces | Where-Object {
            $sender = ([string]$_.SenderAddress).Trim().ToLowerInvariant()
            $senderDomain = Get-EmailDomain $sender
            $sender -and $domainSet.ContainsKey($senderDomain)
        } | Sort-Object @{ Expression = { Get-TraceReceived $_ }; Ascending = $true })
        $failedOutbound = @($outbound | Where-Object { ([string]$_.Status).Trim() -eq "Failed" })
        $deliveredOutbound = @($outbound | Where-Object { ([string]$_.Status).Trim() -eq "Delivered" })

        $tenantThreshold = Find-FirstThresholdTrace -Rows $tenantOutbound -StartDate $StartDate -EndDate $EndDate -MaxDetails $MaxFailedDetailsToInspect
        $assignedThreshold = Find-FirstThresholdTrace -Rows $outbound -StartDate $StartDate -EndDate $EndDate -MaxDetails $MaxFailedDetailsToInspect

        $firstTenantThresholdTrace = $tenantThreshold.Trace
        $firstTenantThresholdCheck = $tenantThreshold.Check
        $firstTenantThresholdAt = if ($firstTenantThresholdTrace) { (Get-TraceReceived $firstTenantThresholdTrace).ToUniversalTime() } else { $null }
        $latestTenantCleanBefore = Get-LatestCleanBefore -Rows $tenantOutbound -Before $firstTenantThresholdAt
        $tenantMetrics = Get-WindowMetrics -Rows $tenantOutbound -FirstThresholdAt $firstTenantThresholdAt

        $firstThresholdTrace = $assignedThreshold.Trace
        $firstThresholdCheck = $assignedThreshold.Check
        $firstThresholdAt = if ($firstThresholdTrace) { (Get-TraceReceived $firstThresholdTrace).ToUniversalTime() } else { $null }
        $latestCleanBefore = Get-LatestCleanBefore -Rows $outbound -Before $firstThresholdAt
        $assignedMetrics = Get-WindowMetrics -Rows $outbound -FirstThresholdAt $firstThresholdAt

        return [pscustomobject]@{
            AdminEmail = $Target.AdminEmail
            Domain = (@($Target.Domains) -join ",")
            StartDateUtc = $StartDate.ToUniversalTime().ToString("o")
            EndDateUtc = $EndDate.ToUniversalTime().ToString("o")
            TracePagesFetched = $script:LastTracePagesFetched
            TraceComplete = $script:LastTraceComplete
            OldestTraceFetchedAtUtc = $script:LastTraceOldestAtUtc
            TotalTraceRowsFetched = $allTraces.Count
            TenantAcceptedDomains = $acceptedDomains
            TenantOutboundTraceRows = $tenantOutbound.Count
            TenantDeliveredOutboundRows = $tenantDeliveredOutbound.Count
            TenantFailedOutboundRows = $tenantFailedOutbound.Count
            TenantFirstOutboundAtUtc = if ($tenantOutbound.Count -gt 0) { (Get-TraceReceived $tenantOutbound[0]).ToUniversalTime().ToString("o") } else { "" }
            TenantLastOutboundAtUtc = if ($tenantOutbound.Count -gt 0) { (Get-TraceReceived $tenantOutbound[$tenantOutbound.Count - 1]).ToUniversalTime().ToString("o") } else { "" }
            TenantFirstThresholdAtUtc = if ($firstTenantThresholdAt) { $firstTenantThresholdAt.ToString("o") } else { "" }
            TenantFirstThresholdSender = if ($firstTenantThresholdTrace) { [string]$firstTenantThresholdTrace.SenderAddress } else { "" }
            TenantFirstThresholdRecipient = if ($firstTenantThresholdTrace) { [string]$firstTenantThresholdTrace.RecipientAddress } else { "" }
            TenantLatestCleanBeforeThresholdAtUtc = if ($latestTenantCleanBefore) { (Get-TraceReceived $latestTenantCleanBefore).ToUniversalTime().ToString("o") } else { "" }
            TenantSends24hBeforeFirstThreshold = $tenantMetrics.Sends24hBefore
            TenantDelivered24hBeforeFirstThreshold = $tenantMetrics.Delivered24hBefore
            TenantFailed24hBeforeFirstThreshold = $tenantMetrics.Failed24hBefore
            TenantSendsPreviousUtcDay = $tenantMetrics.SendsPreviousUtcDay
            TenantDeliveredPreviousUtcDay = $tenantMetrics.DeliveredPreviousUtcDay
            TenantFailedPreviousUtcDay = $tenantMetrics.FailedPreviousUtcDay
            TenantSendsThresholdUtcDayBeforeFirstThreshold = $tenantMetrics.SendsThresholdUtcDayBefore
            TenantDeliveredThresholdUtcDayBeforeFirstThreshold = $tenantMetrics.DeliveredThresholdUtcDayBefore
            TenantFailedThresholdUtcDayBeforeFirstThreshold = $tenantMetrics.FailedThresholdUtcDayBefore
            TenantSendsBeforeFirstThresholdInFetchedWindow = $tenantMetrics.SendsBeforeInFetchedWindow
            TenantDeliveredBeforeFirstThresholdInFetchedWindow = $tenantMetrics.DeliveredBeforeInFetchedWindow
            TenantFailedBeforeFirstThresholdInFetchedWindow = $tenantMetrics.FailedBeforeInFetchedWindow
            TenantOutboundByUtcDay = Convert-DayCounts -Rows $tenantOutbound
            TenantDeliveredByUtcDay = Convert-DayCounts -Rows $tenantDeliveredOutbound
            TenantFailedByUtcDay = Convert-DayCounts -Rows $tenantFailedOutbound
            TenantTopSenderDomains = Convert-DomainCounts -Rows $tenantOutbound
            OutboundTraceRows = $outbound.Count
            DeliveredOutboundRows = $deliveredOutbound.Count
            FailedOutboundRows = $failedOutbound.Count
            FirstOutboundAtUtc = if ($outbound.Count -gt 0) { (Get-TraceReceived $outbound[0]).ToUniversalTime().ToString("o") } else { "" }
            LastOutboundAtUtc = if ($outbound.Count -gt 0) { (Get-TraceReceived $outbound[$outbound.Count - 1]).ToUniversalTime().ToString("o") } else { "" }
            FirstThresholdAtUtc = if ($firstThresholdAt) { $firstThresholdAt.ToString("o") } else { "" }
            FirstThresholdSender = if ($firstThresholdTrace) { [string]$firstThresholdTrace.SenderAddress } else { "" }
            FirstThresholdRecipient = if ($firstThresholdTrace) { [string]$firstThresholdTrace.RecipientAddress } else { "" }
            LatestCleanBeforeThresholdAtUtc = if ($latestCleanBefore) { (Get-TraceReceived $latestCleanBefore).ToUniversalTime().ToString("o") } else { "" }
            Sends24hBeforeFirstThreshold = $assignedMetrics.Sends24hBefore
            Delivered24hBeforeFirstThreshold = $assignedMetrics.Delivered24hBefore
            Failed24hBeforeFirstThreshold = $assignedMetrics.Failed24hBefore
            SendsPreviousUtcDay = $assignedMetrics.SendsPreviousUtcDay
            DeliveredPreviousUtcDay = $assignedMetrics.DeliveredPreviousUtcDay
            FailedPreviousUtcDay = $assignedMetrics.FailedPreviousUtcDay
            SendsThresholdUtcDayBeforeFirstThreshold = $assignedMetrics.SendsThresholdUtcDayBefore
            DeliveredThresholdUtcDayBeforeFirstThreshold = $assignedMetrics.DeliveredThresholdUtcDayBefore
            FailedThresholdUtcDayBeforeFirstThreshold = $assignedMetrics.FailedThresholdUtcDayBefore
            SendsBeforeFirstThresholdInFetchedWindow = $assignedMetrics.SendsBeforeInFetchedWindow
            DeliveredBeforeFirstThresholdInFetchedWindow = $assignedMetrics.DeliveredBeforeInFetchedWindow
            FailedBeforeFirstThresholdInFetchedWindow = $assignedMetrics.FailedBeforeInFetchedWindow
            OutboundByUtcDay = Convert-DayCounts -Rows $outbound
            DeliveredByUtcDay = Convert-DayCounts -Rows $deliveredOutbound
            FailedByUtcDay = Convert-DayCounts -Rows $failedOutbound
            DetailsChecked = $tenantThreshold.DetailsChecked
            AssignedDomainDetailsChecked = $assignedThreshold.DetailsChecked
            ThresholdEvidenceFound = if ($firstTenantThresholdTrace) { "Yes" } else { "No" }
            AssignedDomainThresholdEvidenceFound = if ($firstThresholdTrace) { "Yes" } else { "No" }
            Evidence = if ($firstTenantThresholdCheck) { [string]$firstTenantThresholdCheck.Evidence } else { "" }
            AssignedDomainEvidence = if ($firstThresholdCheck) { [string]$firstThresholdCheck.Evidence } else { "" }
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

$startDate = ([datetime]::Parse($StartDateUtc)).ToUniversalTime()
$endDate = if ($EndDateUtc) { ([datetime]::Parse($EndDateUtc)).ToUniversalTime() } else { (Get-Date).ToUniversalTime() }
if ($endDate -le $startDate) { throw "EndDateUtc must be after StartDateUtc." }

$thresholdAdmins = Get-ThresholdConfirmedAdmins
if ($thresholdAdmins.Count -eq 0) { throw "No threshold-confirmed admins loaded from $ThresholdCsv / $ThresholdRescanCsv" }

$targets = @(Get-BatchTenantTargets -BatchId $BatchId | Where-Object {
    $admin = ([string]$_.AdminEmail).Trim().ToLowerInvariant()
    $thresholdAdmins.ContainsKey($admin) -and @($_.Domains).Count -eq 1
})

if ($AdminEmail) {
    $normalizedAdminEmail = ([string]$AdminEmail).Trim().ToLowerInvariant()
    $targets = @($targets | Where-Object { ([string]$_.AdminEmail).Trim().ToLowerInvariant() -eq $normalizedAdminEmail })
    if ($targets.Count -eq 0) { throw "Admin $AdminEmail was not found among one-domain threshold targets." }
}
if ($SkipTenants -gt 0) { $targets = @($targets | Select-Object -Skip $SkipTenants) }
if ($LimitTenants -gt 0) { $targets = @($targets | Select-Object -First $LimitTenants) }

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
if (-not $OutputCsv) {
    $OutputCsv = Join-Path $LogDir "jack-one-domain-threshold-send-volume-$stamp.csv"
}

Write-Log "Analyzing send volume for $($targets.Count) one-domain threshold tenant(s) from $startDate to $endDate" -Level Info

$results = New-Object System.Collections.Generic.List[object]
$index = 0
foreach ($target in $targets) {
    $index += 1
    Write-Log "[$index/$($targets.Count)] Send-volume trace: $($target.AdminEmail) -> $(@($target.Domains) -join ',')" -Level Info
    try {
        $result = Analyze-Tenant -Target $target -StartDate $startDate -EndDate $endDate
        $results.Add($result) | Out-Null
        $results | Export-Csv -Path $OutputCsv -NoTypeInformation
        $level = if ($result.ThresholdEvidenceFound -eq "Yes") { "Success" } else { "Warning" }
        Write-Log "[$index/$($targets.Count)] $($target.AdminEmail): tenantThreshold=$($result.TenantFirstThresholdAtUtc), tenantPreviousDay=$($result.TenantSendsPreviousUtcDay), tenantPrior24h=$($result.TenantSends24hBeforeFirstThreshold), tenantRows=$($result.TenantOutboundTraceRows)" -Level $level
    } catch {
        $message = "$($_.Exception.Message)"
        if ($_.ScriptStackTrace) { $message = "$message`n$($_.ScriptStackTrace)" }
        $results.Add([pscustomobject]@{
            AdminEmail = $target.AdminEmail
            Domain = (@($target.Domains) -join ",")
            StartDateUtc = $startDate.ToString("o")
            EndDateUtc = $endDate.ToString("o")
            TracePagesFetched = 0
            TraceComplete = $false
            OldestTraceFetchedAtUtc = ""
            TotalTraceRowsFetched = 0
            TenantAcceptedDomains = ""
            TenantOutboundTraceRows = 0
            TenantDeliveredOutboundRows = 0
            TenantFailedOutboundRows = 0
            TenantFirstOutboundAtUtc = ""
            TenantLastOutboundAtUtc = ""
            TenantFirstThresholdAtUtc = ""
            TenantFirstThresholdSender = ""
            TenantFirstThresholdRecipient = ""
            TenantLatestCleanBeforeThresholdAtUtc = ""
            TenantSends24hBeforeFirstThreshold = ""
            TenantDelivered24hBeforeFirstThreshold = ""
            TenantFailed24hBeforeFirstThreshold = ""
            TenantSendsPreviousUtcDay = ""
            TenantDeliveredPreviousUtcDay = ""
            TenantFailedPreviousUtcDay = ""
            TenantSendsThresholdUtcDayBeforeFirstThreshold = ""
            TenantDeliveredThresholdUtcDayBeforeFirstThreshold = ""
            TenantFailedThresholdUtcDayBeforeFirstThreshold = ""
            TenantSendsBeforeFirstThresholdInFetchedWindow = ""
            TenantDeliveredBeforeFirstThresholdInFetchedWindow = ""
            TenantFailedBeforeFirstThresholdInFetchedWindow = ""
            TenantOutboundByUtcDay = ""
            TenantDeliveredByUtcDay = ""
            TenantFailedByUtcDay = ""
            TenantTopSenderDomains = ""
            OutboundTraceRows = 0
            DeliveredOutboundRows = 0
            FailedOutboundRows = 0
            FirstOutboundAtUtc = ""
            LastOutboundAtUtc = ""
            FirstThresholdAtUtc = ""
            FirstThresholdSender = ""
            FirstThresholdRecipient = ""
            LatestCleanBeforeThresholdAtUtc = ""
            Sends24hBeforeFirstThreshold = ""
            Delivered24hBeforeFirstThreshold = ""
            Failed24hBeforeFirstThreshold = ""
            SendsPreviousUtcDay = ""
            DeliveredPreviousUtcDay = ""
            FailedPreviousUtcDay = ""
            SendsThresholdUtcDayBeforeFirstThreshold = ""
            DeliveredThresholdUtcDayBeforeFirstThreshold = ""
            FailedThresholdUtcDayBeforeFirstThreshold = ""
            SendsBeforeFirstThresholdInFetchedWindow = ""
            DeliveredBeforeFirstThresholdInFetchedWindow = ""
            FailedBeforeFirstThresholdInFetchedWindow = ""
            OutboundByUtcDay = ""
            DeliveredByUtcDay = ""
            FailedByUtcDay = ""
            DetailsChecked = 0
            ThresholdEvidenceFound = "Unknown"
            Evidence = ""
            Error = $message
        }) | Out-Null
        $results | Export-Csv -Path $OutputCsv -NoTypeInformation
        Write-Log "[$index/$($targets.Count)] $($target.AdminEmail): $message" -Level Error
    }
}

$results | Export-Csv -Path $OutputCsv -NoTypeInformation
$yes = @($results | Where-Object { $_.ThresholdEvidenceFound -eq "Yes" }).Count
$unknown = @($results | Where-Object { $_.ThresholdEvidenceFound -eq "Unknown" }).Count
$no = @($results | Where-Object { $_.ThresholdEvidenceFound -eq "No" }).Count
Write-Log "Send-volume threshold analysis complete. Yes=$yes No=$no Unknown=$unknown Output=$OutputCsv" -Level Success
