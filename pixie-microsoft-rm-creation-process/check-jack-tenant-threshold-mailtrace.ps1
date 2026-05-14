<#
.SYNOPSIS
    Checks Jack/ProfitPath Azure tenants for 5.7.705 threshold bounces via message trace.
.DESCRIPTION
    Builds the target list from the Simple Inboxes batch/domain assignments,
    groups by assigned Microsoft admin tenant, then scans outbound failed
    message traces. Only messages whose SenderAddress domain is one of the
    Jack domains assigned to that tenant are considered.
#>

param(
    [string]$BatchId = "ffa54dd8-3fd1-4336-8e06-6f73872432f4",
    [int]$HoursBack = 24,
    [int]$LimitTenants = 0,
    [int]$OutboundSampleSize = 20,
    [string]$AdminEmail = "",
    [string]$InputCsv = "",
    [string]$InputCsvStatus = "Likely",
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

    if ($UseMessageTraceV2 -and (Get-Command Get-MessageTraceV2 -ErrorAction SilentlyContinue)) {
        return @(Get-MessageTraceV2 -StartDate $StartDate -EndDate $EndDate -ResultSize 1000 -ErrorAction Stop)
    }

    return @(Get-MessageTrace -StartDate $StartDate -EndDate $EndDate -PageSize 1000 -Page 1 -WarningAction SilentlyContinue -ErrorAction Stop)
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

function Test-TenantOutboundTrace {
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
        } | Sort-Object Received -Descending | Select-Object -First ([Math]::Max(1, $OutboundSampleSize)))

        $failedOutbound = @($outbound | Where-Object { ([string]$_.Status).Trim() -eq "Failed" })

        foreach ($trace in $outbound) {
            $status = ([string]$trace.Status).Trim()
            if ($status -ne "Failed") { continue }

            $traceText = ConvertTo-TraceText $trace
            $detailText = ""
            if ($traceText -notmatch $ThresholdPattern) {
                $details = @(Get-TraceDetails -Trace $trace -StartDate $StartDate -EndDate $EndDate)
                $detailText = (($details | ForEach-Object { ConvertTo-TraceText $_ }) -join "`n")
            }

            $combined = "$traceText`n$detailText"
            if ($combined -match $ThresholdPattern) {
                $match = [pscustomobject]@{
                    Received = $trace.Received
                    SenderAddress = $trace.SenderAddress
                    RecipientAddress = $trace.RecipientAddress
                    Subject = $trace.Subject
                    Status = $trace.Status
                    MessageTraceId = $trace.MessageTraceId
                    Evidence = ($combined -replace "\s+", " ").Trim()
                }

                return [pscustomobject]@{
                    AdminEmail = $Target.AdminEmail
                    DomainCount = @($Target.Domains).Count
                    Domains = (@($Target.Domains) -join ",")
                    TraceWindowHours = $HoursBack
                    RecentTraceCount = $recentTraces.Count
                    RecentOutboundTraceCount = $outbound.Count
                    RecentOutboundFailedCount = $failedOutbound.Count
                    AllRecentOutboundFailed = ($outbound.Count -gt 0 -and $failedOutbound.Count -eq $outbound.Count)
                    HasThreshold = "Yes"
                    DetectionMethod = "MessageTraceDetail"
                    ThresholdMatchCount = 1
                    Evidence = [string]$match.Evidence
                    SampleSender = [string]$match.SenderAddress
                    SampleRecipient = [string]$match.RecipientAddress
                    Error = ""
                }
            }
        }

        $allRecentOutboundFailed = ($outbound.Count -gt 0 -and $failedOutbound.Count -eq $outbound.Count)
        $hasThreshold = if ($allRecentOutboundFailed) {
            "Likely"
        } else {
            "No"
        }
        $detectionMethod = if ($allRecentOutboundFailed) {
            "AllRecentOutboundFailed"
        } else {
            ""
        }

        return [pscustomobject]@{
            AdminEmail = $Target.AdminEmail
            DomainCount = @($Target.Domains).Count
            Domains = (@($Target.Domains) -join ",")
            TraceWindowHours = $HoursBack
            RecentTraceCount = $recentTraces.Count
            RecentOutboundTraceCount = $outbound.Count
            RecentOutboundFailedCount = $failedOutbound.Count
            AllRecentOutboundFailed = $allRecentOutboundFailed
            HasThreshold = $hasThreshold
            DetectionMethod = $detectionMethod
            ThresholdMatchCount = 0
            Evidence = ""
            SampleSender = if ($outbound.Count -gt 0) { [string]$outbound[0].SenderAddress } else { "" }
            SampleRecipient = if ($outbound.Count -gt 0) { [string]$outbound[0].RecipientAddress } else { "" }
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
if ($LimitTenants -gt 0) { $targets = @($targets | Select-Object -First $LimitTenants) }

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
if (-not $OutputCsv) {
    $OutputCsv = Join-Path $LogDir "jack-tenant-threshold-mailtrace-$stamp.csv"
}

$startDate = (Get-Date).ToUniversalTime().AddHours(-1 * [Math]::Max(1, $HoursBack))
$endDate = (Get-Date).ToUniversalTime()

Write-Log "Checking $($targets.Count) Jack tenant admin(s) for outbound 5.7.705 traces from $startDate to $endDate" -Level Info

$results = New-Object System.Collections.Generic.List[object]
$index = 0
foreach ($target in $targets) {
    $index += 1
    Write-Log "[$index/$($targets.Count)] Message trace: $($target.AdminEmail) ($(@($target.Domains).Count) Jack domain(s))" -Level Info
    try {
        $result = Test-TenantOutboundTrace -Target $target -StartDate $startDate -EndDate $endDate
        $results.Add($result) | Out-Null
        $results | Export-Csv -Path $OutputCsv -NoTypeInformation
        $level = if ($result.HasThreshold -eq "Yes") { "Error" } else { "Success" }
        Write-Log "[$index/$($targets.Count)] $($target.AdminEmail): threshold=$($result.HasThreshold), recentOutbound=$($result.RecentOutboundTraceCount), failed=$($result.RecentOutboundFailedCount)" -Level $level
    } catch {
        $message = $_.Exception.Message
        $results.Add([pscustomobject]@{
            AdminEmail = $target.AdminEmail
            DomainCount = @($target.Domains).Count
            Domains = (@($target.Domains) -join ",")
            TraceWindowHours = $HoursBack
            RecentTraceCount = 0
            RecentOutboundTraceCount = 0
            RecentOutboundFailedCount = 0
            AllRecentOutboundFailed = $false
            HasThreshold = "Unknown"
            DetectionMethod = ""
            ThresholdMatchCount = 0
            Evidence = ""
            SampleSender = ""
            SampleRecipient = ""
            Error = $message
        }) | Out-Null
        $results | Export-Csv -Path $OutputCsv -NoTypeInformation
        Write-Log "[$index/$($targets.Count)] $($target.AdminEmail): $message" -Level Error
    }
}

$results | Export-Csv -Path $OutputCsv -NoTypeInformation

$yes = @($results | Where-Object { $_.HasThreshold -eq "Yes" }).Count
$likely = @($results | Where-Object { $_.HasThreshold -eq "Likely" }).Count
$no = @($results | Where-Object { $_.HasThreshold -eq "No" }).Count
$unknown = @($results | Where-Object { $_.HasThreshold -eq "Unknown" }).Count
Write-Log "Jack tenant threshold mailtrace complete. Yes=$yes Likely=$likely No=$no Unknown=$unknown Output=$OutputCsv" -Level Success
