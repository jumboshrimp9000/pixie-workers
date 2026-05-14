param(
    [string[]]$Domains = @(),
    [string]$DomainsFile = "",
    [int]$ExpectedDailyLimit = 5,
    [int]$ExpectedSendingGap = 30,
    [int]$ExpectedWarmupLimit = 5,
    [double]$ExpectedReplyRate = 60,
    [string]$ExpectedTag = "Mailboxpro 5/10",
    [switch]$Live
)

$ErrorActionPreference = "Stop"
. "$PSScriptRoot/config.ps1"

function Normalize-DomainName {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

function Get-DomainInputs {
    param(
        [string[]]$InlineDomains,
        [string]$FilePath
    )

    $values = [System.Collections.Generic.List[string]]::new()
    foreach ($domain in @($InlineDomains)) {
        if (-not [string]::IsNullOrWhiteSpace([string]$domain)) {
            $values.Add([string]$domain) | Out-Null
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($FilePath)) {
        if (-not (Test-Path $FilePath)) {
            throw "Domains file not found: $FilePath"
        }

        if ([System.IO.Path]::GetExtension($FilePath).ToLowerInvariant() -eq ".csv") {
            foreach ($row in @(Import-Csv -Path $FilePath)) {
                foreach ($column in @("domain", "Domain", "new_domain", "replacement_domain")) {
                    $candidate = [string]$row.$column
                    if (-not [string]::IsNullOrWhiteSpace($candidate)) {
                        $values.Add($candidate) | Out-Null
                        break
                    }
                }
            }
        } else {
            foreach ($line in @(Get-Content -Path $FilePath)) {
                if (-not [string]::IsNullOrWhiteSpace([string]$line)) {
                    $values.Add([string]$line) | Out-Null
                }
            }
        }
    }

    return @($values)
}

$inspectScript = Join-Path $PSScriptRoot "Inspect-JackPendingInstantlyRows.ps1"
if (-not (Test-Path $inspectScript)) { throw "Inspector not found: $inspectScript" }

$domainFilter = @{}
foreach ($domain in @(Get-DomainInputs -InlineDomains $Domains -FilePath $DomainsFile)) {
    $normalized = Normalize-DomainName $domain
    if ($normalized) { $domainFilter[$normalized] = $true }
}

$domainRows = @((Invoke-SupabaseApi -Method GET -Table "domains" -Query "select=id,domain,status,interim_status,action_history&limit=5000").Data)
$targets = [System.Collections.Generic.List[object]]::new()

foreach ($domain in $domainRows) {
    $domainName = Normalize-DomainName ([string]$domain.domain)
    if ($domainFilter.Count -gt 0 -and -not $domainFilter.ContainsKey($domainName)) { continue }
    if ([string]$domain.status -ne "active" -or [string]$domain.interim_status -ne "Both - Provisioning Complete") { continue }

    $pendingRows = @(Get-DomainInboxes -DomainId ([string]$domain.id) -Status "pending")
    if ($pendingRows.Count -eq 0) { continue }

    try {
        $inspectJson = pwsh -NoProfile -File $inspectScript -Domain $domainName -Tag $ExpectedTag 2>$null | Out-String
        $inspection = $inspectJson | ConvertFrom-Json
    } catch {
        continue
    }

    if (-not $inspection -or [int]$inspection.pending_count -eq 0) { continue }

    $verifiedIds = [System.Collections.Generic.List[string]]::new()
    foreach ($pendingDbRow in $pendingRows) {
        $email = ([string]$pendingDbRow.email).Trim().ToLowerInvariant()
        if (-not $email) {
            $username = ([string]$pendingDbRow.username).Trim().ToLowerInvariant()
            if ($username) { $email = "$username@$domainName" }
        }
        if (-not $email) { continue }

        $match = @($inspection.pending_rows | Where-Object { ([string]$_.email).Trim().ToLowerInvariant() -eq $email } | Select-Object -First 1)
        if ($match.Count -eq 0) { continue }

        $replyRate = 0.0
        try { $replyRate = [double]$match[0].warmup_reply_rate } catch { $replyRate = -1.0 }
        $ok = (
            $match[0].account_found -eq $true -and
            $match[0].tag_mapped -eq $true -and
            [int]$match[0].daily_limit -eq $ExpectedDailyLimit -and
            [int]$match[0].sending_gap -eq $ExpectedSendingGap -and
            [int]$match[0].warmup_limit -eq $ExpectedWarmupLimit -and
            [Math]::Abs($replyRate - $ExpectedReplyRate) -lt 0.001
        )
        if ($ok) {
            $verifiedIds.Add([string]$pendingDbRow.id) | Out-Null
        }
    }

    if ($verifiedIds.Count -eq 0) { continue }

    $targets.Add([pscustomobject]@{
        Domain = $domain
        DomainName = $domainName
        PendingCount = $pendingRows.Count
        VerifiedPendingIds = @($verifiedIds)
        Inspection = $inspection
    }) | Out-Null
}

Write-Host "Matched $($targets.Count) verified pending-row domain(s). Live=$([bool]$Live)"
foreach ($target in $targets) {
    Write-Host "$($target.DomainName) verified_pending=$($target.VerifiedPendingIds.Count)/$($target.PendingCount)"
    if (-not $Live) { continue }

    foreach ($inboxId in @($target.VerifiedPendingIds)) {
        Update-Inbox -InboxId $inboxId -Fields @{ status = "active" }
    }

    $history = if ($target.Domain.action_history) { [string]$target.Domain.action_history } else { "" }
    $history = Add-HistoryEntry -History $history -Entry "REPAIR: Promoted verified pending inbox row(s) to active after direct Instantly proof."
    Update-Domain -DomainId ([string]$target.Domain.id) -Fields @{ action_history = $history }
}
