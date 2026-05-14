param(
    [Parameter(Mandatory = $true)]
    [string]$Domain,
    [int]$ExpectedCount = 99,
    [int]$DailyLimit = 5,
    [int]$SendingGap = 30,
    [int]$WarmupLimit = 5,
    [double]$ReplyRatePercent = 60,
    [string]$Tag = "Mailboxpro 5/10"
)

$ErrorActionPreference = "Stop"

. "$PSScriptRoot/config.ps1"

function Convert-ReplyRateFraction {
    param([object]$Value)
    if ($null -eq $Value -or "$Value" -eq "") { return $null }
    $number = [double]$Value
    if ($number -gt 1) { return $number / 100.0 }
    return $number
}

function Invoke-InstantlyGet {
    param(
        [string]$Path,
        [hashtable]$Query = @{}
    )
    $uri = "https://api.instantly.ai/api/v2/$Path"
    if ($Query.Count -gt 0) {
        $pairs = @()
        foreach ($key in $Query.Keys) {
            $pairs += "$([uri]::EscapeDataString([string]$key))=$([uri]::EscapeDataString([string]$Query[$key]))"
        }
        $uri = "${uri}?$($pairs -join '&')"
    }
    return Invoke-RestMethod -Method GET -Uri $uri -Headers @{ Authorization = "Bearer $script:InstantlyApiKey" } -TimeoutSec 30
}

$domainResult = Invoke-SupabaseApi -Method GET -Table "domains" -Query "domain=eq.$Domain&select=id,domain,status,fulfillment_settings"
if (-not $domainResult.Success -or -not $domainResult.Data -or $domainResult.Data.Count -eq 0) {
    throw "Domain not found in Simple Inboxes: $Domain"
}
$domainRow = $domainResult.Data[0]

$inboxResult = Invoke-SupabaseApi -Method GET -Table "inboxes" -Query "domain_id=eq.$($domainRow.id)&status=eq.active&select=email,username,password"
if (-not $inboxResult.Success) { throw "Failed to load inboxes: $($inboxResult.Error)" }
$emails = @($inboxResult.Data | ForEach-Object {
    $email = [string]$_.email
    if ([string]::IsNullOrWhiteSpace($email)) { $email = "$($_.username)@$Domain" }
    $email.Trim().ToLowerInvariant()
} | Where-Object { $_ } | Sort-Object -Unique)

$credResult = Invoke-SupabaseApi -Method GET -Table "domain_credentials" -Query "domain_id=eq.$($domainRow.id)&select=sending_tool_credentials(api_key,sending_tools(slug))"
if (-not $credResult.Success -or -not $credResult.Data -or $credResult.Data.Count -eq 0) {
    throw "No sending tool credential assigned for $Domain"
}
$credential = @($credResult.Data | Where-Object { $_.sending_tool_credentials.sending_tools.slug -eq "instantly" } | Select-Object -First 1)
if (-not $credential) { throw "No Instantly credential assigned for $Domain" }
$script:InstantlyApiKey = [string]$credential.sending_tool_credentials.api_key
if ([string]::IsNullOrWhiteSpace($script:InstantlyApiKey)) { throw "Instantly API key is empty for $Domain" }

$tagsResponse = Invoke-InstantlyGet -Path "custom-tags" -Query @{ limit = 100 }
$tagRows = if ($tagsResponse.items) { @($tagsResponse.items) } elseif ($tagsResponse.data) { @($tagsResponse.data) } elseif ($tagsResponse -is [array]) { @($tagsResponse) } else { @() }
$tagRow = $tagRows | Where-Object {
    ([string]($_.label ?? $_.name ?? $_.title)).Trim().ToLowerInvariant() -eq $Tag.Trim().ToLowerInvariant()
} | Select-Object -First 1
if (-not $tagRow) { throw "Instantly tag not found: $Tag" }
$tagId = [string]($tagRow.id ?? $tagRow.tag_id)

$mapped = [System.Collections.Generic.HashSet[string]]::new()
for ($i = 0; $i -lt $emails.Count; $i += 50) {
    $chunk = $emails[$i..([Math]::Min($i + 49, $emails.Count - 1))]
    $mappings = Invoke-InstantlyGet -Path "custom-tag-mappings" -Query @{
        limit = 100
        tag_ids = $tagId
        resource_ids = ($chunk -join ",")
    }
    $rows = if ($mappings.items) { @($mappings.items) } elseif ($mappings.data) { @($mappings.data) } elseif ($mappings -is [array]) { @($mappings) } else { @() }
    foreach ($row in $rows) {
        $mappedEmail = ([string]($row.resource_id ?? $row.email ?? $row.account_email)).Trim().ToLowerInvariant()
        $mappedTagId = [string]($row.tag_id ?? $row.custom_tag_id)
        if ($mappedEmail -and ($mappedTagId -eq "" -or $mappedTagId -eq $tagId)) {
            [void]$mapped.Add($mappedEmail)
        }
    }
}

$accountFailures = @()
foreach ($email in $emails) {
    try {
        $account = Invoke-InstantlyGet -Path "accounts/$([uri]::EscapeDataString($email))"
        $warmup = $account.warmup
        $replyRate = Convert-ReplyRateFraction -Value $warmup.reply_rate
        $expectedReplyRate = Convert-ReplyRateFraction -Value $ReplyRatePercent
        $failed = @()
        if ([int]$account.daily_limit -ne $DailyLimit) { $failed += "daily_limit=$($account.daily_limit)" }
        if ([int]$account.sending_gap -ne $SendingGap) { $failed += "sending_gap=$($account.sending_gap)" }
        if ([int]$warmup.limit -ne $WarmupLimit) { $failed += "warmup.limit=$($warmup.limit)" }
        if ($null -eq $replyRate -or [Math]::Abs($replyRate - $expectedReplyRate) -gt 0.001) { $failed += "warmup.reply_rate=$($warmup.reply_rate)" }
        if (-not $mapped.Contains($email)) { $failed += "missing_tag=$Tag" }
        if ($failed.Count -gt 0) {
            $accountFailures += [pscustomobject]@{ email = $email; failures = ($failed -join "; ") }
        }
    } catch {
        $accountFailures += [pscustomobject]@{ email = $email; failures = $_.Exception.Message }
    }
}

$summary = [ordered]@{
    domain = $Domain
    expectedCount = $ExpectedCount
    dbActiveInboxes = $emails.Count
    instantlyChecked = $emails.Count
    tag = $Tag
    tagId = $tagId
    tagMappedCount = $mapped.Count
    accountFailureCount = $accountFailures.Count
    ok = ($emails.Count -eq $ExpectedCount -and $mapped.Count -eq $ExpectedCount -and $accountFailures.Count -eq 0)
    sampleFailures = @($accountFailures | Select-Object -First 10)
}

$summary | ConvertTo-Json -Depth 6
