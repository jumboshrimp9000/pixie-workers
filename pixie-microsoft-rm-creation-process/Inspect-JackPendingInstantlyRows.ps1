param(
    [Parameter(Mandatory = $true)]
    [string]$Domain,
    [string]$Tag = "Mailboxpro 5/10"
)

$ErrorActionPreference = "Stop"
. "$PSScriptRoot/config.ps1"

function Invoke-InstantlyGet {
    param(
        [string]$ApiKey,
        [string]$Path
    )

    return Invoke-RestMethod -Method GET -Uri ("https://api.instantly.ai/api/v2/" + $Path) -Headers @{
        Authorization = "Bearer $ApiKey"
    } -TimeoutSec 30
}

$domainRow = ((Invoke-SupabaseApi -Method GET -Table "domains" -Query "domain=eq.$Domain&select=id,domain,status,interim_status&limit=1").Data | Select-Object -First 1)
if (-not $domainRow) { throw "Domain not found: $Domain" }

$pendingRows = @((Invoke-SupabaseApi -Method GET -Table "inboxes" -Query "domain_id=eq.$($domainRow.id)&status=eq.pending&select=id,email,username,password,updated_at&limit=50").Data)
$activeRows = @((Invoke-SupabaseApi -Method GET -Table "inboxes" -Query "domain_id=eq.$($domainRow.id)&status=eq.active&select=id,email&limit=200").Data)

$credRows = @((Invoke-SupabaseApi -Method GET -Table "domain_credentials" -Query "domain_id=eq.$($domainRow.id)&select=sending_tool_credentials(api_key,sending_tools(slug))&limit=20").Data)
$credential = @($credRows | Where-Object { $_.sending_tool_credentials.sending_tools.slug -eq "instantly" } | Select-Object -First 1)
if ($credential.Count -eq 0) { throw "No Instantly credential found for $Domain" }
$apiKey = [string]$credential[0].sending_tool_credentials.api_key
if ([string]::IsNullOrWhiteSpace($apiKey)) { throw "Instantly API key is empty for $Domain" }

$tagsResponse = Invoke-InstantlyGet -ApiKey $apiKey -Path "custom-tags?limit=100"
$tagRows = if ($tagsResponse.items) { @($tagsResponse.items) } elseif ($tagsResponse.data) { @($tagsResponse.data) } elseif ($tagsResponse -is [array]) { @($tagsResponse) } else { @() }
$tagRow = @($tagRows | Where-Object {
    ([string]($_.label ?? $_.name ?? $_.title)).Trim().ToLowerInvariant() -eq $Tag.Trim().ToLowerInvariant()
} | Select-Object -First 1)
if ($tagRow.Count -eq 0) { throw "Instantly tag not found: $Tag" }
$tagId = [string]($tagRow[0].id ?? $tagRow[0].tag_id)

$results = [System.Collections.Generic.List[object]]::new()
foreach ($row in $pendingRows) {
    $email = ([string]$row.email).Trim().ToLowerInvariant()
    if (-not $email) {
        $username = ([string]$row.username).Trim().ToLowerInvariant()
        if ($username) { $email = "$username@$Domain" }
    }
    if (-not $email) { continue }

    $account = $null
    $accountError = $null
    $mappingError = $null
    $mappingRows = @()

    try {
        $account = Invoke-InstantlyGet -ApiKey $apiKey -Path ("accounts/" + [uri]::EscapeDataString($email))
    } catch {
        $accountError = $_.Exception.Message
    }

    try {
        $mappings = Invoke-InstantlyGet -ApiKey $apiKey -Path ("custom-tag-mappings?limit=100&tag_ids=" + [uri]::EscapeDataString($tagId) + "&resource_ids=" + [uri]::EscapeDataString($email))
        $mappingRows = if ($mappings.items) { @($mappings.items) } elseif ($mappings.data) { @($mappings.data) } elseif ($mappings -is [array]) { @($mappings) } else { @() }
    } catch {
        $mappingError = $_.Exception.Message
    }

    $results.Add([pscustomobject]@{
        email = $email
        db_status = "pending"
        account_found = [bool]$account
        account_error = $accountError
        daily_limit = if ($account) { $account.daily_limit } else { $null }
        sending_gap = if ($account) { $account.sending_gap } else { $null }
        warmup_limit = if ($account -and $account.warmup) { $account.warmup.limit } else { $null }
        warmup_reply_rate = if ($account -and $account.warmup) { $account.warmup.reply_rate } else { $null }
        tag_mapped = (@($mappingRows).Count -gt 0)
        mapping_error = $mappingError
        db_updated_at = $row.updated_at
    }) | Out-Null
}

[pscustomobject]@{
    domain = $Domain
    domain_status = $domainRow.status
    domain_interim_status = $domainRow.interim_status
    active_count = $activeRows.Count
    pending_count = $pendingRows.Count
    pending_rows = @($results)
} | ConvertTo-Json -Depth 6
