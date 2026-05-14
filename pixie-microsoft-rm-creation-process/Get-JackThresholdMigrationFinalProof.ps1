<#
.SYNOPSIS
    Final proof report for the Jack/ProfitPath threshold-tenant migration plan.
.DESCRIPTION
    Audits only the domains in logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv.
    The database proof is scoped to replacement actions where payload.source is
    jack_threshold_tenant_migration and upload actions linked to those provision actions.

    Instantly deep proof is read-only and uses the same API surfaces as:
      - verify-profitpath-instantly-domain.ps1
      - tag-profitpath-instantly-accounts.mjs
      - AP/backend/src/workers/SendingToolClient.ts

    Use -SkipInstantlyDeepCheck for DB-only progress while provisioning is still running.
#>

param(
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
    [int]$ExpectedInboxes = 99,
    [int]$DailyLimit = 5,
    [int]$SendingGapMinutes = 30,
    [int]$WarmupDailyLimit = 5,
    [double]$WarmupReplyRatePercent = 60,
    [string]$Tag = "Mailboxpro 5/10",
    [switch]$SkipInstantlyDeepCheck,
    [switch]$Json,
    [string]$OutCsv,
    [string]$OutJson,
    [int]$DomainChunkSize = 8,
    [int]$InstantlyPageLimit = 30,
    [int]$InstantlyApiAttempts = 4
)

$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "config.ps1")

function Normalize-DomainName {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

function Normalize-Email {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

function ConvertTo-Array {
    param([object]$Value)
    if ($null -eq $Value) { return @() }
    if ($Value -is [System.Array]) { return @($Value) }
    return @($Value)
}

function Write-Info {
    param([string]$Message)
    if (-not $Json) { Write-Host $Message }
}

function Assert-ApiResult {
    param([hashtable]$Result, [string]$Label)
    if (-not $Result.Success) {
        throw "$Label failed: $($Result.Error)"
    }
    return @(ConvertTo-Array $Result.Data)
}

function Assert-Configured {
    if ([string]::IsNullOrWhiteSpace([string]$SupabaseConfig.Url) -or [string]::IsNullOrWhiteSpace([string]$SupabaseConfig.ServiceRoleKey)) {
        throw "Missing Supabase configuration. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in this process or in $PSScriptRoot/.env. No Instantly proof can run without the database."
    }
}

function Get-RowsByDomainChunks {
    param(
        [string]$Table,
        [string[]]$DomainIds,
        [string]$Select,
        [string]$ExtraQuery = "",
        [int]$ChunkSize = 8
    )

    $rows = [System.Collections.Generic.List[object]]::new()
    $validIds = @($DomainIds | Where-Object { $_ -and $_.Trim() } | ForEach-Object { $_.Trim() })
    if ($validIds.Count -eq 0) { return @() }

    for ($i = 0; $i -lt $validIds.Count; $i += $ChunkSize) {
        $last = [Math]::Min($i + $ChunkSize - 1, $validIds.Count - 1)
        $chunk = @($validIds[$i..$last])
        $query = "domain_id=in.($($chunk -join ','))&select=$Select&limit=20000"
        if ($ExtraQuery) { $query += "&$ExtraQuery" }
        $result = Invoke-SupabaseApi -Method GET -Table $Table -Query $query
        foreach ($row in @(Assert-ApiResult -Result $result -Label "$Table chunk")) {
            $rows.Add($row) | Out-Null
        }
    }

    return @($rows)
}

function Group-ByProperty {
    param([object[]]$Rows, [string]$Property)
    $map = @{}
    foreach ($row in @($Rows)) {
        $key = [string]$row.$Property
        if (-not $key) { continue }
        if (-not $map.ContainsKey($key)) { $map[$key] = [System.Collections.Generic.List[object]]::new() }
        $map[$key].Add($row) | Out-Null
    }
    return $map
}

function Get-FirstPropertyValue {
    param([object]$Object, [string[]]$Names)
    if ($null -eq $Object) { return $null }
    foreach ($name in $Names) {
        $property = $Object.PSObject.Properties[$name]
        if ($property) {
            $value = $property.Value
            if ($null -ne $value -and [string]$value -ne "") { return $value }
        }
    }
    return $null
}

function Get-FirstScalar {
    param([object[]]$Values)
    foreach ($value in @($Values)) {
        if ($null -eq $value -or [string]$value -eq "") { continue }
        if ($value -is [System.Array]) { continue }
        if ($value -is [pscustomobject] -or $value -is [hashtable]) { continue }
        return $value
    }
    return $null
}

function Get-FirstNumber {
    param([object[]]$Values)
    foreach ($value in @($Values)) {
        if ($null -eq $value -or [string]$value -eq "") { continue }
        $number = 0.0
        if ([double]::TryParse([string]$value, [ref]$number)) { return $number }
    }
    return $null
}

function Normalize-ReplyRatePercent {
    param([object]$Value)
    if ($null -eq $Value -or [string]$Value -eq "") { return $null }
    $number = 0.0
    if (-not [double]::TryParse([string]$Value, [ref]$number)) { return $null }
    if ($number -le 1.0) { return ($number * 100.0) }
    return $number
}

function Get-FirstBoolean {
    param([object[]]$Values)
    foreach ($value in @($Values)) {
        if ($null -eq $value -or [string]$value -eq "") { continue }
        if ($value -is [bool]) { return [bool]$value }
        if ($value -is [int] -or $value -is [long] -or $value -is [double]) { return ([double]$value -ne 0) }
        $normalized = ([string]$value).Trim().ToLowerInvariant()
        if (@("true","yes","on","enabled","active","1") -contains $normalized) { return $true }
        if (@("false","no","off","disabled","inactive","0") -contains $normalized) { return $false }
    }
    return $null
}

function Test-NumberEquals {
    param([object]$Actual, [double]$Expected)
    if ($null -eq $Actual -or [string]$Actual -eq "") { return $false }
    $number = 0.0
    if (-not [double]::TryParse([string]$Actual, [ref]$number)) { return $false }
    return ([Math]::Abs($number - $Expected) -le 0.001)
}

function Get-HttpErrorMessage {
    param([object]$ErrorRecord, [string]$Fallback)
    $message = $Fallback
    try {
        if ($ErrorRecord.Exception.Message) { $message = $ErrorRecord.Exception.Message }
        if ($ErrorRecord.ErrorDetails.Message) {
            try {
                $details = $ErrorRecord.ErrorDetails.Message | ConvertFrom-Json
                $detailMessage = Get-FirstPropertyValue -Object $details -Names @("message","error")
                if ($detailMessage) { $message = [string]$detailMessage }
            } catch {
                $message = [string]$ErrorRecord.ErrorDetails.Message
            }
        }
    } catch {
        $message = $Fallback
    }
    return $message
}

function Get-HttpStatusCode {
    param([object]$ErrorRecord)
    try {
        if ($ErrorRecord.Exception.Response.StatusCode) {
            return [int]$ErrorRecord.Exception.Response.StatusCode
        }
    } catch {
        return 0
    }
    return 0
}

function ConvertTo-QueryString {
    param([hashtable]$Query)
    if (-not $Query -or $Query.Count -eq 0) { return "" }
    $pairs = @()
    foreach ($key in $Query.Keys) {
        if ($null -eq $Query[$key] -or [string]$Query[$key] -eq "") { continue }
        $pairs += "$([uri]::EscapeDataString([string]$key))=$([uri]::EscapeDataString([string]$Query[$key]))"
    }
    return ($pairs -join "&")
}

function Invoke-InstantlyGet {
    param(
        [string]$ApiKey,
        [string]$Path,
        [hashtable]$Query = @{}
    )

    if ([string]::IsNullOrWhiteSpace($ApiKey)) {
        throw "Instantly API key is missing for a domain that is ready for deep proof. Re-run with -SkipInstantlyDeepCheck for DB-only progress."
    }

    $uri = "https://api.instantly.ai/api/v2/$Path"
    $queryString = ConvertTo-QueryString -Query $Query
    if ($queryString) { $uri = "$uri`?$queryString" }

    $lastError = ""
    for ($attempt = 1; $attempt -le $InstantlyApiAttempts; $attempt++) {
        try {
            return Invoke-RestMethod -Method GET -Uri $uri -Headers @{ Authorization = "Bearer $ApiKey" } -TimeoutSec 30
        } catch {
            $status = Get-HttpStatusCode -ErrorRecord $_
            $lastError = Get-HttpErrorMessage -ErrorRecord $_ -Fallback "Instantly API request failed"
            $retryable = ($status -eq 429 -or $status -ge 500)
            if ($retryable -and $attempt -lt $InstantlyApiAttempts) {
                Start-Sleep -Seconds ([Math]::Min(20, 2 * $attempt))
                continue
            }
            throw "Instantly API GET $Path failed (status=$status): $lastError"
        }
    }

    throw "Instantly API GET $Path failed: $lastError"
}

function Get-InstantlyRows {
    param([object]$Response)
    $items = Get-FirstPropertyValue -Object $Response -Names @("items")
    if ($items) { return @(ConvertTo-Array $items) }
    $data = Get-FirstPropertyValue -Object $Response -Names @("data")
    if ($data) { return @(ConvertTo-Array $data) }
    return @(ConvertTo-Array $Response)
}

function Get-InstantlyTagId {
    param([string]$ApiKey, [string]$Label)

    $normalized = $Label.Trim().ToLowerInvariant()
    $searches = @($Label, "")
    foreach ($search in $searches) {
        $startingAfter = ""
        for ($page = 1; $page -le 100; $page++) {
            $query = @{ limit = 100 }
            if ($search) { $query.search = $search }
            if ($startingAfter) { $query.starting_after = $startingAfter }
            $response = Invoke-InstantlyGet -ApiKey $ApiKey -Path "custom-tags" -Query $query
            foreach ($row in @(Get-InstantlyRows -Response $response)) {
                $name = [string](Get-FirstPropertyValue -Object $row -Names @("label","name","title"))
                $id = [string](Get-FirstPropertyValue -Object $row -Names @("id","tag_id"))
                if ($id -and $name.Trim().ToLowerInvariant() -eq $normalized) { return $id }
            }
            $startingAfter = [string](Get-FirstPropertyValue -Object $response -Names @("next_starting_after"))
            if (-not $startingAfter) { break }
        }
    }

    throw "Instantly tag not found: $Label. The proof script is read-only and will not create it."
}

function Get-InstantlyAccountsForDomain {
    param([string]$ApiKey, [string]$Domain)

    $accounts = @{}
    $startingAfter = ""
    for ($page = 1; $page -le $InstantlyPageLimit; $page++) {
        $query = @{ limit = 100; search = $Domain }
        if ($startingAfter) { $query.starting_after = $startingAfter }
        $response = Invoke-InstantlyGet -ApiKey $ApiKey -Path "accounts" -Query $query
        foreach ($row in @(Get-InstantlyRows -Response $response)) {
            $email = Normalize-Email ([string](Get-FirstPropertyValue -Object $row -Names @("email","account_email")))
            if ($email) { $accounts[$email] = $row }
        }
        $startingAfter = [string](Get-FirstPropertyValue -Object $response -Names @("next_starting_after"))
        if (-not $startingAfter) { break }
    }
    return $accounts
}

function Get-InstantlyAccountDirect {
    param([string]$ApiKey, [string]$Email)
    $response = Invoke-InstantlyGet -ApiKey $ApiKey -Path "accounts/$([uri]::EscapeDataString($Email))"
    $account = Get-FirstPropertyValue -Object $response -Names @("account")
    if ($account) { return $account }
    return $response
}

function Get-InstantlyTagMappedEmails {
    param(
        [string]$ApiKey,
        [string]$TagId,
        [string[]]$Emails
    )

    $mapped = [System.Collections.Generic.HashSet[string]]::new()
    $targets = @($Emails | ForEach-Object { Normalize-Email $_ } | Where-Object { $_ } | Sort-Object -Unique)
    if ($targets.Count -eq 0) { return $mapped }

    for ($i = 0; $i -lt $targets.Count; $i += 50) {
        $last = [Math]::Min($i + 49, $targets.Count - 1)
        $chunk = @($targets[$i..$last])
        $queries = @(
            @{ limit = 100; tag_ids = $TagId; resource_ids = ($chunk -join ",") },
            @{ limit = 100; tag_id = $TagId; resource_id = ($chunk -join ",") }
        )

        foreach ($query in $queries) {
            $startingAfter = ""
            for ($page = 1; $page -le 20; $page++) {
                if ($startingAfter) { $query.starting_after = $startingAfter } else { $query.Remove("starting_after") | Out-Null }
                $response = Invoke-InstantlyGet -ApiKey $ApiKey -Path "custom-tag-mappings" -Query $query
                foreach ($row in @(Get-InstantlyRows -Response $response)) {
                    $rowTagId = [string](Get-FirstPropertyValue -Object $row -Names @("tag_id","custom_tag_id"))
                    $email = Normalize-Email ([string](Get-FirstPropertyValue -Object $row -Names @("resource_id","email","account_email")))
                    if ($email -and $chunk -contains $email -and (-not $rowTagId -or $rowTagId -eq $TagId)) {
                        [void]$mapped.Add($email)
                    }
                }
                $startingAfter = [string](Get-FirstPropertyValue -Object $response -Names @("next_starting_after"))
                if (-not $startingAfter) { break }
            }
        }
    }

    return $mapped
}

function Test-InstantlyAccountProof {
    param([object]$Account)

    $warmup = Get-FirstPropertyValue -Object $Account -Names @("warmup")
    $advancedWarmup = Get-FirstPropertyValue -Object $warmup -Names @("advanced")
    $failures = [System.Collections.Generic.List[string]]::new()

    $dailyLimit = Get-FirstScalar @(
        (Get-FirstPropertyValue -Object $Account -Names @("daily_limit","dailyLimit"))
    )
    if (-not (Test-NumberEquals -Actual $dailyLimit -Expected $DailyLimit)) {
        $failures.Add("daily_limit expected $DailyLimit, got $([string]$dailyLimit)") | Out-Null
    }

    $sendingGap = Get-FirstScalar @(
        (Get-FirstPropertyValue -Object $Account -Names @("sending_gap","sendingGap"))
    )
    if (-not (Test-NumberEquals -Actual $sendingGap -Expected $SendingGapMinutes)) {
        $failures.Add("sending_gap expected $SendingGapMinutes, got $([string]$sendingGap)") | Out-Null
    }

    $warmupEnabled = Get-FirstBoolean @(
        (Get-FirstPropertyValue -Object $Account -Names @("warmup_enabled","is_warmup_enabled","warmup_status")),
        (Get-FirstPropertyValue -Object $warmup -Names @("enabled","warmup_enabled","status"))
    )
    if ($true -ne $warmupEnabled) {
        $failures.Add("warmup expected enabled, got $([string]$warmupEnabled)") | Out-Null
    }

    $warmupLimit = Get-FirstNumber @(
        (Get-FirstPropertyValue -Object $warmup -Names @("limit","warmup_daily_limit","daily_limit")),
        (Get-FirstPropertyValue -Object $Account -Names @("warmup_daily_limit","warmup_limit"))
    )
    if (-not (Test-NumberEquals -Actual $warmupLimit -Expected $WarmupDailyLimit)) {
        $failures.Add("warmup limit expected $WarmupDailyLimit, got $([string]$warmupLimit)") | Out-Null
    }

    $replyRate = Normalize-ReplyRatePercent (Get-FirstNumber @(
        (Get-FirstPropertyValue -Object $warmup -Names @("reply_rate")),
        (Get-FirstPropertyValue -Object $advancedWarmup -Names @("reply_rate"))
    ))
    if (-not (Test-NumberEquals -Actual $replyRate -Expected $WarmupReplyRatePercent)) {
        $failures.Add("warmup reply_rate expected $WarmupReplyRatePercent, got $([string]$replyRate)") | Out-Null
    }

    return [pscustomobject]@{
        ok = ($failures.Count -eq 0)
        failures = @($failures)
        daily_limit_ok = (Test-NumberEquals -Actual $dailyLimit -Expected $DailyLimit)
        sending_gap_ok = (Test-NumberEquals -Actual $sendingGap -Expected $SendingGapMinutes)
        warmup_enabled_ok = ($true -eq $warmupEnabled)
        warmup_limit_ok = (Test-NumberEquals -Actual $warmupLimit -Expected $WarmupDailyLimit)
        reply_rate_ok = (Test-NumberEquals -Actual $replyRate -Expected $WarmupReplyRatePercent)
    }
}

function Get-InstantlyCredentialByDomainId {
    param([string[]]$DomainIds)

    $rows = Get-RowsByDomainChunks `
        -Table "domain_credentials" `
        -DomainIds $DomainIds `
        -Select "domain_id,sending_tool_credentials(api_key,sending_tools(slug))" `
        -ChunkSize $DomainChunkSize

    $map = @{}
    foreach ($row in @($rows)) {
        $domainId = [string]$row.domain_id
        $credential = $row.sending_tool_credentials
        $slug = Normalize-DomainName ([string]$credential.sending_tools.slug)
        $apiKey = [string]$credential.api_key
        if ($domainId -and $apiKey -and (@("instantly","instantly.ai") -contains $slug)) {
            $map[$domainId] = $apiKey
        }
    }
    return $map
}

Assert-Configured
if (-not (Test-Path $PlanCsv)) { throw "Plan CSV not found: $PlanCsv" }

$generatedAt = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
if (-not $OutCsv) { $OutCsv = Join-Path $PSScriptRoot "logs/jack-threshold-final-proof-$stamp.csv" }
if (-not $OutJson) { $OutJson = Join-Path $PSScriptRoot "logs/jack-threshold-final-proof-$stamp.json" }

$planRaw = @(Import-Csv -Path $PlanCsv)
$plan = @(
    $planRaw |
        Where-Object { Normalize-DomainName ([string]$_.domain) } |
        Sort-Object @{ Expression = { Normalize-DomainName ([string]$_.domain) } } -Unique
)
$planDomains = @($plan | ForEach-Object { Normalize-DomainName ([string]$_.domain) })
$planDomainSet = @{}
foreach ($name in $planDomains) { $planDomainSet[$name] = $true }

Write-Info "Loading Simple Inboxes state for $($planDomains.Count) plan domains..."

$domainResult = Invoke-SupabaseApi -Method GET -Table "domains" -Query "select=id,domain,status,interim_status,provider,customer_id,workspace_id,updated_at&limit=5000"
$allDomains = @(Assert-ApiResult -Result $domainResult -Label "domains")
$domains = @($allDomains | Where-Object { $planDomainSet.ContainsKey((Normalize-DomainName ([string]$_.domain))) })

$domainByName = @{}
$domainById = @{}
foreach ($domain in $domains) {
    $name = Normalize-DomainName ([string]$domain.domain)
    $domainByName[$name] = $domain
    $domainById[[string]$domain.id] = $domain
}

$domainIds = @($domains | ForEach-Object { [string]$_.id })
$adminResult = Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query "provider=eq.microsoft&select=id,email,status,active,locked_by_action_id,locked_domain_id,lock_expires_at&limit=10000"
$admins = @(Assert-ApiResult -Result $adminResult -Label "admin_credentials")
$adminById = @{}
foreach ($admin in $admins) { $adminById[[string]$admin.id] = $admin }

$assignmentResult = Invoke-SupabaseApi -Method GET -Table "domain_admin_assignments" -Query "select=domain_id,admin_cred_id,assigned_at&order=assigned_at.desc&limit=50000"
$assignments = @(Assert-ApiResult -Result $assignmentResult -Label "domain_admin_assignments")
$latestAssignmentByDomainId = @{}
foreach ($assignment in $assignments) {
    $domainId = [string]$assignment.domain_id
    if ($domainId -and $domainById.ContainsKey($domainId) -and -not $latestAssignmentByDomainId.ContainsKey($domainId)) {
        $latestAssignmentByDomainId[$domainId] = $assignment
    }
}

$inboxes = @(Get-RowsByDomainChunks -Table "inboxes" -DomainIds $domainIds -Select "id,domain_id,email,username,status,created_at,updated_at" -ChunkSize $DomainChunkSize)
$actions = @(Get-RowsByDomainChunks -Table "actions" -DomainIds $domainIds -Select "id,domain_id,type,status,error,attempts,max_attempts,payload,result,created_at,updated_at,started_at,next_retry_at,completed_at" -ExtraQuery "order=updated_at.desc" -ChunkSize $DomainChunkSize)

$inboxesByDomainId = Group-ByProperty -Rows $inboxes -Property "domain_id"
$actionsByDomainId = Group-ByProperty -Rows $actions -Property "domain_id"

$rows = [System.Collections.Generic.List[object]]::new()

foreach ($planRow in $plan) {
    $domainName = Normalize-DomainName ([string]$planRow.domain)
    $destinationAdmin = Normalize-Email ([string]$planRow.proposed_destination_admin)
    $sourceAdmin = Normalize-Email ([string]$planRow.source_admin)
    $domain = if ($domainByName.ContainsKey($domainName)) { $domainByName[$domainName] } else { $null }

    $domainId = if ($domain) { [string]$domain.id } else { "" }
    $currentAdmin = $null
    if ($domainId -and $latestAssignmentByDomainId.ContainsKey($domainId)) {
        $assignment = $latestAssignmentByDomainId[$domainId]
        if ($adminById.ContainsKey([string]$assignment.admin_cred_id)) {
            $currentAdmin = $adminById[[string]$assignment.admin_cred_id]
        }
    }

    $currentAdminEmail = Normalize-Email ([string]$currentAdmin.email)
    $movedToDestination = ($currentAdminEmail -and $destinationAdmin -and $currentAdminEmail -eq $destinationAdmin)

    $domainInboxes = if ($domainId -and $inboxesByDomainId.ContainsKey($domainId)) { @($inboxesByDomainId[$domainId]) } else { @() }
    $activeInboxes = @($domainInboxes | Where-Object { [string]$_.status -eq "active" })
    $activeEmails = @(
        $activeInboxes |
            ForEach-Object {
                $email = Normalize-Email ([string]$_.email)
                if (-not $email -and $_.username) { $email = "$(Normalize-Email ([string]$_.username))@$domainName" }
                $email
            } |
            Where-Object { $_ } |
            Sort-Object -Unique
    )

    $domainActions = if ($domainId -and $actionsByDomainId.ContainsKey($domainId)) { @($actionsByDomainId[$domainId]) } else { @() }
    $replacementProvisionActions = @($domainActions | Where-Object {
        [string]$_.type -eq "provision_inbox" -and $_.payload -and [string]$_.payload.source -eq "jack_threshold_tenant_migration"
    })
    $completedProvisionActions = @($replacementProvisionActions | Where-Object { [string]$_.status -eq "completed" } | Sort-Object updated_at -Descending)
    $completedProvisionIds = @($completedProvisionActions | ForEach-Object { [string]$_.id })
    $linkedUploadActions = @($domainActions | Where-Object {
        [string]$_.type -eq "reupload_inboxes" -and
        $_.payload -and
        [string]$_.payload.provision_action_id -and
        ($completedProvisionIds -contains [string]$_.payload.provision_action_id)
    })
    $completedLinkedUploadActions = @($linkedUploadActions | Where-Object { [string]$_.status -eq "completed" } | Sort-Object updated_at -Descending)

    $failureReasons = [System.Collections.Generic.List[string]]::new()
    if (-not $domain) { $failureReasons.Add("domain not found in Simple Inboxes") | Out-Null }
    if ($domain -and [string]$domain.status -ne "active") { $failureReasons.Add("domain status is $($domain.status)") | Out-Null }
    if (-not $movedToDestination) { $failureReasons.Add("current admin does not match proposed destination") | Out-Null }
    if ($activeEmails.Count -ne $ExpectedInboxes) { $failureReasons.Add("active inbox count expected $ExpectedInboxes, got $($activeEmails.Count)") | Out-Null }
    if ($completedProvisionActions.Count -eq 0) { $failureReasons.Add("replacement provision_inbox action is not completed") | Out-Null }
    if ($completedLinkedUploadActions.Count -eq 0) { $failureReasons.Add("linked replacement reupload_inboxes action is not completed") | Out-Null }

    $dbProven = (
        $domain -and
        [string]$domain.status -eq "active" -and
        $movedToDestination -and
        $activeEmails.Count -eq $ExpectedInboxes -and
        $completedProvisionActions.Count -gt 0 -and
        $completedLinkedUploadActions.Count -gt 0
    )

    $rows.Add([pscustomobject][ordered]@{
        domain = $domainName
        domain_id = $domainId
        db_domain_found = [bool]$domain
        domain_status = if ($domain) { [string]$domain.status } else { "" }
        interim_status = if ($domain) { [string]$domain.interim_status } else { "" }
        source_admin = $sourceAdmin
        destination_admin = $destinationAdmin
        current_admin = $currentAdminEmail
        current_admin_status = if ($currentAdmin) { [string]$currentAdmin.status } else { "" }
        moved_to_destination = [bool]$movedToDestination
        active_inboxes = [int]$activeEmails.Count
        active_inboxes_exact = [bool]($activeEmails.Count -eq $ExpectedInboxes)
        total_inboxes = [int]$domainInboxes.Count
        replacement_provision_completed = [bool]($completedProvisionActions.Count -gt 0)
        replacement_provision_action_id = if ($completedProvisionActions.Count -gt 0) { [string]$completedProvisionActions[0].id } else { "" }
        replacement_provision_statuses = (@($replacementProvisionActions | Group-Object status | ForEach-Object { "$($_.Name):$($_.Count)" }) -join "; ")
        linked_reupload_completed = [bool]($completedLinkedUploadActions.Count -gt 0)
        linked_reupload_action_id = if ($completedLinkedUploadActions.Count -gt 0) { [string]$completedLinkedUploadActions[0].id } else { "" }
        linked_reupload_statuses = (@($linkedUploadActions | Group-Object status | ForEach-Object { "$($_.Name):$($_.Count)" }) -join "; ")
        db_proven = [bool]$dbProven
        instantly_check_status = if ($SkipInstantlyDeepCheck) { "skipped" } elseif ($dbProven) { "pending" } else { "not_checked_db_incomplete" }
        instantly_accounts_found = 0
        instantly_settings_ok = 0
        instantly_warmup_enabled_ok = 0
        instantly_warmup_limit_ok = 0
        instantly_reply_rate_ok = 0
        instantly_tagged = 0
        instantly_missing_accounts = 0
        instantly_bad_settings = 0
        instantly_missing_tag_mappings = 0
        instantly_failure_samples = ""
        instantly_proven = $false
        final_proven = $false
        failure_reasons = (@($failureReasons) -join "; ")
        expected_daily_limit = $DailyLimit
        expected_sending_gap_minutes = $SendingGapMinutes
        expected_warmup_daily_limit = $WarmupDailyLimit
        expected_warmup_reply_rate_percent = $WarmupReplyRatePercent
        expected_tag = $Tag
    }) | Out-Null
}

if (-not $SkipInstantlyDeepCheck) {
    $deepRows = @($rows | Where-Object { $_.db_proven })
    if ($deepRows.Count -gt 0) {
        Write-Info "Running read-only Instantly deep proof for $($deepRows.Count) DB-proven domains..."
        $domainIdsForDeepCheck = @($deepRows | Select-Object -ExpandProperty domain_id)
        $apiKeysByDomainId = Get-InstantlyCredentialByDomainId -DomainIds $domainIdsForDeepCheck
        $missingCredentialRows = @($deepRows | Where-Object { -not $apiKeysByDomainId.ContainsKey([string]$_.domain_id) })
        if ($missingCredentialRows.Count -gt 0) {
            $samples = @($missingCredentialRows | Select-Object -First 10 -ExpandProperty domain)
            throw "Cannot run Instantly deep check: $($missingCredentialRows.Count) DB-proven plan domain(s) are missing an Instantly API key in domain_credentials. Samples: $($samples -join ', '). Re-run with -SkipInstantlyDeepCheck for DB-only progress."
        }

        $tagIdByApiKey = @{}
        foreach ($row in $deepRows) {
            $apiKey = $apiKeysByDomainId[[string]$row.domain_id]
            if (-not $tagIdByApiKey.ContainsKey($apiKey)) {
                $tagIdByApiKey[$apiKey] = Get-InstantlyTagId -ApiKey $apiKey -Label $Tag
            }
        }

        for ($rowIndex = 0; $rowIndex -lt $rows.Count; $rowIndex++) {
            $row = $rows[$rowIndex]
            if (-not $row.db_proven) { continue }

            $domainId = [string]$row.domain_id
            $apiKey = $apiKeysByDomainId[$domainId]
            $tagId = $tagIdByApiKey[$apiKey]
            $activeEmails = @(
                @($inboxesByDomainId[$domainId]) |
                    Where-Object { [string]$_.status -eq "active" } |
                    ForEach-Object {
                        $email = Normalize-Email ([string]$_.email)
                        if (-not $email -and $_.username) { $email = "$(Normalize-Email ([string]$_.username))@$($row.domain)" }
                        $email
                    } |
                    Where-Object { $_ } |
                    Sort-Object -Unique
            )

            try {
                $accounts = Get-InstantlyAccountsForDomain -ApiKey $apiKey -Domain ([string]$row.domain)
                $mappedEmails = Get-InstantlyTagMappedEmails -ApiKey $apiKey -TagId $tagId -Emails $activeEmails

                $found = 0
                $settingsOk = 0
                $warmupEnabledOk = 0
                $warmupLimitOk = 0
                $replyRateOk = 0
                $tagged = 0
                $missingAccounts = [System.Collections.Generic.List[string]]::new()
                $badSettings = [System.Collections.Generic.List[string]]::new()
                $missingTags = [System.Collections.Generic.List[string]]::new()

                foreach ($email in $activeEmails) {
                    if (-not $accounts.ContainsKey($email)) {
                        $missingAccounts.Add($email) | Out-Null
                        continue
                    }

                    $found++
                    $account = $accounts[$email]
                    $proof = Test-InstantlyAccountProof -Account $account
                    if (-not $proof.ok) {
                        try {
                            $directAccount = Get-InstantlyAccountDirect -ApiKey $apiKey -Email $email
                            $proof = Test-InstantlyAccountProof -Account $directAccount
                        } catch {
                            $badSettings.Add("$email :: $(Get-HttpErrorMessage -ErrorRecord $_ -Fallback $_.Exception.Message)") | Out-Null
                            continue
                        }
                    }

                    if ($proof.ok) { $settingsOk++ } else { $badSettings.Add("$email :: $(@($proof.failures) -join ', ')") | Out-Null }
                    if ($proof.warmup_enabled_ok) { $warmupEnabledOk++ }
                    if ($proof.warmup_limit_ok) { $warmupLimitOk++ }
                    if ($proof.reply_rate_ok) { $replyRateOk++ }
                    if ($mappedEmails.Contains($email)) { $tagged++ } else { $missingTags.Add($email) | Out-Null }
                }

                $failureSamples = @(
                    @($missingAccounts | Select-Object -First 5 | ForEach-Object { "missing_account=$_" }) +
                    @($badSettings | Select-Object -First 5 | ForEach-Object { "bad_settings=$_" }) +
                    @($missingTags | Select-Object -First 5 | ForEach-Object { "missing_tag=$_" })
                )
                $instantlyProven = (
                    $found -eq $ExpectedInboxes -and
                    $settingsOk -eq $ExpectedInboxes -and
                    $warmupEnabledOk -eq $ExpectedInboxes -and
                    $warmupLimitOk -eq $ExpectedInboxes -and
                    $replyRateOk -eq $ExpectedInboxes -and
                    $tagged -eq $ExpectedInboxes
                )

                $row.instantly_check_status = if ($instantlyProven) { "passed" } else { "failed" }
                $row.instantly_accounts_found = [int]$found
                $row.instantly_settings_ok = [int]$settingsOk
                $row.instantly_warmup_enabled_ok = [int]$warmupEnabledOk
                $row.instantly_warmup_limit_ok = [int]$warmupLimitOk
                $row.instantly_reply_rate_ok = [int]$replyRateOk
                $row.instantly_tagged = [int]$tagged
                $row.instantly_missing_accounts = [int]$missingAccounts.Count
                $row.instantly_bad_settings = [int]$badSettings.Count
                $row.instantly_missing_tag_mappings = [int]$missingTags.Count
                $row.instantly_failure_samples = ($failureSamples -join "; ")
                $row.instantly_proven = [bool]$instantlyProven
                $row.final_proven = [bool]($row.db_proven -and $instantlyProven)

                if (-not $instantlyProven) {
                    $existingReasons = @($row.failure_reasons -split "; " | Where-Object { $_ })
                    $row.failure_reasons = (@($existingReasons + @("Instantly deep proof failed")) -join "; ")
                }
            } catch {
                $message = Get-HttpErrorMessage -ErrorRecord $_ -Fallback $_.Exception.Message
                $row.instantly_check_status = "error"
                $row.instantly_failure_samples = $message
                $row.instantly_proven = $false
                $row.final_proven = $false
                $existingReasons = @($row.failure_reasons -split "; " | Where-Object { $_ })
                $row.failure_reasons = (@($existingReasons + @("Instantly deep proof error")) -join "; ")
            }
        }
    }
} else {
    Write-Info "Skipping Instantly deep proof; producing DB-only progress report."
}

$summary = [pscustomobject][ordered]@{
    generated_at_utc = $generatedAt
    plan_csv = $PlanCsv
    plan_domains = $planDomains.Count
    db_domains_found = @($rows | Where-Object { $_.db_domain_found }).Count
    domains_on_destination_admin = @($rows | Where-Object { $_.moved_to_destination }).Count
    domains_with_exact_active_inboxes = @($rows | Where-Object { $_.active_inboxes_exact }).Count
    replacement_provision_completed = @($rows | Where-Object { $_.replacement_provision_completed }).Count
    linked_reupload_completed = @($rows | Where-Object { $_.linked_reupload_completed }).Count
    db_proven_domains = @($rows | Where-Object { $_.db_proven }).Count
    instantly_deep_check_skipped = [bool]$SkipInstantlyDeepCheck
    instantly_checked_domains = if ($SkipInstantlyDeepCheck) { 0 } else { @($rows | Where-Object { $_.instantly_check_status -in @("passed","failed","error") }).Count }
    instantly_proven_domains = @($rows | Where-Object { $_.instantly_proven }).Count
    final_proven_domains = @($rows | Where-Object { $_.final_proven }).Count
    expected_inboxes_per_domain = $ExpectedInboxes
    expected_total_inboxes = $planDomains.Count * $ExpectedInboxes
    observed_active_inboxes = @($rows | Measure-Object -Property active_inboxes -Sum).Sum
    expected_daily_limit = $DailyLimit
    expected_sending_gap_minutes = $SendingGapMinutes
    expected_warmup_daily_limit = $WarmupDailyLimit
    expected_warmup_reply_rate_percent = $WarmupReplyRatePercent
    expected_tag = $Tag
    not_final_samples = @(
        $rows |
            Where-Object { -not $_.final_proven } |
            Select-Object -First 25 domain,db_proven,instantly_check_status,active_inboxes,replacement_provision_completed,linked_reupload_completed,failure_reasons,instantly_failure_samples
    )
}

$report = [pscustomobject][ordered]@{
    summary = $summary
    domains = @($rows)
    endpoint_strategy = [pscustomobject][ordered]@{
        database = "Supabase REST: domains, domain_admin_assignments, inboxes, actions, domain_credentials; actions are scoped to payload.source=jack_threshold_tenant_migration and linked payload.provision_action_id."
        instantly = if ($SkipInstantlyDeepCheck) {
            "Skipped by -SkipInstantlyDeepCheck."
        } else {
            "Instantly API v2: custom-tags lookup, accounts domain search, direct account fallback, and custom-tag-mappings by tag_ids/resource_ids. Read-only; no tags or settings are created or changed."
        }
        reply_rate_contract = "Warmup reply_rate is verified as a human percent value: $WarmupReplyRatePercent means $WarmupReplyRatePercent percent, not 0.$WarmupReplyRatePercent percent."
    }
}

@($rows) | Export-Csv -Path $OutCsv -NoTypeInformation
$report | ConvertTo-Json -Depth 12 | Set-Content -Path $OutJson

if ($Json) {
    $report | ConvertTo-Json -Depth 12
} else {
    $summary
    Write-Host ""
    Write-Host "Wrote CSV:  $OutCsv"
    Write-Host "Wrote JSON: $OutJson"
}
