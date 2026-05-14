<#
.SYNOPSIS
    Repairs Jack replacement uploads blocked by Instantly cross-workspace account conflicts.
.DESCRIPTION
    Instantly can reject OAuth for an address with "Account already exists in
    another workspace" even when our known workspace API keys cannot read it.
    For this one-off migration the safe recovery is to rename only the
    conflicted Microsoft mailbox to a fresh persona on the same destination
    tenant, then retry the linked replacement upload action.
#>

param(
    [string]$PlanCsv = (Join-Path $PSScriptRoot "logs/jack-threshold-domain-migration-plan-20260512T033321Z.csv"),
    [int]$ExpectedInboxes = 99,
    [datetimeoffset]$RecycleInProgressUpdatedBefore = ([datetimeoffset]::UtcNow.AddMinutes(-20)),
    [switch]$Live
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

function New-ConflictRepairAction {
    param(
        [object]$Domain,
        [object[]]$Updates,
        [string[]]$ConflictEmails
    )

    $payload = @{
        source = "jack_instantly_conflict_repair"
        reason = "Instantly OAuth reported one or more accounts already exist in another workspace"
        conflict_emails = $ConflictEmails
        updates = $Updates
    }
    $body = @{
        customer_id = $Domain.customer_id
        domain_id = $Domain.id
        type = "microsoft_update_inboxes"
        status = "pending"
        attempts = 0
        max_attempts = 5
        payload = $payload
    }
    $result = Invoke-SupabaseApi -Method POST -Table "actions" -Body $body
    if (-not $result.Success -or -not $result.Data -or $result.Data.Count -eq 0) {
        throw "Failed to enqueue microsoft_update_inboxes repair for $($Domain.domain): $($result.Error)"
    }
    return $result.Data[0]
}

function Reset-ActionForConflictRetry {
    param(
        [object]$Action,
        [string]$Reason
    )

    $body = @{
        status = "pending"
        error = $Reason
        attempts = 0
        started_at = $null
        completed_at = $null
        next_retry_at = $null
        updated_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    }
    return Invoke-SupabaseApi -Method PATCH -Table "actions" -Query "id=eq.$($Action.id)" -Body $body
}

function Extract-ConflictEmails {
    param([string]$ErrorText)

    $matches = [regex]::Matches(
        [string]$ErrorText,
        "([a-z0-9._%+\-]+@[a-z0-9.\-]+):[^;]*(?:another workspace|exists in another workspace)",
        [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
    )
    $emails = [System.Collections.Generic.HashSet[string]]::new()
    foreach ($match in $matches) {
        [void]$emails.Add((Normalize-Email ([string]$match.Groups[1].Value)))
    }
    return @($emails)
}

function Get-FreshPersona {
    param(
        [hashtable]$UsedLocalParts,
        [string]$DomainName
    )

    $firstNames = @(
        "madeline","aubrey","kaitlyn","julia","sophie","claire","paige","brooke","hailey","jasmine",
        "taylor","rachel","lindsey","caroline","sydney","morgan","allison","kelsey","brianna","kayla",
        "sabrina","jocelyn","rebecca","vanessa","natalie","olivia","hannah","molly","kristen","chelsea",
        "lauren","ashley","emily","megan","sarah","jessica","nicole","stephanie","brittany","mackenzie",
        "amanda","melissa","amber","amy","catherine","riley","kimberly","holly","abigail","katie",
        "kelly","erin","jenna","michelle","courtney","victoria","samantha","elizabeth","gabrielle","isabelle",
        "jillian","marissa","audrey","leah","meredith","whitney","alexa","cassandra","danielle","christina",
        "valerie","laura","heather","tracy","erin","marina","tessa","bianca","alyssa","camille",
        "savannah","kendra","cassidy","veronica","melanie","adrienne","lillian","brenna","alicia","kara"
    )
    $lastNames = @(
        "Reed","Parker","Collins","Brooks","Hayes","Bennett","Foster","Morgan","Miller","Clark",
        "Turner","Phillips","Anderson","Roberts","Richardson","Watson","Hughes","Carter","Adams","Cooper"
    )

    for ($i = 0; $i -lt $firstNames.Count; $i++) {
        $local = $firstNames[$i].ToLowerInvariant()
        if ($UsedLocalParts.ContainsKey($local)) { continue }
        $UsedLocalParts[$local] = $true
        return @{
            username = $local
            email = "$local@$DomainName"
            first_name = (Get-Culture).TextInfo.ToTitleCase($local)
            last_name = $lastNames[$i % $lastNames.Count]
        }
    }

    for ($i = 0; $i -lt 200; $i++) {
        $local = "julia$((Get-Random -Minimum 1000 -Maximum 9999))"
        if ($UsedLocalParts.ContainsKey($local)) { continue }
        $UsedLocalParts[$local] = $true
        return @{
            username = $local
            email = "$local@$DomainName"
            first_name = "Julia"
            last_name = $lastNames[$i % $lastNames.Count]
        }
    }

    throw "Could not generate an unused replacement persona for $DomainName"
}

if (-not (Test-Path $PlanCsv)) { throw "Plan CSV not found: $PlanCsv" }

$planDomains = @{}
foreach ($row in @(Import-Csv -Path $PlanCsv)) {
    $name = Normalize-DomainName ([string]$row.domain)
    if ($name) { $planDomains[$name] = $true }
}

$domainRows = @(Invoke-SupabaseApi -Method GET -Table "domains" -Query "select=id,domain,customer_id,status,interim_status&limit=5000").Data | Where-Object {
    $planDomains.ContainsKey((Normalize-DomainName ([string]$_.domain)))
}
$domainById = @{}
$domainIds = @()
foreach ($domain in $domainRows) {
    $domainById[[string]$domain.id] = $domain
    $domainIds += [string]$domain.id
}

$actions = @()
for ($i = 0; $i -lt $domainIds.Count; $i += 50) {
    $end = [Math]::Min($i + 49, $domainIds.Count - 1)
    $chunk = @($domainIds[$i..$end])
    $query = "domain_id=in.($($chunk -join ','))&type=in.(provision_inbox,reupload_inboxes,microsoft_update_inboxes)&select=id,domain_id,type,status,error,attempts,payload,updated_at,started_at&limit=20000"
    $result = Invoke-SupabaseApi -Method GET -Table "actions" -Query $query
    if (-not $result.Success) { throw $result.Error }
    $actions += @($result.Data)
}

$replacementProvisionById = @{}
foreach ($action in $actions) {
    if (
        [string]$action.type -eq "provision_inbox" -and
        $action.payload -and
        [string]$action.payload.source -eq "jack_threshold_tenant_migration"
    ) {
        $replacementProvisionById[[string]$action.id] = $action
    }
}

$actionsByDomain = @{}
foreach ($action in $actions) {
    $domainId = [string]$action.domain_id
    if (-not $actionsByDomain.ContainsKey($domainId)) {
        $actionsByDomain[$domainId] = [System.Collections.Generic.List[object]]::new()
    }
    $actionsByDomain[$domainId].Add($action) | Out-Null
}

$targetUploads = @($actions | Where-Object {
    if ([string]$_.type -ne "reupload_inboxes") { return $false }
    if (-not $_.payload -or -not $replacementProvisionById.ContainsKey([string]$_.payload.provision_action_id)) { return $false }
    $errorText = [string]$_.error
    if ([string]$_.status -eq "failed") { return ($errorText -match "another workspace|exists in another workspace") }
    if ([string]$_.status -ne "in_progress") { return $false }
    if (-not $_.updated_at) { return $true }
    return ([datetimeoffset]::Parse([string]$_.updated_at) -lt $RecycleInProgressUpdatedBefore -and $errorText -match "another workspace|exists in another workspace")
})

$mutationCreated = 0
$uploadReset = 0
$provisionReset = 0

foreach ($uploadAction in $targetUploads) {
    $domainId = [string]$uploadAction.domain_id
    if (-not $domainById.ContainsKey($domainId)) { continue }
    $domain = $domainById[$domainId]
    $domainName = Normalize-DomainName ([string]$domain.domain)
    $domainActions = @($actionsByDomain[$domainId])
    $conflictEmails = @(Extract-ConflictEmails -ErrorText ([string]$uploadAction.error) | Where-Object { $_.EndsWith("@$domainName") } | Sort-Object -Unique)
    if ($conflictEmails.Count -eq 0) { continue }

    $openMutation = @($domainActions | Where-Object {
        [string]$_.type -eq "microsoft_update_inboxes" -and
        $_.payload -and
        [string]$_.payload.source -eq "jack_instantly_conflict_repair" -and
        [string]$_.status -in @("pending","in_progress")
    } | Select-Object -First 1)

    $completedMutation = @($domainActions | Where-Object {
        [string]$_.type -eq "microsoft_update_inboxes" -and
        $_.payload -and
        [string]$_.payload.source -eq "jack_instantly_conflict_repair" -and
        [string]$_.status -eq "completed"
    } | Sort-Object updated_at -Descending | Select-Object -First 1)

    if ($completedMutation.Count -gt 0) {
        Write-Host "reset_upload $($uploadAction.id) $domainName after completed conflict repair $($completedMutation[0].id)"
        if ($Live) {
            $result = Reset-ActionForConflictRetry -Action $uploadAction -Reason "Requeued after Microsoft mailbox conflict repair for Instantly cross-workspace account"
            if (-not $result.Success) { Write-Warning "Failed to reset upload $($uploadAction.id): $($result.Error)" } else { $uploadReset += 1 }

            $provision = $replacementProvisionById[[string]$uploadAction.payload.provision_action_id]
            if ($provision -and [string]$provision.status -eq "failed" -and [string]$provision.error -match "another workspace|exists in another workspace") {
                $provisionResult = Reset-ActionForConflictRetry -Action $provision -Reason "Requeued finalization after Instantly cross-workspace mailbox repair"
                if (-not $provisionResult.Success) { Write-Warning "Failed to reset provision $($provision.id): $($provisionResult.Error)" } else { $provisionReset += 1 }
            }

            Update-Domain -DomainId $domainId -Fields @{
                status = "in_progress"
                interim_status = "Both - Sending Tool Upload Pending"
            }
        }
        continue
    }

    if ($openMutation.Count -gt 0) {
        Write-Host "open_mutation $($openMutation[0].id) $domainName conflicts=$($conflictEmails -join ',')"
        continue
    }

    $admin = Get-AssignedAdmin -DomainId $domainId
    if (-not $admin -or [string]$admin.status -ne "Active") {
        Write-Warning "Skipping $domainName conflict repair because assigned admin is not Active"
        continue
    }

    $activeInboxes = @(Get-DomainInboxes -DomainId $domainId -Status "active")
    if ($activeInboxes.Count -ne $ExpectedInboxes) {
        Write-Warning "Skipping $domainName conflict repair because active inbox count is $($activeInboxes.Count), expected $ExpectedInboxes"
        continue
    }

    $inboxByEmail = @{}
    $usedLocal = @{}
    foreach ($inbox in $activeInboxes) {
        $email = Normalize-Email ([string]$inbox.email)
        if (-not $email) { $email = Normalize-Email ("$($inbox.username)@$domainName") }
        if ($email) {
            $inboxByEmail[$email] = $inbox
            $usedLocal[($email -split "@")[0]] = $true
        }
    }

    $updates = @()
    foreach ($conflictEmail in $conflictEmails) {
        if (-not $inboxByEmail.ContainsKey($conflictEmail)) {
            Write-Warning "Skipping $conflictEmail because no active inbox row exists"
            continue
        }
        $replacement = Get-FreshPersona -UsedLocalParts $usedLocal -DomainName $domainName
        $oldInbox = $inboxByEmail[$conflictEmail]
        $updates += @{
            inbox_id = [string]$oldInbox.id
            old_email = $conflictEmail
            new_email = [string]$replacement.email
            username = [string]$replacement.username
            new_username = [string]$replacement.username
            first_name = [string]$replacement.first_name
            new_first_name = [string]$replacement.first_name
            last_name = [string]$replacement.last_name
            new_last_name = [string]$replacement.last_name
            keep_old_email_as_alias = $false
        }
    }

    if ($updates.Count -eq 0) { continue }
    Write-Host "enqueue_mutation $domainName conflicts=$($conflictEmails -join ',') replacements=$(@($updates | ForEach-Object { $_.new_email }) -join ',')"
    if ($Live) {
        $mutation = New-ConflictRepairAction -Domain $domain -Updates $updates -ConflictEmails $conflictEmails
        Add-ActionLog -ActionId ([string]$mutation.id) -DomainId $domainId -CustomerId ([string]$domain.customer_id) -EventType "instantly_cross_workspace_conflict_repair_queued" -Severity "warn" -Message "Queued mailbox rename for Instantly cross-workspace account conflict" -Metadata @{
            conflict_emails = $conflictEmails
            replacement_emails = @($updates | ForEach-Object { $_.new_email })
            upload_action_id = [string]$uploadAction.id
            admin_email = [string]$admin.email
        }
        $mutationCreated += 1
    }
}

Write-Host "Matched $($targetUploads.Count) cross-workspace upload blocker(s). MutationsCreated=$mutationCreated UploadsReset=$uploadReset ProvisionsReset=$provisionReset Live=$([bool]$Live)"
