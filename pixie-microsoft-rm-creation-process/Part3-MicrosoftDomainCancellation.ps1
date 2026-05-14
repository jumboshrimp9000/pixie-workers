<#
.SYNOPSIS
    Part 3: Microsoft domain cancellation teardown.
.DESCRIPTION
    Processes `microsoft_cancel_domain` actions from Supabase and performs
    real tenant teardown for a Microsoft domain:
      1. Resolve admin + connect Exchange Online
      2. Delete domain mailboxes / recipients
      3. Remove any remaining Graph users on that domain
      4. Remove accepted domain from Exchange
      5. Finalize Supabase status (domain cancelled, inboxes deleted)

    Script is idempotent and safe to retry. Missing objects are treated as
    already deleted.

.PARAMETER DomainId
    Supabase domain UUID
.PARAMETER ActionId
    Supabase action UUID
.PARAMETER DryRun
    Run without making destructive tenant changes
#>

param(
    [Parameter(Mandatory=$true)][string]$DomainId,
    [Parameter(Mandatory=$true)][string]$ActionId,
    [switch]$DryRun
)

. (Join-Path $PSScriptRoot "config.ps1")

$AzureCliPublicClientId = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
$script:DomainName = ""
$script:CustomerId = $null
$script:ActionRecord = $null
$script:Summary = [ordered]@{
    inboxRows = 0
    mailboxDeleted = 0
    mailboxAlreadyMissing = 0
    recipientCleanupDeleted = 0
    recipientAliasCleanupRemoved = 0
    recipientProtectedSkipped = 0
    graphUsersDeleted = 0
    acceptedDomainRemoved = $false
}
$script:Steps = @()

function Get-TenantIdFromDomain {
    param([string]$Domain)
    try {
        $response = Invoke-RestMethod -Uri "https://login.microsoftonline.com/$Domain/v2.0/.well-known/openid-configuration" -TimeoutSec 30 -ErrorAction Stop
        if ($response.token_endpoint) {
            $parts = $response.token_endpoint -split '/'
            if ($parts.Length -ge 4 -and $parts[3] -and $parts[3].ToLower() -ne "organizations") { return $parts[3] }
        }
    } catch { }
    return $null
}

function Get-ROPCToken {
    param(
        [string]$TenantId,
        [string]$ClientId,
        [string]$Username,
        [string]$Password,
        [string]$ScopeString = "https://graph.microsoft.com/.default"
    )

    $tokenUrl = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
    $body = @{
        grant_type = "password"
        client_id = $ClientId
        scope = $ScopeString
        username = $Username
        password = $Password
    }

    try {
        $response = Invoke-RestMethod -Method POST -Uri $tokenUrl -Body $body -ContentType "application/x-www-form-urlencoded" -TimeoutSec 30 -ErrorAction Stop
        return $response.access_token
    } catch {
        Write-Log "ROPC token failed: $($_.Exception.Message)" -Level Error
        return $null
    }
}

function Invoke-GraphRequest {
    param(
        [string]$Method,
        [string]$Url,
        [string]$Bearer,
        [object]$Body = $null
    )

    $headers = @{ Authorization = "Bearer $Bearer" }
    $params = @{
        Method = $Method
        Uri = $Url
        Headers = $headers
        ContentType = "application/json"
        TimeoutSec = 60
        ErrorAction = "Stop"
    }
    if ($Body) {
        $params.Body = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 15 }
    }

    return Invoke-RestMethod @params
}

function Normalize-DomainValue {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant().Replace("https://", "").Replace("http://", "").TrimEnd("/")
}

function Get-ObjectPropertyValue {
    param([object]$Object, [string]$Name)
    if (-not $Object -or -not $Name) { return $null }
    if ($Object -is [string]) {
        try { $Object = $Object | ConvertFrom-Json -Depth 20 } catch { return $null }
    }
    $property = $Object.PSObject.Properties[$Name]
    if ($property) { return $property.Value }
    return $null
}

function Get-ActionPayloadValue {
    param([object]$ActionRecord, [string]$Name)
    return Get-ObjectPropertyValue -Object (Get-ObjectPropertyValue -Object $ActionRecord -Name "payload") -Name $Name
}

function Get-ReplacementProtectedDomain {
    $payloadValue = Get-ActionPayloadValue -ActionRecord $script:ActionRecord -Name "replacement_new_domain"
    if ($payloadValue) { return Normalize-DomainValue ([string]$payloadValue) }

    $settings = Get-ObjectPropertyValue -Object $script:DomainRecord -Name "fulfillment_settings"
    $replacement = Get-ObjectPropertyValue -Object $settings -Name "replacement"
    foreach ($key in @("new_domain", "newDomain", "replacement_new_domain")) {
        $value = Get-ObjectPropertyValue -Object $replacement -Name $key
        if ($value) { return Normalize-DomainValue ([string]$value) }
    }
    return ""
}

function Get-EmailAddressDomain {
    param([string]$Address)
    $value = ([string]$Address).Trim()
    if ($value -match '^[^:]+:(.+)$') { $value = $matches[1] }
    $value = $value.Trim().Trim('"').ToLowerInvariant()
    $at = $value.LastIndexOf("@")
    if ($at -lt 0 -or $at -eq ($value.Length - 1)) { return "" }
    return $value.Substring($at + 1)
}

function Get-RecipientAddressesForDomain {
    param([object]$Recipient, [string]$Domain)
    $domainName = Normalize-DomainValue $Domain
    if (-not $Recipient -or -not $domainName) { return @() }

    $matches = @()
    foreach ($address in @($Recipient.EmailAddresses)) {
        if (-not $address) { continue }
        $raw = [string]$address
        if ((Get-EmailAddressDomain -Address $raw) -eq $domainName) {
            $matches += $raw
        }
    }
    return @($matches | Select-Object -Unique)
}

function Test-RecipientHasDomain {
    param([object]$Recipient, [string]$Domain)
    $domainName = Normalize-DomainValue $Domain
    if (-not $Recipient -or -not $domainName) { return $false }

    $primary = [string]$Recipient.PrimarySmtpAddress
    if ($primary -and (Get-EmailAddressDomain -Address $primary) -eq $domainName) { return $true }
    return (@(Get-RecipientAddressesForDomain -Recipient $Recipient -Domain $domainName).Count -gt 0)
}

function New-Step {
    param([string]$Name)
    $step = [ordered]@{
        step = $Name
        status = "in_progress"
        startedAt = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
        attempt = if ($script:ActionRecord -and $script:ActionRecord.attempts -ne $null) { [int]$script:ActionRecord.attempts } else { 1 }
    }
    $script:Steps += ,$step
    Persist-Progress | Out-Null
    return ,$step
}

function Complete-Step {
    param(
        [object]$Step,
        [hashtable]$Details = $null
    )
    $Step["status"] = "completed"
    $Step["completedAt"] = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    if ($Details) { $Step["details"] = $Details }
    Persist-Progress | Out-Null
}

function Fail-Step {
    param(
        [object]$Step,
        [string]$ErrorMessage
    )
    $Step["status"] = "failed"
    $Step["completedAt"] = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    $Step["error"] = $ErrorMessage
    Persist-Progress | Out-Null
}

function Persist-Progress {
    $result = [ordered]@{
        checkpoint_version = 1
        type = "microsoft_cancel_domain"
        domain = $script:DomainName
        action_id = $ActionId
        steps = $script:Steps
        summary = $script:Summary
        lastUpdated = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    }
    Update-ActionResult -ActionId $ActionId -Result $result
}

function Write-CancelLog {
    param(
        [string]$EventType,
        [string]$Severity,
        [string]$Message,
        [hashtable]$Metadata = $null
    )

    $level = switch ($Severity) {
        "error" { "Error" }
        "warn" { "Warning" }
        default { "Info" }
    }
    Write-Log $Message -Level $level
    Add-ActionLog -ActionId $ActionId -DomainId $DomainId -CustomerId $script:CustomerId -EventType $EventType -Severity $Severity -Message $Message -Metadata $Metadata
}

function Resolve-DomainInboxes {
    $query = "domain_id=eq.$DomainId&select=id,email,username,status,first_name,last_name&order=created_at.asc"
    $result = Invoke-SupabaseApi -Method GET -Table "inboxes" -Query $query
    if (-not $result.Success) {
        throw "Failed to load inboxes for domain: $($result.Error)"
    }

    $rows = @($result.Data)
    $script:Summary.inboxRows = $rows.Count
    $emails = New-Object System.Collections.Generic.List[string]

    foreach ($row in $rows) {
        $email = [string]$row.email
        if (-not $email) {
            $username = [string]$row.username
            if ($username) { $email = "$username@$($script:DomainName)" }
        }
        $email = ($email | ForEach-Object { [string]$_ }).Trim().ToLower()
        if (-not $email) { continue }
        if (-not $emails.Contains($email)) { $emails.Add($email) | Out-Null }
    }

    return @{ Rows = $rows; Emails = @($emails) }
}

function Remove-MailboxesByEmail {
    param([string[]]$Emails)

    $deleted = 0
    $missing = 0

    foreach ($email in @($Emails)) {
        if (-not $email) { continue }
        try {
            $mailbox = Get-Mailbox -Identity $email -ErrorAction Stop
            if ($DryRun) {
                Write-CancelLog -EventType "microsoft_cancel_dryrun_mailbox" -Severity "info" -Message "[DryRun] Would remove mailbox $email"
                continue
            }
            # Use the exact SMTP address for removal. `$mailbox.Identity` can be
            # a display name, which is ambiguous in bulk persona-style tenants.
            Remove-Mailbox -Identity $email -Confirm:$false -ErrorAction Stop
            $deleted += 1
            Write-CancelLog -EventType "microsoft_cancel_mailbox_deleted" -Severity "info" -Message "Removed mailbox $email"
        } catch {
            $errorText = $_.Exception.Message
            $normalized = [string]$errorText
            if ($normalized -match "couldn't be found" -or $normalized -match "Cannot find object" -or $normalized -match "doesn't exist") {
                $missing += 1
                Write-CancelLog -EventType "microsoft_cancel_mailbox_missing" -Severity "info" -Message "Mailbox already absent: $email"
            } else {
                Write-CancelLog -EventType "microsoft_cancel_mailbox_delete_error" -Severity "warn" -Message "Mailbox removal error for ${email}: $errorText"
            }
        }
    }

    $script:Summary.mailboxDeleted = [int]$script:Summary.mailboxDeleted + $deleted
    $script:Summary.mailboxAlreadyMissing = [int]$script:Summary.mailboxAlreadyMissing + $missing
    return @{ Deleted = $deleted; Missing = $missing }
}

function Get-StableRecipientRemovalIdentity {
    param([object]$Recipient)

    if (-not $Recipient) { return "" }

    $primary = [string]$Recipient.PrimarySmtpAddress
    if ($primary) { return $primary }

    $windowsEmail = [string]$Recipient.WindowsEmailAddress
    if ($windowsEmail) { return $windowsEmail }

    $externalDirectoryObjectId = [string]$Recipient.ExternalDirectoryObjectId
    if ($externalDirectoryObjectId) { return $externalDirectoryObjectId }

    $guid = [string]$Recipient.Guid
    if ($guid) { return $guid }

    return [string]$Recipient.Identity
}

function Remove-ExchangeRecipientsByDomain {
    param([string]$Domain, [string]$ProtectedDomain = "")

    $domainName = Normalize-DomainValue $Domain
    $protectedDomainName = Normalize-DomainValue $ProtectedDomain
    $removed = 0
    $aliasRemoved = 0
    $protectedSkipped = 0

    $recipientList = @()
    try {
        $recipientList = @(Get-Recipient -ResultSize Unlimited -ErrorAction SilentlyContinue)
    } catch {
        return @{ Removed = 0; Error = $_.Exception.Message }
    }

    foreach ($recipient in $recipientList) {
        if (-not $recipient) { continue }

        $oldAliases = @(Get-RecipientAddressesForDomain -Recipient $recipient -Domain $domainName)
        $primary = [string]$recipient.PrimarySmtpAddress
        $primaryBelongsToDomain = ($primary -and (Get-EmailAddressDomain -Address $primary) -eq $domainName)
        $hasOldDomainAlias = ($oldAliases.Count -gt 0)

        if (-not $primaryBelongsToDomain -and -not $hasOldDomainAlias) { continue }

        $hasProtectedDomain = ($protectedDomainName -and (Test-RecipientHasDomain -Recipient $recipient -Domain $protectedDomainName))
        if ($hasProtectedDomain) {
            $protectedSkipped += 1
            if ($oldAliases.Count -gt 0) {
                if ($DryRun) {
                    Write-CancelLog -EventType "microsoft_cancel_dryrun_remove_protected_alias" -Severity "info" -Message "[DryRun] Would remove old-domain alias(es) from protected replacement recipient $($recipient.Identity): $($oldAliases -join ', ')"
                } else {
                    try {
                        Set-Mailbox -Identity $recipient.Identity -EmailAddresses @{Remove=$oldAliases} -Confirm:$false -ErrorAction Stop
                        $aliasRemoved += $oldAliases.Count
                        Write-CancelLog -EventType "microsoft_cancel_protected_alias_removed" -Severity "warn" -Message "Removed old-domain alias(es) from protected replacement recipient $($recipient.Identity): $($oldAliases -join ', ')"
                    } catch {
                        Write-CancelLog -EventType "microsoft_cancel_protected_alias_remove_error" -Severity "error" -Message "Failed to remove old-domain alias(es) from protected replacement recipient $($recipient.Identity): $($_.Exception.Message)"
                    }
                }
            }
            continue
        }

        if (-not $primaryBelongsToDomain) {
            if ($oldAliases.Count -gt 0) {
                if ($DryRun) {
                    Write-CancelLog -EventType "microsoft_cancel_dryrun_remove_alias" -Severity "info" -Message "[DryRun] Would remove old-domain alias(es) from non-domain-primary recipient $($recipient.Identity): $($oldAliases -join ', ')"
                } else {
                    try {
                        Set-Mailbox -Identity $recipient.Identity -EmailAddresses @{Remove=$oldAliases} -Confirm:$false -ErrorAction Stop
                        $aliasRemoved += $oldAliases.Count
                        Write-CancelLog -EventType "microsoft_cancel_alias_removed" -Severity "warn" -Message "Removed old-domain alias(es) from non-domain-primary recipient $($recipient.Identity): $($oldAliases -join ', ')"
                    } catch {
                        Write-CancelLog -EventType "microsoft_cancel_alias_remove_error" -Severity "warn" -Message "Failed to remove old-domain alias(es) from $($recipient.Identity): $($_.Exception.Message)"
                    }
                }
            }
            continue
        }

        if ($DryRun) {
            Write-CancelLog -EventType "microsoft_cancel_dryrun_recipient" -Severity "info" -Message "[DryRun] Would remove recipient $($recipient.Identity) [$($recipient.RecipientTypeDetails)]"
            continue
        }

        try {
            $recipientType = [string]$recipient.RecipientTypeDetails
            $removeIdentity = Get-StableRecipientRemovalIdentity -Recipient $recipient
            switch ($recipientType) {
                "MailUniversalDistributionGroup" {
                    Remove-DistributionGroup -Identity $removeIdentity -BypassSecurityGroupManagerCheck -Confirm:$false -ErrorAction Stop
                    $removed += 1
                }
                "MailUniversalSecurityGroup" {
                    Remove-DistributionGroup -Identity $removeIdentity -BypassSecurityGroupManagerCheck -Confirm:$false -ErrorAction Stop
                    $removed += 1
                }
                "GroupMailbox" {
                    Remove-UnifiedGroup -Identity $removeIdentity -Confirm:$false -ErrorAction Stop
                    $removed += 1
                }
                "MailContact" {
                    Remove-MailContact -Identity $removeIdentity -Confirm:$false -ErrorAction Stop
                    $removed += 1
                }
                "MailUser" {
                    Remove-MailUser -Identity $removeIdentity -Confirm:$false -ErrorAction Stop
                    $removed += 1
                }
                "RoomMailbox" {
                    Remove-Mailbox -Identity $removeIdentity -Confirm:$false -ErrorAction Stop
                    $removed += 1
                }
                "UserMailbox" {
                    Remove-Mailbox -Identity $removeIdentity -Confirm:$false -ErrorAction Stop
                    $removed += 1
                }
                "SharedMailbox" {
                    Remove-Mailbox -Identity $removeIdentity -Confirm:$false -ErrorAction Stop
                    $removed += 1
                }
                default {
                    Write-CancelLog -EventType "microsoft_cancel_recipient_skipped" -Severity "warn" -Message "Skipped unsupported recipient type $recipientType for $($recipient.Identity)"
                }
            }
        } catch {
            Write-CancelLog -EventType "microsoft_cancel_recipient_error" -Severity "warn" -Message "Recipient removal failed for $($recipient.Identity): $($_.Exception.Message)"
        }
    }

    $script:Summary.recipientCleanupDeleted = [int]$script:Summary.recipientCleanupDeleted + $removed
    $script:Summary.recipientAliasCleanupRemoved = [int]$script:Summary.recipientAliasCleanupRemoved + $aliasRemoved
    $script:Summary.recipientProtectedSkipped = [int]$script:Summary.recipientProtectedSkipped + $protectedSkipped
    return @{ Removed = $removed; AliasRemoved = $aliasRemoved; ProtectedSkipped = $protectedSkipped }
}

function Get-GraphUsersByDomain {
    param(
        [string]$Bearer,
        [string]$Domain
    )

    $users = New-Object System.Collections.Generic.List[object]
    $suffix = "@$($Domain.ToLower())"
    $url = "https://graph.microsoft.com/v1.0/users?`$select=id,userPrincipalName,mail&`$top=999"

    while ($url) {
        $response = Invoke-GraphRequest -Method GET -Url $url -Bearer $Bearer
        foreach ($user in @($response.value)) {
            if (-not $user) { continue }
            $upn = [string]$user.userPrincipalName
            $mail = [string]$user.mail
            if (($upn -and $upn.ToLower().EndsWith($suffix)) -or ($mail -and $mail.ToLower().EndsWith($suffix))) {
                $users.Add($user) | Out-Null
            }
        }
        $url = [string]$response.'@odata.nextLink'
        if (-not $url) { break }
    }

    return $users.ToArray()
}

function Remove-GraphUsersByDomain {
    param(
        [string]$Bearer,
        [string]$Domain,
        [string]$AdminEmail
    )

    $deleted = 0
    $users = @(Get-GraphUsersByDomain -Bearer $Bearer -Domain $Domain)
    foreach ($user in $users) {
        if (-not $user -or -not $user.id) { continue }
        $upn = [string]$user.userPrincipalName
        if ($upn -and $AdminEmail -and $upn.Trim().ToLower() -eq $AdminEmail.Trim().ToLower()) {
            continue
        }

        if ($DryRun) {
            Write-CancelLog -EventType "microsoft_cancel_dryrun_graph_user" -Severity "info" -Message "[DryRun] Would delete Graph user $upn"
            continue
        }

        try {
            Invoke-GraphRequest -Method DELETE -Url "https://graph.microsoft.com/v1.0/users/$($user.id)" -Bearer $Bearer | Out-Null
            $deleted += 1
            Write-CancelLog -EventType "microsoft_cancel_graph_user_deleted" -Severity "info" -Message "Deleted Graph user $upn"
        } catch {
            Write-CancelLog -EventType "microsoft_cancel_graph_user_error" -Severity "warn" -Message "Graph user delete failed for ${upn}: $($_.Exception.Message)"
        }
    }

    $script:Summary.graphUsersDeleted = [int]$script:Summary.graphUsersDeleted + $deleted
    return @{ Deleted = $deleted; Total = $users.Count }
}

function Remove-AcceptedDomainWithRetry {
    param(
        [string]$Domain,
        [string]$Bearer,
        [string]$AdminEmail,
        [string]$ProtectedDomain = "",
        [int]$MaxAttempts = 6
    )

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            $domainExists = $true
            try {
                Invoke-GraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer | Out-Null
            } catch {
                $probeMessage = $_.Exception.Message
                if ($probeMessage -match "Request_ResourceNotFound" -or $probeMessage -match "404" -or $probeMessage -match "ResourceNotFound") {
                    $domainExists = $false
                } else {
                    throw
                }
            }

            if (-not $domainExists) {
                Write-CancelLog -EventType "microsoft_cancel_domain_already_removed" -Severity "info" -Message "Accepted domain already removed: $Domain"
                $script:Summary.acceptedDomainRemoved = $true
                return @{ Success = $true; AlreadyRemoved = $true; Attempts = $attempt }
            }

            if ($DryRun) {
                Write-CancelLog -EventType "microsoft_cancel_dryrun_remove_domain" -Severity "info" -Message "[DryRun] Would remove accepted domain $Domain"
                $script:Summary.acceptedDomainRemoved = $false
                return @{ Success = $true; DryRun = $true; Attempts = $attempt }
            }

            Invoke-GraphRequest -Method DELETE -Url "https://graph.microsoft.com/v1.0/domains/$Domain" -Bearer $Bearer | Out-Null
            Write-CancelLog -EventType "microsoft_cancel_domain_removed" -Severity "info" -Message "Accepted domain removed: $Domain"
            $script:Summary.acceptedDomainRemoved = $true
            return @{ Success = $true; Attempts = $attempt }
        } catch {
            $errorText = $_.Exception.Message
            if ($errorText -match "Request_ResourceNotFound" -or $errorText -match "404" -or $errorText -match "ResourceNotFound") {
                Write-CancelLog -EventType "microsoft_cancel_domain_already_removed" -Severity "info" -Message "Accepted domain already removed: $Domain"
                $script:Summary.acceptedDomainRemoved = $true
                return @{ Success = $true; AlreadyRemoved = $true; Attempts = $attempt }
            }
            Write-CancelLog -EventType "microsoft_cancel_domain_remove_retry" -Severity "warn" -Message "Remove accepted domain failed (attempt $attempt/$MaxAttempts): $errorText"

            if ($attempt -lt $MaxAttempts) {
                Remove-ExchangeRecipientsByDomain -Domain $Domain -ProtectedDomain $ProtectedDomain | Out-Null
                Remove-GraphUsersByDomain -Bearer $Bearer -Domain $Domain -AdminEmail $AdminEmail | Out-Null
                Start-Sleep -Seconds ([Math]::Min(60, 10 * $attempt))
                continue
            }

            return @{ Success = $false; Error = $errorText; Attempts = $attempt }
        }
    }

    return @{ Success = $false; Error = "Unknown accepted domain removal failure"; Attempts = $MaxAttempts }
}

function Finalize-SupabaseState {
    param(
        [string]$PaymentStatusOnCancel = ""
    )

    $validPaymentStatuses = @("paid", "unpaid", "refunded")
    $paymentStatus = if ($validPaymentStatuses -contains $PaymentStatusOnCancel) { $PaymentStatusOnCancel } else { "refunded" }

    $history = ""
    if ($script:DomainRecord -and $script:DomainRecord.action_history) {
        $history = [string]$script:DomainRecord.action_history
    }
    $history = Add-HistoryEntry -History $history -Entry "Microsoft cancellation completed by worker (accepted domain removed: $($script:Summary.acceptedDomainRemoved))"

    Update-Domain -DomainId $DomainId -Fields @{
        status = "cancelled"
        payment_status = $paymentStatus
        interim_status = "Microsoft - Cancellation Complete"
        action_history = $history
        cancel_at = $null
    }

    $inboxData = Resolve-DomainInboxes
    $inboxRows = @($inboxData.Rows)
    foreach ($row in $inboxRows) {
        if (-not $row.id) { continue }
        Update-Inbox -InboxId $row.id -Fields @{ status = "deleted" }
    }
}

function Fail-CancellationAction {
    param(
        [string]$ErrorMessage,
        [object]$FailedStep = $null
    )

    if ($FailedStep) {
        Fail-Step -Step $FailedStep -ErrorMessage $ErrorMessage
    }

    $history = ""
    if ($script:DomainRecord -and $script:DomainRecord.action_history) {
        $history = [string]$script:DomainRecord.action_history
    }
    $history = Add-HistoryEntry -History $history -Entry "Microsoft cancellation failed: $ErrorMessage"

    Update-Domain -DomainId $DomainId -Fields @{
        status = "in_progress"
        interim_status = "Microsoft - Cancellation Failed"
        action_history = $history
    }

    Write-CancelLog -EventType "microsoft_cancel_failed" -Severity "error" -Message "Microsoft cancellation failed for $($script:DomainName): $ErrorMessage" -Metadata @{
        summary = $script:Summary
    }

    Fail-Action -Action $script:ActionRecord -ErrorMessage $ErrorMessage -DefaultMaxRetries 5
}

Add-Type -AssemblyName System.Web

if (-not (Get-Module -ListAvailable -Name ExchangeOnlineManagement)) {
    Write-Host "Installing ExchangeOnlineManagement module..." -ForegroundColor Yellow
    Install-Module -Name ExchangeOnlineManagement -Force -AllowClobber -Scope CurrentUser
}
Import-Module ExchangeOnlineManagement -ErrorAction SilentlyContinue

$script:ActionRecord = Get-Action -ActionId $ActionId
if (-not $script:ActionRecord) {
    throw "Action not found: $ActionId"
}

$script:DomainRecord = Get-Domain -DomainId $DomainId
if (-not $script:DomainRecord) {
    Update-ActionStatus -ActionId $ActionId -Status "failed" -Error "Domain not found for action"
    throw "Domain not found: $DomainId"
}

$script:DomainName = [string]$script:DomainRecord.domain
$script:CustomerId = [string]$script:DomainRecord.customer_id
$script:ReplacementProtectedDomain = Get-ReplacementProtectedDomain
if ($script:ReplacementProtectedDomain) {
    $script:Summary.replacementProtectedDomain = $script:ReplacementProtectedDomain
    Write-CancelLog -EventType "microsoft_cancel_replacement_protection_enabled" -Severity "warn" -Message "Replacement cancellation guard enabled: protected new domain $($script:ReplacementProtectedDomain) will not be deleted while cancelling $($script:DomainName)" -Metadata @{
        old_domain = $script:DomainName
        protected_domain = $script:ReplacementProtectedDomain
    }
}

if ([string]$script:DomainRecord.provider -ne "microsoft" -and [string]$script:DomainRecord.provider -ne "smtp_plus" -and [string]$script:DomainRecord.provider -ne "azure") {
    Update-ActionStatus -ActionId $ActionId -Status "failed" -Error "microsoft_cancel_domain is only valid for Microsoft-backed domains"
    throw "Domain $($script:DomainName) is not Microsoft provider"
}

Update-ActionStatus -ActionId $ActionId -Status "in_progress"
Write-CancelLog -EventType "microsoft_cancel_started" -Severity "info" -Message "Microsoft cancellation started for $($script:DomainName)"
Update-Domain -DomainId $DomainId -Fields @{
    status = "in_progress"
    interim_status = "Microsoft - Cancellation In Progress"
}
Persist-Progress

$adminStep = New-Step -Name "resolve_admin"
$assignedAdminRecord = Get-AssignedAdmin -DomainId $DomainId
$preferredAdminId = if ($assignedAdminRecord -and $assignedAdminRecord.id) { [string]$assignedAdminRecord.id } else { $null }
$adminRecord = Acquire-MicrosoftAdminLock -ActionId $ActionId -DomainId $DomainId -PreferredAdminId $preferredAdminId -AllowThresholdExceededPreferredAdmin
if (-not $adminRecord) {
    if (Test-ActiveAdminExists -Provider "microsoft") {
        $waitMessage = if ($preferredAdminId) {
            "Waiting for the assigned Microsoft admin lock"
        } else {
            "Waiting for an available Microsoft admin lock"
        }
        Write-CancelLog -EventType "admin_lock_wait" -Severity "warn" -Message "$waitMessage for $($script:DomainName)"
        Requeue-ActionWithoutPenalty -Action $script:ActionRecord -Reason $waitMessage -DelaySeconds 60
        return
    }
}
if (-not $adminRecord -or -not $adminRecord.email -or -not $adminRecord.password) {
    Fail-CancellationAction -ErrorMessage "No Microsoft admin credentials available for cancellation" -FailedStep $adminStep
    exit 1
}
Ensure-DomainAdminAssignment -DomainId $DomainId -AdminCredId $adminRecord.id
Complete-Step -Step $adminStep -Details @{ admin_email = $adminRecord.email }

$authStep = New-Step -Name "authenticate"
$adminEmail = [string]$adminRecord.email
$adminPassword = [string]$adminRecord.password
$adminDomainPart = ($adminEmail -split '@')[1]
$tenantId = Get-TenantIdFromDomain -Domain $adminDomainPart
if (-not $tenantId) {
    Fail-CancellationAction -ErrorMessage "Could not resolve tenant id for admin $adminEmail" -FailedStep $authStep
    exit 1
}

$bearer = Get-ROPCToken -TenantId $tenantId -ClientId $AzureCliPublicClientId -Username $adminEmail -Password $adminPassword
if (-not $bearer) {
    Fail-CancellationAction -ErrorMessage "Failed to obtain Graph token for admin $adminEmail" -FailedStep $authStep
    exit 1
}

if (-not $DryRun) {
    try {
        $securePwd = ConvertTo-SecureString $adminPassword -AsPlainText -Force
        $creds = New-Object System.Management.Automation.PSCredential($adminEmail, $securePwd)
        Connect-ExchangeOnline -Credential $creds -ShowBanner:$false -ErrorAction Stop
    } catch {
        Fail-CancellationAction -ErrorMessage "Exchange Online connection failed: $($_.Exception.Message)" -FailedStep $authStep
        exit 1
    }
}
Complete-Step -Step $authStep -Details @{ tenant_id = $tenantId; exchange_connected = (-not $DryRun) }

try {
    $mailboxStep = New-Step -Name "delete_mailboxes"
    $domainMailboxData = Resolve-DomainInboxes
    $mailboxResult = Remove-MailboxesByEmail -Emails $domainMailboxData.Emails
    Complete-Step -Step $mailboxStep -Details @{
        inbox_rows = $domainMailboxData.Rows.Count
        deleted = $mailboxResult.Deleted
        missing = $mailboxResult.Missing
    }

    $recipientStep = New-Step -Name "cleanup_exchange_recipients"
    $recipientResult = Remove-ExchangeRecipientsByDomain -Domain $script:DomainName -ProtectedDomain $script:ReplacementProtectedDomain
    Complete-Step -Step $recipientStep -Details @{
        removed = $recipientResult.Removed
        aliases_removed = $recipientResult.AliasRemoved
        protected_skipped = $recipientResult.ProtectedSkipped
        error = $recipientResult.Error
    }

    $graphStep = New-Step -Name "cleanup_graph_users"
    $graphResult = Remove-GraphUsersByDomain -Bearer $bearer -Domain $script:DomainName -AdminEmail $adminEmail
    Complete-Step -Step $graphStep -Details @{
        total = $graphResult.Total
        deleted = $graphResult.Deleted
    }

    $removeDomainStep = New-Step -Name "remove_accepted_domain"
    $removeDomainResult = Remove-AcceptedDomainWithRetry -Domain $script:DomainName -Bearer $bearer -AdminEmail $adminEmail -ProtectedDomain $script:ReplacementProtectedDomain -MaxAttempts 6
    if (-not $removeDomainResult.Success) {
        Fail-CancellationAction -ErrorMessage "Failed to remove accepted domain $($script:DomainName): $($removeDomainResult.Error)" -FailedStep $removeDomainStep
        exit 1
    }
    Complete-Step -Step $removeDomainStep -Details @{
        attempts = $removeDomainResult.Attempts
        already_removed = $removeDomainResult.AlreadyRemoved
        dry_run = $removeDomainResult.DryRun
    }

    $finalizeStep = New-Step -Name "finalize_supabase"
    $paymentStatusOnCancel = ""
    if ($script:ActionRecord.payload -and $script:ActionRecord.payload.payment_status_on_cancel) {
        $paymentStatusOnCancel = [string]$script:ActionRecord.payload.payment_status_on_cancel
    }
    Finalize-SupabaseState -PaymentStatusOnCancel $paymentStatusOnCancel
    Complete-Step -Step $finalizeStep -Details @{ payment_status = $paymentStatusOnCancel }

    Update-ActionStatus -ActionId $ActionId -Status "completed" -Result @{
        message = "Microsoft domain cancellation complete"
        type = "microsoft_cancel_domain"
        domain = $script:DomainName
        summary = $script:Summary
        steps = $script:Steps
    }
    Write-CancelLog -EventType "microsoft_cancel_completed" -Severity "info" -Message "Microsoft cancellation completed for $($script:DomainName)" -Metadata @{ summary = $script:Summary }
} catch {
    $fatalError = $_.Exception.Message
    if ($_.InvocationInfo -and $_.InvocationInfo.PositionMessage) {
        $fatalError = "$fatalError | at: $($_.InvocationInfo.PositionMessage)"
    }
    if ($_.ScriptStackTrace) {
        $fatalError = "$fatalError | stack: $($_.ScriptStackTrace)"
    }
    Fail-CancellationAction -ErrorMessage $fatalError
    exit 1
} finally {
    if (-not $DryRun) {
        try { Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue } catch { }
    }
    if ($adminRecord -and $adminRecord.id) {
        Release-MicrosoftAdminLock -ActionId $ActionId | Out-Null
    }
}
