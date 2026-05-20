<#
.SYNOPSIS
    Actively proves whether Jack/ProfitPath destination Microsoft tenants can send.
.DESCRIPTION
    For each destination admin used by the Jack threshold migration, creates one
    temporary shared mailbox on the tenant's onmicrosoft.com domain, unblocks it,
    enables SMTP AUTH, sends one email, then checks sender-side message trace
    detail for 5.7.705 / threshold evidence.

    This intentionally does not assign licenses.
#>

param(
    [string]$BatchId = "ffa54dd8-3fd1-4336-8e06-6f73872432f4",
    [string]$Recipient = "leads+99@justresultsagency.com",
    [int]$ShardCount = 1,
    [int]$ShardIndex = 0,
    [int]$Limit = 0,
    [int]$TracePolls = 4,
    [int]$TracePollSeconds = 60,
    [int]$SmtpMaxAttempts = 4,
    [int]$SmtpTimeoutSeconds = 90,
    [int]$SmtpRetryBaseSeconds = 20,
    [int]$SmtpTcpProbeSeconds = 15,
    [ValidateSet("Graph","Smtp","Auto")]
    [string]$SendTransport = "Graph",
    [switch]$IncludeKnownThreshold,
    [switch]$NoCleanup,
    [string]$RunId = "",
    [string]$OutputJsonl = "",
    [string]$OutputCsv = "",
    [string]$LogDir = (Join-Path $PSScriptRoot "logs"),
    [string[]]$SkipAdminEmail = @(),
    [string]$TargetAdminCsv = "",
    [string]$CredentialCsv = "",
    [switch]$AllActiveMicrosoftTenants
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

. (Join-Path $PSScriptRoot "config.ps1")

$ThresholdPattern = "5\.7\.705|5\.7\.708|tenant has exceeded threshold|exceeded threshold|Access denied.*threshold|S\(7136\)"
$SmtpTransientPattern = "timed out|timeout|temporarily unavailable|resource temporarily unavailable|connection.*closed|connection.*reset|connection.*aborted|connection.*timed|DNS|No such host|Name or service not known|try again|rate|throttl|4\.\d+\.\d+"
$SmtpPermanentAuthPattern = "authentication unsuccessful|535|5\.7\.3|5\.7\.57|client was not authenticated|smtp auth.*disabled|basic authentication is disabled|invalid credentials|logon failure|account is disabled|AADSTS50034|AADSTS50053|AADSTS50055|AADSTS50056|AADSTS50076|AADSTS50126"
$GraphResourceAppId = "00000003-0000-0000-c000-000000000000"
$ProbeMailSendAppName = "SI Threshold Probe MailSend"
$ProbeMailSendScopes = @(
    @{ name = "Mail.Send"; id = "e383f46e-2787-4529-855e-0e479a3ffac0"; type = "Scope" },
    @{ name = "User.Read"; id = "e1fe6dd8-ba31-4d61-89e7-88639da4683d"; type = "Scope" },
    @{ name = "openid"; id = "37f7f235-527c-4136-accd-4a02d197296e"; type = "Scope" },
    @{ name = "profile"; id = "14dad69e-099b-42c9-810b-d002981feec1"; type = "Scope" },
    @{ name = "offline_access"; id = "7427e0e9-2fba-42fe-b0c0-848c9e6a8182"; type = "Scope" }
)

if (-not $RunId) { $RunId = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ") }
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Force -Path $LogDir | Out-Null }
if (-not $OutputJsonl) { $OutputJsonl = Join-Path $LogDir "jack-destination-active-threshold-$RunId-shard-$ShardIndex.jsonl" }
if (-not $OutputCsv) { $OutputCsv = Join-Path $LogDir "jack-destination-active-threshold-$RunId-shard-$ShardIndex.csv" }
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12

function Write-Jsonl {
    param([object]$Payload)
    ($Payload | ConvertTo-Json -Depth 12 -Compress) | Add-Content -Path $OutputJsonl -Encoding UTF8
}

function Normalize-Text {
    param([object]$Value)
    return ([string]$Value).Trim()
}

function Get-TenantTargets {
    if ($CredentialCsv) {
        if (-not (Test-Path $CredentialCsv)) { throw "Credential CSV not found: $CredentialCsv" }
        $rows = @(Import-Csv -Path $CredentialCsv)
        $targets = New-Object System.Collections.Generic.List[object]
        $seen = @{}

        foreach ($row in $rows) {
            $email = ""
            $password = ""
            foreach ($name in @("email", "admin", "adminemail", "admin_email", "microsoft email", "full onmicrosoft email", "AdminEmail")) {
                $prop = $row.PSObject.Properties | Where-Object { $_.Name.Trim().ToLowerInvariant() -eq $name } | Select-Object -First 1
                if ($prop -and $prop.Value) { $email = ([string]$prop.Value).Trim().ToLowerInvariant(); break }
            }
            foreach ($name in @("password", "admin password", "admin_password", "microsoft password", "AdminPassword")) {
                $prop = $row.PSObject.Properties | Where-Object { $_.Name.Trim().ToLowerInvariant() -eq $name } | Select-Object -First 1
                if ($prop -and $prop.Value) { $password = [string]$prop.Value; break }
            }

            if (-not $email -or -not $password) { continue }
            if ($email -notmatch "^admin@[a-z0-9._%+-]+\.onmicrosoft\.com$") {
                throw "Invalid Microsoft admin email in credential CSV: $email"
            }
            if ($seen.ContainsKey($email)) { continue }
            $seen[$email] = $true

            $targets.Add([pscustomobject]@{
                AdminId = ""
                AdminEmail = $email
                AdminPassword = $password
                Status = "Candidate"
                Active = $false
                DomainCount = 0
                Domains = @()
            }) | Out-Null
        }

        $credentialTargets = @($targets.ToArray() | Sort-Object AdminEmail)
        if ($ShardCount -gt 1) {
            $filtered = New-Object System.Collections.Generic.List[object]
            for ($i = 0; $i -lt $credentialTargets.Count; $i++) {
                if (($i % $ShardCount) -eq $ShardIndex) { $filtered.Add($credentialTargets[$i]) | Out-Null }
            }
            $credentialTargets = @($filtered.ToArray())
        }
        if ($Limit -gt 0) { $credentialTargets = @($credentialTargets | Select-Object -First $Limit) }
        return $credentialTargets
    }

    if ($TargetAdminCsv) {
        if (-not (Test-Path $TargetAdminCsv)) { throw "Target admin CSV not found: $TargetAdminCsv" }
        $csvRows = @(Import-Csv -Path $TargetAdminCsv)
        $targetMetaById = @{}
        $targetIds = New-Object System.Collections.Generic.List[string]

        foreach ($row in $csvRows) {
            $adminId = ""
            if ($row.AdminId) { $adminId = [string]$row.AdminId }
            elseif ($row.admin_id) { $adminId = [string]$row.admin_id }
            elseif ($row.id) { $adminId = [string]$row.id }
            $adminId = $adminId.Trim()
            if (-not $adminId -or $targetMetaById.ContainsKey($adminId)) { continue }
            $targetMetaById[$adminId] = $row
            $targetIds.Add($adminId) | Out-Null
        }

        if ($targetIds.Count -eq 0) { throw "Target admin CSV did not contain AdminId/admin_id/id rows: $TargetAdminCsv" }

        $targets = New-Object System.Collections.Generic.List[object]
        for ($offset = 0; $offset -lt $targetIds.Count; $offset += 75) {
            $chunk = @($targetIds | Select-Object -Skip $offset -First 75)
            $query = "id=in.($($chunk -join ','))&select=id,email,password,status,active&limit=1000"
            $result = Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query $query
            if (-not $result.Success) { throw "Failed to load target Microsoft admin credentials: $($result.Error)" }

            foreach ($admin in @($result.Data)) {
                if (-not $admin.id -or -not $admin.email -or -not $admin.password) { continue }
                $adminId = [string]$admin.id
                $meta = $targetMetaById[$adminId]
                $domainCount = 0
                if ($meta -and $meta.DomainCount) { [void][int]::TryParse([string]$meta.DomainCount, [ref]$domainCount) }
                elseif ($meta -and $meta.domain_count) { [void][int]::TryParse([string]$meta.domain_count, [ref]$domainCount) }

                $targets.Add([pscustomobject]@{
                    AdminId = $adminId
                    AdminEmail = ([string]$admin.email).Trim().ToLowerInvariant()
                    AdminPassword = [string]$admin.password
                    Status = [string]$admin.status
                    Active = [bool]$admin.active
                    DomainCount = $domainCount
                    Domains = @()
                }) | Out-Null
            }
        }

        $csvTargets = @($targets.ToArray() | Sort-Object AdminEmail)
        if ($ShardCount -gt 1) {
            $filtered = New-Object System.Collections.Generic.List[object]
            for ($i = 0; $i -lt $csvTargets.Count; $i++) {
                if (($i % $ShardCount) -eq $ShardIndex) { $filtered.Add($csvTargets[$i]) | Out-Null }
            }
            $csvTargets = @($filtered.ToArray())
        }
        if ($Limit -gt 0) { $csvTargets = @($csvTargets | Select-Object -First $Limit) }
        return $csvTargets
    }

    if ($AllActiveMicrosoftTenants) {
        $targets = New-Object System.Collections.Generic.List[object]
        $offset = 0
        $pageSize = 1000

        while ($true) {
            $query = "active=eq.true&status=eq.Active&email=ilike.*onmicrosoft.com&select=id,email,password,status,active&order=email.asc&limit=$pageSize&offset=$offset"
            $result = Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query $query
            if (-not $result.Success) { throw "Failed to load active Microsoft admin credentials: $($result.Error)" }
            $rows = @($result.Data)
            foreach ($admin in $rows) {
                if (-not $admin.id -or -not $admin.email -or -not $admin.password) { continue }
                $email = ([string]$admin.email).Trim().ToLowerInvariant()
                if (-not $email.EndsWith(".onmicrosoft.com")) { continue }
                $targets.Add([pscustomobject]@{
                    AdminId = [string]$admin.id
                    AdminEmail = $email
                    AdminPassword = [string]$admin.password
                    Status = [string]$admin.status
                    Active = [bool]$admin.active
                    DomainCount = 0
                    Domains = @()
                }) | Out-Null
            }

            if ($rows.Count -lt $pageSize) { break }
            $offset += $pageSize
        }

        $allActiveTargets = @($targets.ToArray() | Sort-Object AdminEmail)
        if ($ShardCount -gt 1) {
            $filtered = New-Object System.Collections.Generic.List[object]
            for ($i = 0; $i -lt $allActiveTargets.Count; $i++) {
                if (($i % $ShardCount) -eq $ShardIndex) { $filtered.Add($allActiveTargets[$i]) | Out-Null }
            }
            $allActiveTargets = @($filtered.ToArray())
        }
        if ($Limit -gt 0) { $allActiveTargets = @($allActiveTargets | Select-Object -First $Limit) }
        return $allActiveTargets
    }

    $domainsResult = Invoke-SupabaseApi -Method GET -Table "domains" -Query "order_batch_id=eq.$BatchId&select=id,domain,status,interim_status&limit=1000"
    if (-not $domainsResult.Success) { throw "Failed to load domains: $($domainsResult.Error)" }
    $domains = @($domainsResult.Data)
    $domainIds = @($domains | ForEach-Object { [string]$_.id } | Where-Object { $_ })

    $assignments = @()
    for ($offset = 0; $offset -lt $domainIds.Count; $offset += 75) {
        $chunk = @($domainIds | Select-Object -Skip $offset -First 75)
        $query = "domain_id=in.($($chunk -join ','))&select=domain_id,admin_cred_id,assigned_at,admin_credentials(id,email,password,status,active)&order=assigned_at.asc&limit=1000"
        $result = Invoke-SupabaseApi -Method GET -Table "domain_admin_assignments" -Query $query
        if (-not $result.Success) { throw "Failed to load domain_admin_assignments: $($result.Error)" }
        $assignments += @($result.Data)
    }

    $domainById = @{}
    foreach ($domain in $domains) { $domainById[[string]$domain.id] = $domain }

    $byDomain = @{}
    foreach ($assignment in $assignments) {
        $domainId = [string]$assignment.domain_id
        if (-not $byDomain.ContainsKey($domainId)) { $byDomain[$domainId] = New-Object System.Collections.Generic.List[object] }
        $byDomain[$domainId].Add($assignment) | Out-Null
    }

    $targetMap = @{}
    foreach ($domainId in $byDomain.Keys) {
        $ordered = @($byDomain[$domainId] | Sort-Object { [datetime]$_.assigned_at })
        if ($ordered.Count -lt 2) { continue }
        $first = $ordered[0]
        $last = $ordered[$ordered.Count - 1]
        if ([string]$first.admin_cred_id -eq [string]$last.admin_cred_id) { continue }

        $admin = $last.admin_credentials
        if (-not $admin -or -not $admin.id -or -not $admin.email -or -not $admin.password) { continue }
        $adminId = [string]$admin.id
        if (-not $targetMap.ContainsKey($adminId)) {
            $targetMap[$adminId] = [ordered]@{
                AdminId = $adminId
                AdminEmail = ([string]$admin.email).Trim().ToLowerInvariant()
                AdminPassword = [string]$admin.password
                Status = [string]$admin.status
                Active = [bool]$admin.active
                DomainCount = 0
                Domains = New-Object System.Collections.Generic.List[string]
            }
        }
        $targetMap[$adminId].DomainCount += 1
        if ($domainById.ContainsKey($domainId)) {
            $targetMap[$adminId].Domains.Add([string]$domainById[$domainId].domain) | Out-Null
        }
    }

    $targets = @($targetMap.Values | ForEach-Object {
        [pscustomobject]@{
            AdminId = $_.AdminId
            AdminEmail = $_.AdminEmail
            AdminPassword = $_.AdminPassword
            Status = $_.Status
            Active = $_.Active
            DomainCount = $_.DomainCount
            Domains = @($_.Domains | Select-Object -Unique)
        }
    } | Sort-Object AdminEmail)

    if ($ShardCount -gt 1) {
        $filtered = New-Object System.Collections.Generic.List[object]
        for ($i = 0; $i -lt $targets.Count; $i++) {
            if (($i % $ShardCount) -eq $ShardIndex) { $filtered.Add($targets[$i]) | Out-Null }
        }
        $targets = @($filtered.ToArray())
    }
    if ($Limit -gt 0) { $targets = @($targets | Select-Object -First $Limit) }
    return $targets
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

function Get-TraceDetailsSafe {
    param([object]$Trace, [datetime]$StartDate, [datetime]$EndDate)
    if (-not $Trace.MessageTraceId -or -not $Trace.RecipientAddress) { return @() }
    try {
        if (Get-Command Get-MessageTraceDetailV2 -ErrorAction SilentlyContinue) {
            return @(Get-MessageTraceDetailV2 -MessageTraceId $Trace.MessageTraceId -RecipientAddress $Trace.RecipientAddress -StartDate $StartDate -EndDate $EndDate -ErrorAction Stop)
        }
        return @(Get-MessageTraceDetail -MessageTraceId $Trace.MessageTraceId -RecipientAddress $Trace.RecipientAddress -ErrorAction Stop)
    } catch {
        return @([pscustomobject]@{ error = $_.Exception.Message })
    }
}

function Get-GraphBearer {
    param([string]$AdminEmail, [string]$AdminPassword)
    $tenantDomain = $AdminEmail.Split("@", 2)[1]
    $tenantId = Get-ProbeTenantIdFromDomain -Domain $tenantDomain
    if (-not $tenantId) { throw "Could not resolve tenant id for $tenantDomain" }
    $bearer = Get-ProbeROPCToken -TenantId $tenantId -ClientId $AzureCliPublicClientId -Username $AdminEmail -Password $AdminPassword
    if (-not $bearer) { throw "Could not obtain Graph token for $AdminEmail" }
    return $bearer
}

function Get-ProbeTenantIdFromDomain {
    param([string]$Domain)
    $cleanDomain = ([string]$Domain).Trim().ToLowerInvariant()
    if (-not $cleanDomain) { return $null }

    $urls = @(
        "https://login.microsoftonline.com/$cleanDomain/v2.0/.well-known/openid-configuration",
        "https://login.microsoftonline.com/$cleanDomain/.well-known/openid-configuration"
    )
    $lastError = ""
    foreach ($url in $urls) {
        try {
            $response = Invoke-RestMethod -Uri $url -TimeoutSec 30 -ErrorAction Stop
            $text = @([string]$response.token_endpoint, [string]$response.issuer) -join "`n"
            $match = [regex]::Match($text, "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
            if ($match.Success) {
                return $match.Value
            }
        } catch {
            $lastError = $_.Exception.Message
        }
    }
    if ($lastError) {
        Write-Log "Tenant id discovery failed for ${cleanDomain}: $lastError" -Level Warning
    }
    return $null
}

function Get-ProbeROPCToken {
    param([string]$TenantId, [string]$ClientId, [string]$Username, [string]$Password, [string]$ScopeString = "https://graph.microsoft.com/.default")

    $tokenUrl = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
    $body = @{
        grant_type = "password"
        client_id = $ClientId
        scope = $ScopeString
        username = $Username
        password = $Password
    }

    $lastError = $null
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            $response = Invoke-RestMethod -Method POST -Uri $tokenUrl -Body $body -ContentType "application/x-www-form-urlencoded" -TimeoutSec 45 -ErrorAction Stop
            return $response.access_token
        } catch {
            $detail = ""
            try {
                if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
                    $detail = [string]$_.ErrorDetails.Message
                }
            } catch { }
            $lastError = (($_.Exception.Message, $detail) | Where-Object { $_ } | Select-Object -First 2) -join " :: "
            Start-Sleep -Seconds ([Math]::Min(30, 5 * $attempt))
        }
    }
    throw "ROPC token failed for ${Username}: $lastError"
}

function Invoke-ProbeGraphRequest {
    param(
        [string]$Method,
        [string]$Url,
        [string]$Bearer,
        [object]$Body = $null
    )
    $headers = @{ Authorization = "Bearer $Bearer"; "Content-Type" = "application/json" }
    if ($null -ne $Body) {
        return Invoke-RestMethod -Method $Method -Uri $Url -Headers $headers -Body ($Body | ConvertTo-Json -Depth 10) -TimeoutSec 45 -ErrorAction Stop
    }
    return Invoke-RestMethod -Method $Method -Uri $Url -Headers $headers -TimeoutSec 45 -ErrorAction Stop
}

function Get-ProbeGraphServicePrincipal {
    param([string]$Bearer, [string]$AppId)
    $filter = [uri]::EscapeDataString("appId eq '$AppId'")
    $result = Invoke-ProbeGraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/servicePrincipals?`$filter=$filter&`$select=id,appId" -Bearer $Bearer
    if ($result.value -and $result.value.Count -gt 0) { return $result.value[0] }
    return Invoke-ProbeGraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/servicePrincipals" -Bearer $Bearer -Body @{ appId = $AppId }
}

function Ensure-ProbeMailSendApp {
    param([string]$Bearer)

    $graphSp = Get-ProbeGraphServicePrincipal -Bearer $Bearer -AppId $GraphResourceAppId
    if (-not $graphSp.id) { throw "Could not resolve Microsoft Graph service principal" }

    $filter = [uri]::EscapeDataString("displayName eq '$ProbeMailSendAppName'")
    $apps = Invoke-ProbeGraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/applications?`$filter=$filter&`$select=id,appId,displayName" -Bearer $Bearer
    if ($apps.value -and $apps.value.Count -gt 0) {
        $app = $apps.value[0]
    } else {
        $app = Invoke-ProbeGraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/applications" -Bearer $Bearer -Body @{
            displayName = $ProbeMailSendAppName
            signInAudience = "AzureADMyOrg"
            isFallbackPublicClient = $true
            publicClient = @{ redirectUris = @() }
            requiredResourceAccess = @(@{
                resourceAppId = $GraphResourceAppId
                resourceAccess = @($ProbeMailSendScopes | ForEach-Object { @{ id = $_.id; type = $_.type } })
            })
        }
    }
    if (-not $app.appId) { throw "Probe Mail.Send app did not return appId" }

    $clientSp = Get-ProbeGraphServicePrincipal -Bearer $Bearer -AppId ([string]$app.appId)
    if (-not $clientSp.id) { throw "Could not resolve probe Mail.Send service principal" }

    $grantFilter = [uri]::EscapeDataString("clientId eq '$($clientSp.id)' and resourceId eq '$($graphSp.id)' and consentType eq 'AllPrincipals'")
    $grants = Invoke-ProbeGraphRequest -Method GET -Url "https://graph.microsoft.com/v1.0/oauth2PermissionGrants?`$filter=$grantFilter" -Bearer $Bearer
    $desiredScopes = @($ProbeMailSendScopes | ForEach-Object { $_.name })
    $desiredScopeText = ($desiredScopes | Sort-Object -Unique) -join " "
    if ($grants.value -and $grants.value.Count -gt 0) {
        $grant = $grants.value[0]
        $existing = @(([string]$grant.scope).Split(" ") | Where-Object { $_ })
        $combined = @(($existing + $desiredScopes) | Sort-Object -Unique) -join " "
        if ($combined -ne [string]$grant.scope) {
            Invoke-ProbeGraphRequest -Method PATCH -Url "https://graph.microsoft.com/v1.0/oauth2PermissionGrants/$($grant.id)" -Bearer $Bearer -Body @{ scope = $combined } | Out-Null
        }
    } else {
        Invoke-ProbeGraphRequest -Method POST -Url "https://graph.microsoft.com/v1.0/oauth2PermissionGrants" -Bearer $Bearer -Body @{
            clientId = $clientSp.id
            consentType = "AllPrincipals"
            resourceId = $graphSp.id
            scope = $desiredScopeText
        } | Out-Null
    }

    return [string]$app.appId
}

function Set-TestMailboxReady {
    param([string]$Email, [string]$Password, [string]$Bearer)

    $headers = @{ Authorization = "Bearer $Bearer"; "Content-Type" = "application/json" }
    $userId = $null
    for ($i = 1; $i -le 6; $i++) {
        try {
            $mbx = Get-Mailbox -Identity $Email -ErrorAction SilentlyContinue
            if ($mbx -and $mbx.ExternalDirectoryObjectId) { $userId = [string]$mbx.ExternalDirectoryObjectId; break }
        } catch { }
        Start-Sleep -Seconds 5
    }
    if (-not $userId) {
        try {
            $user = Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$Email" -Headers $headers -TimeoutSec 30 -ErrorAction Stop
            if ($user -and $user.id) { $userId = [string]$user.id }
        } catch { }
    }
    if (-not $userId) { throw "Could not resolve test mailbox user id for $Email" }

    try {
        Set-Mailbox -Identity $Email -Type Shared -MicrosoftOnlineServicesID $Email -WindowsEmailAddress $Email -Confirm:$false -ErrorAction SilentlyContinue
    } catch { }

    $body = @{
        accountEnabled = $true
        passwordProfile = @{ forceChangePasswordNextSignIn = $false; password = $Password }
    }
    Invoke-RestMethod -Method PATCH -Uri "https://graph.microsoft.com/v1.0/users/$userId" -Headers $headers -Body ($body | ConvertTo-Json -Depth 4) -TimeoutSec 30 -ErrorAction Stop | Out-Null
    try { Set-CASMailbox -Identity $Email -SmtpClientAuthenticationDisabled $false -ErrorAction SilentlyContinue } catch { }
}

function Send-GraphThresholdProbe {
    param([string]$TenantId, [string]$From, [string]$Password, [string]$Recipient, [string]$Subject, [string]$AdminBearer)

    try {
        $clientId = $AzureCliPublicClientId
        try {
            $mailToken = Get-ProbeROPCToken -TenantId $TenantId -ClientId $clientId -Username $From -Password $Password -ScopeString "Mail.Send openid profile offline_access"
        } catch {
            $tokenError = $_.Exception.Message
            if ($AdminBearer -and ($tokenError -match "AADSTS65001|AADSTS65002|consent")) {
                $clientId = Ensure-ProbeMailSendApp -Bearer $AdminBearer
                Start-Sleep -Seconds 5
                $mailToken = Get-ProbeROPCToken -TenantId $TenantId -ClientId $clientId -Username $From -Password $Password -ScopeString "Mail.Send User.Read openid profile offline_access"
            } else {
                throw
            }
        }
        $headers = @{ Authorization = "Bearer $mailToken"; "Content-Type" = "application/json" }
        $body = @{
            message = @{
                subject = $Subject
                body = @{ contentType = "Text"; content = "SI tenant threshold probe $Subject" }
                toRecipients = @(@{ emailAddress = @{ address = $Recipient } })
            }
            saveToSentItems = $true
        }
        Invoke-RestMethod -Method POST -Uri "https://graph.microsoft.com/v1.0/me/sendMail" -Headers $headers -Body ($body | ConvertTo-Json -Depth 10) -TimeoutSec 45 -ErrorAction Stop | Out-Null
        return [pscustomobject]@{ Sent = $true; Threshold = $false; Error = ""; Method = "graph_send"; Attempts = 1; Transient = $false }
    } catch {
        $text = $_.Exception.ToString()
        return [pscustomobject]@{
            Sent = $false
            Threshold = ($text -match $ThresholdPattern)
            Error = (($text -replace "\s+", " ").Trim()).Substring(0, [Math]::Min(1000, (($text -replace "\s+", " ").Trim()).Length))
            Method = "graph_send_failed"
            Attempts = 1
            Transient = ($text -match "timed out|timeout|temporarily unavailable|throttl|rate|429|503|504")
        }
    }
}

function Test-SmtpNetworkConnectivity {
    param([string]$HostName = "smtp.office365.com", [int]$Port = 587, [int]$TimeoutSeconds = 10)

    $errors = New-Object System.Collections.Generic.List[string]
    try {
        $addresses = @([System.Net.Dns]::GetHostAddresses($HostName) | Where-Object { $_.AddressFamily -eq [System.Net.Sockets.AddressFamily]::InterNetwork } | Select-Object -First 4)
        if ($addresses.Count -eq 0) {
            $addresses = @([System.Net.Dns]::GetHostAddresses($HostName) | Select-Object -First 4)
        }
    } catch {
        $text = (($_.Exception.ToString() -replace "\s+", " ").Trim())
        return [pscustomobject]@{ Ok = $false; Error = "DNS resolution failed for ${HostName}: $text" }
    }

    foreach ($address in $addresses) {
        $client = $null
        try {
            $client = [System.Net.Sockets.TcpClient]::new($address.AddressFamily)
            $async = $client.BeginConnect($address, $Port, $null, $null)
            if (-not $async.AsyncWaitHandle.WaitOne([TimeSpan]::FromSeconds($TimeoutSeconds))) {
                try { $client.Close() } catch { }
                $errors.Add("${address}: timed out connecting to ${HostName}:${Port}") | Out-Null
                continue
            }
            $client.EndConnect($async)
            return [pscustomobject]@{ Ok = $true; Error = ""; Address = [string]$address }
        } catch {
            $text = (($_.Exception.ToString() -replace "\s+", " ").Trim())
            $errors.Add("${address}: $text") | Out-Null
        } finally {
            if ($client) { try { $client.Dispose() } catch { } }
        }
    }

    $combined = (($errors.ToArray() -join " || ") -replace "\s+", " ").Trim()
    if (-not $combined) { $combined = "No SMTP endpoints could be tested for ${HostName}:${Port}" }
    return [pscustomobject]@{ Ok = $false; Error = $combined.Substring(0, [Math]::Min(1200, $combined.Length)) }
}

function Send-SmtpThresholdProbe {
    param([string]$From, [string]$Password, [string]$Recipient, [string]$Subject)

    $errors = New-Object System.Collections.Generic.List[string]
    $network = Test-SmtpNetworkConnectivity -TimeoutSeconds $SmtpTcpProbeSeconds
    if (-not $network.Ok) {
        return [pscustomobject]@{
            Sent = $false
            Threshold = $false
            Error = "SMTP network preflight failed: $($network.Error)"
            Method = "smtp_network_unreachable"
            Attempts = 0
            Transient = $true
        }
    }

    for ($attempt = 1; $attempt -le $SmtpMaxAttempts; $attempt++) {
        $message = $null
        $client = $null
        try {
            Write-Log "SMTP attempt $attempt/$SmtpMaxAttempts from $From to $Recipient via $($network.Address)" -Level Info
            if ($attempt -gt 1) {
                $delay = [Math]::Min(180, ($SmtpRetryBaseSeconds * [Math]::Pow(2, $attempt - 2)) + (Get-Random -Minimum 3 -Maximum 18))
                Start-Sleep -Seconds ([int]$delay)
            } else {
                Start-Sleep -Seconds (Get-Random -Minimum 2 -Maximum 10)
            }

            $message = [System.Net.Mail.MailMessage]::new($From, $Recipient)
            $message.Subject = $Subject
            $message.Body = "SI tenant threshold probe $Subject"
            $message.Headers.Add("X-SI-Threshold-Probe", $RunId)

            $client = [System.Net.Mail.SmtpClient]::new("smtp.office365.com", 587)
            $client.EnableSsl = $true
            $client.DeliveryMethod = [System.Net.Mail.SmtpDeliveryMethod]::Network
            $client.UseDefaultCredentials = $false
            $client.Credentials = [System.Net.NetworkCredential]::new($From, $Password)
            $client.Timeout = [Math]::Max(15000, $SmtpTimeoutSeconds * 1000)
            $client.Send($message)

            return [pscustomobject]@{
                Sent = $true
                Threshold = $false
                Error = ""
                Method = "smtp_send"
                Attempts = $attempt
                Transient = $false
            }
        } catch {
            $text = (($_.Exception.ToString() -replace "\s+", " ").Trim())
            $short = $text.Substring(0, [Math]::Min(1000, $text.Length))
            $errors.Add("attempt ${attempt}: $short") | Out-Null

            if ($text -match $ThresholdPattern) {
                return [pscustomobject]@{
                    Sent = $false
                    Threshold = $true
                    Error = $short
                    Method = "smtp_threshold_rejected"
                    Attempts = $attempt
                    Transient = $false
                }
            }

            $isPermanentAuth = ($text -match $SmtpPermanentAuthPattern)
            $isTransient = ($text -match $SmtpTransientPattern)
            if ($isPermanentAuth -and -not $isTransient) {
                return [pscustomobject]@{
                    Sent = $false
                    Threshold = $false
                    Error = $short
                    Method = "smtp_auth_or_policy_failed"
                    Attempts = $attempt
                    Transient = $false
                }
            }

            Write-Log "SMTP attempt $attempt failed for ${From}: $short" -Level Warning
        } finally {
            if ($message) { $message.Dispose() }
            if ($client) { $client.Dispose() }
        }
    }

    $combinedError = (($errors.ToArray() -join " || ") -replace "\s+", " ").Trim()
    return [pscustomobject]@{
        Sent = $false
        Threshold = ($combinedError -match $ThresholdPattern)
        Error = $combinedError.Substring(0, [Math]::Min(1600, $combinedError.Length))
        Method = "smtp_send_failed_after_retries"
        Attempts = $SmtpMaxAttempts
        Transient = ($combinedError -match $SmtpTransientPattern)
    }
}

function Send-ThresholdProbe {
    param([string]$TenantId, [string]$From, [string]$Password, [string]$Recipient, [string]$Subject, [string]$AdminBearer)

    if ($SendTransport -eq "Graph") {
        return Send-GraphThresholdProbe -TenantId $TenantId -From $From -Password $Password -Recipient $Recipient -Subject $Subject -AdminBearer $AdminBearer
    }
    if ($SendTransport -eq "Smtp") {
        return Send-SmtpThresholdProbe -From $From -Password $Password -Recipient $Recipient -Subject $Subject
    }

    $graph = Send-GraphThresholdProbe -TenantId $TenantId -From $From -Password $Password -Recipient $Recipient -Subject $Subject -AdminBearer $AdminBearer
    if ($graph.Sent -or $graph.Threshold) { return $graph }
    return Send-SmtpThresholdProbe -From $From -Password $Password -Recipient $Recipient -Subject $Subject
}

function Read-ProbeTrace {
    param([string]$Sender, [string]$Recipient, [string]$Subject, [datetime]$StartDate)

    $endDate = (Get-Date).ToUniversalTime().AddMinutes(5)
    if (Get-Command Get-MessageTraceV2 -ErrorAction SilentlyContinue) {
        $traces = @(Get-MessageTraceV2 -StartDate $StartDate -EndDate $endDate -ResultSize 1000 -ErrorAction Stop)
    } else {
        $traces = @(Get-MessageTrace -StartDate $StartDate -EndDate $endDate -PageSize 1000 -Page 1 -WarningAction SilentlyContinue -ErrorAction Stop)
    }

    $probeTraces = @($traces | Where-Object {
        ([string]$_.SenderAddress).Trim().ToLowerInvariant() -eq $Sender.ToLowerInvariant() -and
        ([string]$_.RecipientAddress).Trim().ToLowerInvariant() -eq $Recipient.ToLowerInvariant() -and
        ([string]$_.Subject).Trim() -eq $Subject
    } | Sort-Object Received -Descending)

    foreach ($trace in $probeTraces) {
        $traceText = ConvertTo-TraceText $trace
        $details = @(Get-TraceDetailsSafe -Trace $trace -StartDate $StartDate -EndDate $endDate)
        $detailText = (($details | ForEach-Object { ConvertTo-TraceText $_ }) -join "`n")
        $combined = "$traceText`n$detailText"
        if ($combined -match $ThresholdPattern) {
            return [pscustomobject]@{
                Found = $true
                HasThreshold = $true
                Status = [string]$trace.Status
                Evidence = (($combined -replace "\s+", " ").Trim()).Substring(0, [Math]::Min(1600, (($combined -replace "\s+", " ").Trim()).Length))
            }
        }
    }

    if ($probeTraces.Count -gt 0) {
        return [pscustomobject]@{
            Found = $true
            HasThreshold = $false
            Status = [string]$probeTraces[0].Status
            Evidence = ""
        }
    }

    return [pscustomobject]@{ Found = $false; HasThreshold = $false; Status = ""; Evidence = "" }
}

function Update-AdminStatus {
    param([string]$AdminId, [string]$Status)
    if (-not ([string]$AdminId).Trim()) { return }
    $body = @{ status = $Status; updated_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ") }
    Invoke-SupabaseApi -Method PATCH -Table "admin_credentials" -Query "id=eq.$AdminId" -Body $body | Out-Null
}

function Update-AdminThresholdCheck {
    param([object]$Result)
    $adminId = ([string]$Result.admin_id).Trim()
    if (-not $adminId) { return }

    $completedAt = ([string]$Result.completed_at).Trim()
    if (-not $completedAt) { $completedAt = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ") }

    $body = @{
        last_threshold_checked_at = $completedAt
        last_threshold_check_result = [string]$Result.result
        last_threshold_check_method = [string]$Result.method
        last_threshold_check_trace_status = [string]$Result.trace_status
        last_threshold_check_evidence = [string]$Result.evidence
        last_threshold_check_error = [string]$Result.error
        last_threshold_probe_mailbox = [string]$Result.test_mailbox
        updated_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    }

    $domainCount = 0
    if ([int]::TryParse([string]$Result.domain_count, [ref]$domainCount)) {
        $body.threshold_check_domain_count = $domainCount
    }

    $activeInboxCount = 0
    if ([int]::TryParse([string]$Result.active_inbox_count, [ref]$activeInboxCount)) {
        $body.threshold_check_active_inbox_count = $activeInboxCount
    }

    if (([string]$Result.result).Trim().ToLowerInvariant() -eq "threshold") {
        $body.status = "Threshold Exceeded"
    }

    $updateResult = Invoke-SupabaseApi -Method PATCH -Table "admin_credentials" -Query "id=eq.$adminId" -Body $body
    if (-not $updateResult.Success) {
        Write-Log "Threshold result was captured in output files, but app-side tracking update failed for ${adminId}: $($updateResult.Error)" -Level Warning
    }
}

function Connect-ExchangeOnlineWithRetry {
    param([System.Management.Automation.PSCredential]$Credential, [string]$AdminEmail)

    $lastError = $null
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            Connect-ExchangeOnline -Credential $Credential -ShowBanner:$false -ErrorAction Stop
            return $true
        } catch {
            $lastError = $_.Exception.Message
            Write-Log "Exchange connect attempt $attempt failed for ${AdminEmail}: $lastError" -Level Warning
            try { Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue | Out-Null } catch { }
            Start-Sleep -Seconds ([Math]::Min(90, 15 * $attempt))
        }
    }
    throw "Exchange connect failed after retries for ${AdminEmail}: $lastError"
}

function Test-TargetTenant {
    param([object]$Target)

    $startedAt = (Get-Date).ToUniversalTime()
    if (([string]$Target.Status).Trim().ToLowerInvariant() -eq "threshold exceeded" -and -not $IncludeKnownThreshold) {
        return [pscustomobject]@{
            admin_id = $Target.AdminId; admin_email = $Target.AdminEmail; domain_count = $Target.DomainCount
            result = "threshold"; method = "already_marked_threshold"; test_mailbox = ""; smtp_sent = $false
            smtp_attempts = 0; smtp_transient = $false; trace_status = ""; evidence = ""; error = ""; started_at = $startedAt.ToString("o"); completed_at = (Get-Date).ToUniversalTime().ToString("o")
        }
    }

    $tenantDomain = $Target.AdminEmail.Split("@", 2)[1].ToLowerInvariant()
    $safe = (($Target.AdminEmail.Split("@", 2)[1] -replace "\.onmicrosoft\.com$", "") -replace "[^a-zA-Z0-9]", "")
    if ($safe.Length -gt 18) { $safe = $safe.Substring(0, 18) }
    $testLocal = "si-test-$($RunId.ToLowerInvariant().Replace('z',''))-$ShardIndex"
    if ($testLocal.Length -gt 45) { $testLocal = $testLocal.Substring(0, 45) }
    $testEmail = "$testLocal@$tenantDomain"
    $testPassword = "SIProbe!" + (Get-Random -Minimum 10000000 -Maximum 99999999) + "aA"
    $subject = "SI-TENANT-CHECK-$RunId-$ShardIndex-$safe"

    $securePwd = ConvertTo-SecureString ([string]$Target.AdminPassword) -AsPlainText -Force
    $adminCreds = [System.Management.Automation.PSCredential]::new([string]$Target.AdminEmail, $securePwd)

    $connected = $false
    try {
        Connect-ExchangeOnlineWithRetry -Credential $adminCreds -AdminEmail $Target.AdminEmail | Out-Null
        $connected = $true
        $tenantId = Get-ProbeTenantIdFromDomain -Domain $tenantDomain
        if (-not $tenantId) { throw "Could not resolve tenant id for $tenantDomain" }
        $bearer = Get-ProbeROPCToken -TenantId $tenantId -ClientId $AzureCliPublicClientId -Username $Target.AdminEmail -Password $Target.AdminPassword

        try { Set-TransportConfig -SmtpClientAuthenticationDisabled $false -ErrorAction SilentlyContinue } catch { }

        $existing = Get-Mailbox -Identity $testEmail -ErrorAction SilentlyContinue
        if (-not $existing) {
            $probeSuffix = "$($RunId)-$($ShardIndex)"
            if ($probeSuffix.Length -gt 42) { $probeSuffix = $probeSuffix.Substring(0, 42) }
            $displayName = "SI Threshold Probe $probeSuffix"
            $secureMailboxPwd = ConvertTo-SecureString $testPassword -AsPlainText -Force
            New-Mailbox -Shared -Name $displayName -DisplayName $displayName -PrimarySmtpAddress $testEmail -Password $secureMailboxPwd -ResetPasswordOnNextLogon $false -ErrorAction Stop | Out-Null
            Start-Sleep -Seconds 12
        }

        Set-TestMailboxReady -Email $testEmail -Password $testPassword -Bearer $bearer
        Start-Sleep -Seconds 12

        $sendStart = (Get-Date).ToUniversalTime().AddMinutes(-2)
        $smtp = Send-ThresholdProbe -TenantId $tenantId -From $testEmail -Password $testPassword -Recipient $Recipient -Subject $subject -AdminBearer $bearer
        if ($smtp.Threshold) {
            Update-AdminStatus -AdminId $Target.AdminId -Status "Threshold Exceeded"
            return [pscustomobject]@{
                admin_id = $Target.AdminId; admin_email = $Target.AdminEmail; domain_count = $Target.DomainCount
                result = "threshold"; method = $smtp.Method; test_mailbox = $testEmail; smtp_sent = $false
                smtp_attempts = $smtp.Attempts; smtp_transient = $smtp.Transient; trace_status = ""; evidence = $smtp.Error; error = ""; started_at = $startedAt.ToString("o"); completed_at = (Get-Date).ToUniversalTime().ToString("o")
            }
        }
        if (-not $smtp.Sent) {
            return [pscustomobject]@{
                admin_id = $Target.AdminId; admin_email = $Target.AdminEmail; domain_count = $Target.DomainCount
                result = "unknown"; method = $smtp.Method; test_mailbox = $testEmail; smtp_sent = $false
                smtp_attempts = $smtp.Attempts; smtp_transient = $smtp.Transient; trace_status = ""; evidence = ""; error = $smtp.Error; started_at = $startedAt.ToString("o"); completed_at = (Get-Date).ToUniversalTime().ToString("o")
            }
        }

        $traceResult = $null
        for ($i = 1; $i -le $TracePolls; $i++) {
            Start-Sleep -Seconds $TracePollSeconds
            $traceResult = Read-ProbeTrace -Sender $testEmail -Recipient $Recipient -Subject $subject -StartDate $sendStart
            if ($traceResult.Found) { break }
        }

        if ($traceResult -and $traceResult.Found -and $traceResult.HasThreshold) {
            Update-AdminStatus -AdminId $Target.AdminId -Status "Threshold Exceeded"
            return [pscustomobject]@{
                admin_id = $Target.AdminId; admin_email = $Target.AdminEmail; domain_count = $Target.DomainCount
                result = "threshold"; method = "message_trace_detail"; test_mailbox = $testEmail; smtp_sent = $true
                smtp_attempts = $smtp.Attempts; smtp_transient = $smtp.Transient; trace_status = $traceResult.Status; evidence = $traceResult.Evidence; error = ""; started_at = $startedAt.ToString("o"); completed_at = (Get-Date).ToUniversalTime().ToString("o")
            }
        }

        if ($traceResult -and $traceResult.Found) {
            return [pscustomobject]@{
                admin_id = $Target.AdminId; admin_email = $Target.AdminEmail; domain_count = $Target.DomainCount
                result = "clean"; method = "$($smtp.Method)_and_trace"; test_mailbox = $testEmail; smtp_sent = $true
                smtp_attempts = $smtp.Attempts; smtp_transient = $smtp.Transient; trace_status = $traceResult.Status; evidence = ""; error = ""; started_at = $startedAt.ToString("o"); completed_at = (Get-Date).ToUniversalTime().ToString("o")
            }
        }

        return [pscustomobject]@{
            admin_id = $Target.AdminId; admin_email = $Target.AdminEmail; domain_count = $Target.DomainCount
            result = "unknown"; method = "trace_missing_after_$($smtp.Method)_success"; test_mailbox = $testEmail; smtp_sent = $true
            smtp_attempts = $smtp.Attempts; smtp_transient = $smtp.Transient; trace_status = ""; evidence = ""; error = "Probe send succeeded but message trace did not surface within poll window"; started_at = $startedAt.ToString("o"); completed_at = (Get-Date).ToUniversalTime().ToString("o")
        }
    } catch {
        return [pscustomobject]@{
            admin_id = $Target.AdminId; admin_email = $Target.AdminEmail; domain_count = $Target.DomainCount
            result = "unknown"; method = "exception"; test_mailbox = $testEmail; smtp_sent = $false
            smtp_attempts = 0; smtp_transient = $false; trace_status = ""; evidence = ""; error = ($_.Exception.Message.Substring(0, [Math]::Min(1000, $_.Exception.Message.Length))); started_at = $startedAt.ToString("o"); completed_at = (Get-Date).ToUniversalTime().ToString("o")
        }
    } finally {
        if ($connected) {
            if (-not $NoCleanup -and $testEmail) {
                try { Remove-Mailbox -Identity $testEmail -Confirm:$false -ErrorAction SilentlyContinue | Out-Null } catch { }
            }
            Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
        }
    }
}

if (-not (Get-Module -ListAvailable -Name ExchangeOnlineManagement)) {
    Install-Module -Name ExchangeOnlineManagement -Force -AllowClobber -Scope CurrentUser
}
Import-Module ExchangeOnlineManagement -ErrorAction Stop

$allTargets = @(Get-TenantTargets)
if ($SkipAdminEmail -and $SkipAdminEmail.Count -gt 0) {
    $skipSet = @{}
    foreach ($email in $SkipAdminEmail) {
        $normalizedSkip = ([string]$email).Trim().ToLowerInvariant()
        if ($normalizedSkip) { $skipSet[$normalizedSkip] = $true }
    }
    $allTargets = @($allTargets | Where-Object { -not $skipSet.ContainsKey(([string]$_.AdminEmail).Trim().ToLowerInvariant()) })
}
$targetLabel = if ($CredentialCsv) { "candidate credential tenant(s)" } elseif ($AllActiveMicrosoftTenants) { "active Microsoft tenant(s)" } else { "destination tenant(s)" }
Write-Log "Active threshold test shard ${ShardIndex}/${ShardCount}: $($allTargets.Count) $targetLabel, recipient=$Recipient, run=$RunId" -Level Info

$results = New-Object System.Collections.Generic.List[object]
$index = 0
foreach ($target in $allTargets) {
    $index += 1
    Write-Log "[$index/$($allTargets.Count)] Testing $($target.AdminEmail) ($($target.DomainCount) moved domain(s))" -Level Info
    $result = Test-TargetTenant -Target $target
    Update-AdminThresholdCheck -Result $result
    $results.Add($result) | Out-Null
    Write-Jsonl -Payload $result
    $results | Export-Csv -Path $OutputCsv -NoTypeInformation

    $level = switch ($result.result) {
        "clean" { "Success" }
        "threshold" { "Error" }
        default { "Warning" }
    }
    Write-Log "[$index/$($allTargets.Count)] $($target.AdminEmail): $($result.result) via $($result.method)" -Level $level
}

$results | Export-Csv -Path $OutputCsv -NoTypeInformation
$clean = @($results | Where-Object { $_.result -eq "clean" }).Count
$threshold = @($results | Where-Object { $_.result -eq "threshold" }).Count
$unknown = @($results | Where-Object { $_.result -eq "unknown" }).Count
Write-Log "Active threshold test complete. clean=$clean threshold=$threshold unknown=$unknown output=$OutputCsv jsonl=$OutputJsonl" -Level Success
