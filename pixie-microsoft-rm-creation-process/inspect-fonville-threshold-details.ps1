param(
    [string]$AdminEmail = "admin@fonvillebargainincpvtmoinf.onmicrosoft.com",
    [string[]]$Domains = @("acquisitionadvantage.info", "conversionadvantage.info"),
    [int]$HoursBack = 24,
    [int]$SampleSize = 5,
    [string]$OutputJson = (Join-Path $PSScriptRoot "logs/fonville-threshold-message-trace-details.json")
)

$ErrorActionPreference = "Stop"

. "$PSScriptRoot/config.ps1"

function Get-EmailDomain {
    param([string]$Email)
    $value = ([string]$Email).Trim().ToLowerInvariant()
    $at = $value.LastIndexOf("@")
    if ($at -lt 0 -or $at -eq ($value.Length - 1)) { return "" }
    return $value.Substring($at + 1)
}

function Convert-ObjectToHashtable {
    param([object]$Object)
    $hash = [ordered]@{}
    if (-not $Object) { return $hash }
    foreach ($property in $Object.PSObject.Properties) {
        $value = $property.Value
        if ($value -is [datetime]) { $value = $value.ToString("o") }
        $hash[$property.Name] = $value
    }
    return $hash
}

function Get-TraceDetailsSafe {
    param([object]$Trace)
    if (-not $Trace.MessageTraceId -or -not $Trace.RecipientAddress) { return @() }
    try {
        return @(Get-MessageTraceDetail -MessageTraceId $Trace.MessageTraceId -RecipientAddress $Trace.RecipientAddress -ErrorAction Stop)
    } catch {
        return @([pscustomobject]@{ error = $_.Exception.Message })
    }
}

if (-not (Get-Module -ListAvailable -Name ExchangeOnlineManagement)) {
    Install-Module -Name ExchangeOnlineManagement -Force -AllowClobber -Scope CurrentUser
}
Import-Module ExchangeOnlineManagement -ErrorAction Stop

$admin = Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query "email=eq.$AdminEmail&select=id,email,password&limit=1"
if (-not $admin.Success -or -not $admin.Data -or $admin.Data.Count -eq 0) {
    throw "Admin credential not found: $AdminEmail"
}

$securePwd = ConvertTo-SecureString ([string]$admin.Data[0].password) -AsPlainText -Force
$creds = New-Object System.Management.Automation.PSCredential(([string]$admin.Data[0].email), $securePwd)
$startDate = (Get-Date).ToUniversalTime().AddHours(-1 * [Math]::Max(1, $HoursBack))
$endDate = (Get-Date).ToUniversalTime()
$domainSet = @{}
foreach ($domain in $Domains) { $domainSet[$domain.Trim().ToLowerInvariant()] = $true }

Connect-ExchangeOnline -Credential $creds -ShowBanner:$false -ErrorAction Stop
try {
    $commands = @(Get-Command *MessageTrace* | Select-Object -ExpandProperty Name | Sort-Object -Unique)
    $traces = @(Get-MessageTraceV2 -StartDate $startDate -EndDate $endDate -ResultSize 1000 -ErrorAction Stop)
    $outbound = @($traces | Where-Object {
        $senderDomain = Get-EmailDomain ([string]$_.SenderAddress)
        $domainSet.ContainsKey($senderDomain)
    } | Sort-Object Received -Descending)
    $failedOutbound = @($outbound | Where-Object { ([string]$_.Status).Trim() -eq "Failed" } | Select-Object -First $SampleSize)

    $senderSet = @{}
    foreach ($trace in $outbound) {
        $sender = ([string]$trace.SenderAddress).Trim().ToLowerInvariant()
        if ($sender) { $senderSet[$sender] = $true }
    }
    $bounceRows = @($traces | Where-Object {
        $recipient = ([string]$_.RecipientAddress).Trim().ToLowerInvariant()
        $sender = ([string]$_.SenderAddress).Trim().ToLowerInvariant()
        $senderSet.ContainsKey($recipient) -and ($sender -like "microsoftexchange*" -or ([string]$_.Subject) -like "Undeliverable:*")
    } | Sort-Object Received -Descending | Select-Object -First $SampleSize)

    $failedSamples = @()
    foreach ($trace in $failedOutbound) {
        $failedSamples += [ordered]@{
            trace = Convert-ObjectToHashtable $trace
            details = @(Get-TraceDetailsSafe $trace | ForEach-Object { Convert-ObjectToHashtable $_ })
        }
    }

    $bounceSamples = @()
    foreach ($trace in $bounceRows) {
        $bounceSamples += [ordered]@{
            trace = Convert-ObjectToHashtable $trace
            details = @(Get-TraceDetailsSafe $trace | ForEach-Object { Convert-ObjectToHashtable $_ })
        }
    }

    $result = [ordered]@{
        adminEmail = $AdminEmail
        domains = $Domains
        startDate = $startDate.ToString("o")
        endDate = $endDate.ToString("o")
        availableMessageTraceCommands = $commands
        totalTraceRows = $traces.Count
        outboundRows = $outbound.Count
        failedOutboundRows = @($outbound | Where-Object { ([string]$_.Status).Trim() -eq "Failed" }).Count
        failedOutboundSamples = $failedSamples
        ndrBounceRowsToTenantSenders = $bounceRows.Count
        ndrBounceSamples = $bounceSamples
    }

    $json = $result | ConvertTo-Json -Depth 12
    $json | Set-Content -Path $OutputJson -Encoding UTF8
    $json
} finally {
    Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
}
