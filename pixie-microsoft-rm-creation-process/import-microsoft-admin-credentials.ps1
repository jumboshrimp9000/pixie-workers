<#
.SYNOPSIS
    Safely imports Microsoft admin credentials into the SimpleInboxes admin pool.
.DESCRIPTION
    One-off operational importer for vendor/admin inventory reconciliation.
    It reads a local CSV/TSV/text file containing admin email + password pairs,
    masks all password logging, and defaults to dry-run. Missing admins are
    inserted as active=false unless -ActivateNew is explicitly supplied, so
    unverified tenants are not picked up by live workers before threshold checks.
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$InputPath,
    [switch]$Live,
    [switch]$ActivateNew,
    [switch]$UpdateExistingPasswords,
    [string]$ReadinessCsv = "",
    [switch]$AllowUnverifiedImport,
    [string]$ConfirmText = "",
    [string]$OutputCsv = "",
    [string]$LogDir = (Join-Path $PSScriptRoot "logs")
)

$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "config.ps1")

function Normalize-AdminEmail {
    param([string]$Value)
    return ([string]$Value).Trim().ToLowerInvariant()
}

function Read-InputCredentialRows {
    param([string]$Path)

    if (-not (Test-Path $Path)) { throw "Input file not found: $Path" }

    $rows = New-Object System.Collections.Generic.List[object]
    $extension = [System.IO.Path]::GetExtension($Path).ToLowerInvariant()

    if ($extension -eq ".csv") {
        foreach ($row in @(Import-Csv -Path $Path)) {
            $email = ""
            $password = ""
            foreach ($name in @("email", "admin", "adminemail", "admin_email", "microsoft email", "full onmicrosoft email")) {
                $prop = $row.PSObject.Properties | Where-Object { $_.Name.Trim().ToLowerInvariant() -eq $name } | Select-Object -First 1
                if ($prop -and $prop.Value) { $email = [string]$prop.Value; break }
            }
            foreach ($name in @("password", "admin password", "admin_password", "microsoft password")) {
                $prop = $row.PSObject.Properties | Where-Object { $_.Name.Trim().ToLowerInvariant() -eq $name } | Select-Object -First 1
                if ($prop -and $prop.Value) { $password = [string]$prop.Value; break }
            }
            if ($email -or $password) {
                $rows.Add([pscustomobject]@{ Email = $email; Password = $password }) | Out-Null
            }
        }
        return @($rows.ToArray())
    }

    $emailPattern = "admin@[A-Za-z0-9._%+-]+\.onmicrosoft\.com"
    foreach ($line in @(Get-Content -Path $Path)) {
        $text = ([string]$line).Trim()
        if (-not $text) { continue }
        $match = [regex]::Match($text, $emailPattern)
        if (-not $match.Success) { continue }
        $email = $match.Value
        $tail = $text.Substring($match.Index + $match.Length).Trim()
        $password = ""
        if ($tail) {
            $parts = @($tail -split "\s+")
            if ($parts.Count -gt 0) { $password = [string]$parts[0] }
        }
        $rows.Add([pscustomobject]@{ Email = $email; Password = $password }) | Out-Null
    }
    return @($rows.ToArray())
}

function Get-ExistingMicrosoftAdminsByEmail {
    $result = Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query "provider=eq.microsoft&select=id,email,status,active,usage_count,locked_by_action_id,locked_domain_id,lock_expires_at&limit=5000"
    if (-not $result.Success) { throw "Failed to load existing Microsoft admins: $($result.Error)" }

    $map = @{}
    foreach ($row in @($result.Data)) {
        $email = Normalize-AdminEmail ([string]$row.email)
        if ($email) { $map[$email] = $row }
    }
    return $map
}

function Write-Report {
    param([object[]]$Rows, [string]$Path)
    $Rows | Export-Csv -Path $Path -NoTypeInformation
}

function Get-ReadyToInsertEmails {
    param([string]$Path)
    if (-not (Test-Path $Path)) { throw "Readiness CSV not found: $Path" }
    $ready = @{}
    foreach ($row in @(Import-Csv -Path $Path)) {
        $email = Normalize-AdminEmail ([string]$row.email)
        if (-not $email) { $email = Normalize-AdminEmail ([string]$row.admin_email) }
        $stage = ([string]$row.move_stage).Trim().ToLowerInvariant()
        $eligible = ([string]$row.eligible_for_insert).Trim().ToLowerInvariant()
        if ($email -and ($stage -eq "ready_to_insert" -or $eligible -in @("true", "1", "yes", "y"))) {
            $ready[$email] = $true
        }
    }
    return $ready
}

if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}

$inputRows = @(Read-InputCredentialRows -Path $InputPath)
$byEmail = @{}
$conflicts = New-Object System.Collections.Generic.List[string]
$blankPasswords = New-Object System.Collections.Generic.List[string]

foreach ($row in $inputRows) {
    $email = Normalize-AdminEmail ([string]$row.Email)
    $password = [string]$row.Password
    if (-not $email) { continue }
    if ($email -notmatch "^admin@[a-z0-9._%+-]+\.onmicrosoft\.com$") {
        throw "Invalid Microsoft admin email in input: $email"
    }
    if (-not $password) {
        $blankPasswords.Add($email) | Out-Null
        continue
    }
    if ($byEmail.ContainsKey($email) -and [string]$byEmail[$email] -ne $password) {
        $conflicts.Add($email) | Out-Null
        continue
    }
    $byEmail[$email] = $password
}

if ($conflicts.Count -gt 0) {
    throw "Input contains conflicting passwords for $($conflicts.Count) admin(s); first conflict: $($conflicts[0])"
}

$existing = Get-ExistingMicrosoftAdminsByEmail
$readyEmails = $null
if ($ReadinessCsv) {
    $readyEmails = Get-ReadyToInsertEmails -Path $ReadinessCsv
}

if ($Live -and -not $ReadinessCsv -and -not $AllowUnverifiedImport) {
    throw "Live import requires -ReadinessCsv from Build-SIAdminCandidateMovePlan.ps1, or explicit -AllowUnverifiedImport."
}

$reportRows = New-Object System.Collections.Generic.List[object]
$missing = New-Object System.Collections.Generic.List[string]
$existingRows = New-Object System.Collections.Generic.List[string]

foreach ($email in @($byEmail.Keys | Sort-Object)) {
    $readinessStatus = ""
    $readyForInsert = $true
    if ($null -ne $readyEmails) {
        $readyForInsert = $readyEmails.ContainsKey($email)
        $readinessStatus = if ($readyForInsert) { "ready_to_insert" } else { "not_ready_to_insert" }
    }

    if ($existing.ContainsKey($email)) {
        $current = $existing[$email]
        $existingRows.Add($email) | Out-Null
        $reportRows.Add([pscustomobject]@{
            email = $email
            readiness_status = $readinessStatus
            action = if ($UpdateExistingPasswords -and $readyForInsert) { "update_existing_password" } elseif ($UpdateExistingPasswords) { "blocked_not_ready_to_insert" } else { "already_exists_no_password_update" }
            existing_status = [string]$current.status
            existing_active = [string]$current.active
            inserted_active = ""
            live_applied = "False"
            error = ""
        }) | Out-Null
    } else {
        if ($readyForInsert) { $missing.Add($email) | Out-Null }
        $reportRows.Add([pscustomobject]@{
            email = $email
            readiness_status = $readinessStatus
            action = if ($readyForInsert) { "insert_missing" } else { "blocked_not_ready_to_insert" }
            existing_status = ""
            existing_active = ""
            inserted_active = [string]$ActivateNew.IsPresent
            live_applied = "False"
            error = ""
        }) | Out-Null
    }
}

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
if (-not $OutputCsv) {
    $mode = if ($Live) { "live" } else { "dryrun" }
    $OutputCsv = Join-Path $LogDir "microsoft-admin-import-$mode-$stamp.csv"
}

Write-Log "Credential input parsed: rows=$($inputRows.Count), unique=$($byEmail.Count), blank_passwords=$($blankPasswords.Count), existing=$($existingRows.Count), missing=$($missing.Count)" -Level Info
Write-Log "New admins will be inserted active=$($ActivateNew.IsPresent). Existing password update=$($UpdateExistingPasswords.IsPresent)." -Level Warning

if ($Live) {
    $expected = "IMPORT MICROSOFT ADMINS"
    if ($ConfirmText -ne $expected) {
        throw "Live import requires ConfirmText exactly: $expected"
    }

    foreach ($email in @($missing | Sort-Object)) {
        $body = @{
            provider = "microsoft"
            email = $email
            password = [string]$byEmail[$email]
            active = [bool]$ActivateNew.IsPresent
            status = "Active"
            usage_count = 0
        }
        $result = Invoke-SupabaseApi -Method POST -Table "admin_credentials" -Body $body
        $row = $reportRows | Where-Object { $_.email -eq $email } | Select-Object -First 1
        if ($result.Success) {
            $row.live_applied = "True"
        } else {
            $row.error = [string]$result.Error
        }
    }

    if ($UpdateExistingPasswords) {
        foreach ($email in @($existingRows | Sort-Object)) {
            if ($null -ne $readyEmails -and -not $readyEmails.ContainsKey($email)) { continue }
            $admin = $existing[$email]
            $body = @{
                password = [string]$byEmail[$email]
            }
            $result = Invoke-SupabaseApi -Method PATCH -Table "admin_credentials" -Query "id=eq.$($admin.id)" -Body $body
            $row = $reportRows | Where-Object { $_.email -eq $email } | Select-Object -First 1
            if ($result.Success) {
                $row.live_applied = "True"
            } else {
                $row.error = [string]$result.Error
            }
        }
    }
}

Write-Report -Rows @($reportRows.ToArray()) -Path $OutputCsv

$applied = @($reportRows | Where-Object { $_.live_applied -eq "True" }).Count
$errors = @($reportRows | Where-Object { $_.error }).Count
Write-Log "Import reconcile complete. Live=$($Live.IsPresent) applied=$applied errors=$errors report=$OutputCsv" -Level Success
