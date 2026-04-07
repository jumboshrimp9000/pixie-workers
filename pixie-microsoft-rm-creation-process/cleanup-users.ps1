. (Join-Path $PSScriptRoot "config.ps1")

$admin = (Invoke-SupabaseApi -Method GET -Table "admin_credentials" -Query "active=eq.true&limit=1").Data[0]

# Get Graph token
$adminDomain = ($admin.email -split "@")[1]
$wellKnown = Invoke-RestMethod -Uri "https://login.microsoftonline.com/$adminDomain/v2.0/.well-known/openid-configuration"
$tenantId = ($wellKnown.issuer -split "/")[3]

$tokenBody = "grant_type=password&client_id=04b07795-8ddb-461a-bbee-02f9e1bf7b46&scope=https://graph.microsoft.com/.default&username=$($admin.email)&password=$($admin.password)"
$tokenResp = Invoke-RestMethod -Method POST -Uri "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token" -Body $tokenBody -ContentType "application/x-www-form-urlencoded"
$headers = @{ Authorization = "Bearer $($tokenResp.access_token)"; "Content-Type" = "application/json" }

# Get all users on test domains
$users = (Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users?`$select=id,userPrincipalName,displayName&`$top=100" -Headers $headers).value

$testDomains = @("testrmboxes2026.xyz", "testinboxflow2026.xyz")
foreach ($u in $users) {
    foreach ($td in $testDomains) {
        if ($u.userPrincipalName -like "*@$td") {
            Write-Host "Deleting: $($u.userPrincipalName) ($($u.displayName))"
            try {
                Invoke-RestMethod -Uri "https://graph.microsoft.com/v1.0/users/$($u.id)" -Method DELETE -Headers $headers -ErrorAction Stop
                Write-Host "  Deleted OK"
            } catch {
                Write-Host "  Failed: $($_.Exception.Message)"
            }
        }
    }
}

Write-Host "`nCleanup complete"
