# Microsoft Ops Checks

This is the operator-facing checklist for the one-off Microsoft/Simple Inboxes checks we keep reusing.

The goal is to separate:

- what we are trying to prove
- which script or API proves it
- whether the check is passive/read-only or an active send/probe
- where the output artifacts should be saved

## Where the knowledge lives today

There are three kinds of saved knowledge:

1. Reusable scripts
   - Folder: `/Users/omermullick/Downloads/Projects/job-workers/pixie-microsoft-rm-creation-process/`
2. Audit outputs
   - Local: `/Users/omermullick/Downloads/Projects/admin-order-audit/`
   - Server: `/opt/pixie/AP/backend/logs/admin-order-audit/`
3. Migration learnings
   - `/Users/omermullick/Downloads/Projects/job-workers/pixie-microsoft-rm-creation-process/PROFITPATH_AZURE_BULK_LEARNINGS.md`

## Standard output rule

Every one-off check should save:

- a machine-readable JSON file
- a CSV when the result is row-based
- a short summary JSON

Preferred locations:

- local copied artifacts: `/Users/omermullick/Downloads/Projects/admin-order-audit/`
- server-side raw run artifacts: `/opt/pixie/AP/backend/logs/admin-order-audit/`

## Check catalog

### 1. Does this admin have a license?

Use when:
- deciding whether a Microsoft admin is usable for SI pool import
- checking whether a tenant is still healthy enough for mailbox work

Source of truth:
- Microsoft Graph `/me/licenseDetails`
- fallback: Graph `assignedLicenses`

Current implementation:
- ad hoc Python/Graph audit
- related helper scripts in this folder:
  - `check-licenses.ts`

Notes:
- this is read-only
- this does not test sendability

### 2. Does this tenant still have any custom domains on it?

Use when:
- deciding whether an admin is safe to move into the reusable SI admin pool
- checking whether a snapshot/live Airtable tenant mapping is still true

Source of truth:
- Microsoft Graph `/domains`

Rule:
- `*.onmicrosoft.com` does not count as a custom domain
- any other verified domain means the tenant is still carrying real domain state

Current implementation:
- ad hoc Python/Graph audit
- domain ownership checks during panel/admin audits

Notes:
- this is read-only
- this is the right proof for "no domains"

### 3. Is this admin flagged with Threshold Exceeded in our database?

Use when:
- deciding whether an admin can be leased for Simple Inboxes work
- excluding threshold-hit tenants from reconnect/import operations

Source of truth:
- Supabase `admin_credentials.status`

Known status values:
- `Active`
- `Threshold Exceeded`
- `MFA`
- `Subscription Disabled`

Current implementation:
- database read against `admin_credentials`

Notes:
- this is an internal status check
- this alone does not prove whether Microsoft is currently allowing outbound mail

### 4. Passive threshold check: does message trace show threshold-style failures?

Use when:
- checking whether a tenant appears thresholded without sending a new test message
- reviewing recent outbound failures on existing active domains

Script:
- `check-jack-tenant-threshold-mailtrace.ps1`

Source of truth:
- Exchange message trace
- optional message trace detail

Detection pattern:
- `5.7.705`
- `5.7.708`
- `tenant has exceeded threshold`
- similar threshold denial text

Classification:
- passive/read-only

Tradeoff:
- fast and non-destructive
- only works if the tenant already has recent outbound mail to inspect

### 5. Active threshold check: can the tenant send a fresh message right now?

Use when:
- proving a destination tenant is actually send-capable before migration
- validating a supposedly clean tenant more strongly than database status or trace review

Script:
- `Test-JackDestinationTenantActiveThreshold.ps1`

What it does:
- creates a temporary probe mailbox on the tenant
- sends one fresh message
- reads sender-side message trace detail
- classifies threshold evidence from the resulting send attempt

Classification:
- active/probe

Tradeoff:
- stronger proof than passive trace
- slower and more invasive than read-only checks

Important:
- this is the email-sending threshold method
- it is not the same as the passive mailtrace-only method

### 6. Which tenant currently owns this domain?

Use when:
- reconciling Airtable against live Microsoft ownership
- deciding whether a panel/admin assignment is still correct

Source of truth:
- Microsoft Graph `/domains` after logging into the target admin
- public Microsoft tenant discovery for lightweight brand/tenant lookup

Common workflow:
1. read the snapshot/live expected admin
2. log into that admin and list custom domains
3. if needed, search other candidate admins for the domain

Notes:
- this is the right proof for "what tenant is this domain actually on?"

### 7. Is live Airtable pointing to the right tenant/admin?

Use when:
- snapshot and Microsoft ownership need to be reconciled with Hyper-V Airtable

Source of truth order:
1. current Microsoft ownership
2. then compare live Airtable panel/admin assignment
3. snapshot is historical evidence, not final truth

Notes:
- a domain can be correct on Microsoft and still be wrong in Airtable
- `akdining.com` was an example of exactly that pattern

## What we actually used in the recent SI-candidate audit

For the May 15, 2026 SI-candidate admin check, we used:

- login proof: Microsoft token request
- license proof: Graph `/me/licenseDetails`
- no-domain proof: Graph `/domains`
- threshold proof: Supabase `admin_credentials.status`

We did **not** use:

- passive message trace
- active send/probe threshold testing

So that audit answered:
- can we log in?
- is the admin licensed?
- does the tenant have zero custom domains?
- is it not already flagged `Threshold Exceeded` in our DB?

It did **not** independently prove:
- the tenant can send a fresh outbound message right now

## Recommended rule of thumb

For reusable SI admin pool import:

Preclear gate:
- login works
- licensed
- zero custom domains
- `admin_credentials.status != Threshold Exceeded`

Final move gate:
- all preclear checks above
- plus active threshold probe with `Test-JackDestinationTenantActiveThreshold.ps1`

Only admins that pass the final move gate should be inserted into the reusable SI pool.

## Current SI candidate move workflow

Use this staged vocabulary:

- `precleared`: login, license, no-domain, and DB threshold checks pass
- `threshold_cleared`: the active send-proof threshold test returns clean
- `ready_to_insert`: both stages pass, and this row can be imported
- `blocked`: one of the required gates failed

Workflow:

1. Run the read-only candidate audit and save a CSV.
2. Run active threshold proof against the same candidate credential CSV:
   - script: `Test-JackDestinationTenantActiveThreshold.ps1`
   - pass `-CredentialCsv <candidate email/password csv>`
3. Build the final move plan:
   - script: `Build-SIAdminCandidateMovePlan.ps1`
   - inputs: precheck CSV + threshold CSV
   - output: one CSV with `move_stage` and `eligible_for_insert`
4. Import only rows marked `ready_to_insert`.

The Python importer `import_microsoft_admin_pool.py` now requires a readiness report for `--apply` unless an explicit emergency override is used. The PowerShell importer `import-microsoft-admin-credentials.ps1` has the same guard through `-ReadinessCsv`.

## Open cleanup we should keep doing

- Convert one-off audits into named scripts when they become repeated ops behavior.
- Save every run with timestamped JSON/CSV outputs.
- Copy the final artifacts back into `/Users/omermullick/Downloads/Projects/admin-order-audit/`.
- Add a short sentence to this file whenever we invent a new test, so the next pass starts from a playbook instead of memory.
