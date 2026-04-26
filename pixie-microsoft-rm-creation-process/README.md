# Pixie Microsoft Room Mailbox Worker

Hybrid TypeScript + PowerShell worker for Microsoft 365 room mailboxes. This folder is Supabase-native and owns three Microsoft action families:

- `provision_inbox` — initial domain + room mailbox provisioning
- `microsoft_update_inboxes` — tracked rename / username-change mutations on existing room mailboxes
- `microsoft_cancel_domain` — post-grace cancellation teardown and final state sync

## Architecture

```text
Supabase actions queue
  prepare_domain                -> AP shared worker
  provision_inbox               -> this folder
  microsoft_update_inboxes      -> this folder
  microsoft_cancel_domain       -> this folder

Part 1: Universal domain setup (TypeScript)
  - buy domain when source=buy
  - create/get Cloudflare zone
  - move nameservers to Cloudflare
  - wait for nameserver propagation

Part 2A: Microsoft provisioning (PowerShell)
  - acquire a lease on one Microsoft admin from Supabase
  - get Graph token + connect Exchange Online
  - add domain to M365
  - fetch verification TXT and write to Cloudflare
  - verify the domain in M365
  - enable email service
  - add M365 DNS records in Cloudflare
  - wait for Exchange sync
  - create room mailboxes with unique temp display names
  - rename display names back to the requested values
  - disable calendar auto-processing
  - fix UPNs / primary SMTP state
  - enable SMTP AUTH
  - enable DKIM
  - enqueue `reupload_inboxes` for sending-tool upload/validation
  - finalize Supabase state only after upload validation confirms the expected active inbox count

Part 2B: Microsoft inbox mutations (PowerShell)
  - load the queued mutation request/items from Supabase
  - acquire a lease on the domain's assigned Microsoft admin
  - connect Exchange Online
  - rename the existing room mailbox in place
  - update first name / last name / display name
  - update UPN / primary SMTP address
  - explicitly preserve the old email as an alias
  - update Supabase inbox row + mutation tracking rows
  - resume only incomplete items on retry

Part 3: Microsoft cancellation teardown (PowerShell)
  - acquire a lease on the domain admin and connect to Exchange
  - remove domain mailboxes + domain-linked Exchange recipients
  - remove remaining Graph users on the domain
  - remove accepted domain from Exchange (cleanup + retry loop)
  - set domain `cancelled` + inboxes `deleted` in Supabase
```

## Why PowerShell

PowerShell is required for the Exchange Online cmdlets that do not have a clean Graph replacement for this workflow:

- `New-Mailbox -Room`
- `Set-CalendarProcessing`
- `Set-Mailbox` mailbox-type / SMTP / alias operations
- `Get-DkimSigningConfig` / `Set-DkimSigningConfig` / `New-DkimSigningConfig`
- `Set-TransportConfig -SmtpClientAuthenticationDisabled $false`
- `Connect-ExchangeOnline`

PowerShell is also the correct place for Microsoft rename-in-place mutations because alias preservation is part of the contract and Exchange has to be authoritative for that step.

## Room Mailbox Model

This worker creates **room/resource mailboxes**, not shared mailboxes and not long-lived licensed users.

Important behavior:
- room mailboxes do not need a permanent user license after creation
- Microsoft does not allow duplicate display names during initial creation, so provisioning uses temporary display names and renames them back after creation
- room mailboxes delete non-calendar items by default unless calendar auto-processing is disabled
- this worker must run `Set-CalendarProcessing -AutomateProcessing None -DeleteNonCalendarItems $false`

## Sending Tool Upload Gate

Microsoft provisioning does not mark a domain `active` immediately after DKIM. After room mailboxes are created and active inbox rows are verified, the worker enqueues a Supabase `actions.type='reupload_inboxes'` row with `payload.source='microsoft_provision'`.

Endpoint strategy and tradeoff:
- The PowerShell worker uses Supabase REST to enqueue and poll the existing `reupload_inboxes` action because the AP `ReuploadWorker` already owns provider-specific upload and API validation for Instantly/Smartlead.
- While upload is pending, the domain stays `status='in_progress'` with `interim_status='Both - Sending Tool Upload Pending'`; the `provision_inbox` action is requeued without consuming attempts.
- The domain is marked `active` only when the upload action is `completed`, has zero failed uploads, and reports `uploaded >= active inbox count`.
- If no sending-tool credential is assigned, or the upload action fails validation, provisioning stops in an actionable upload-blocked/failed state instead of pretending completion.

## Microsoft Mutation Model

`microsoft_update_inboxes` is the Microsoft equivalent of the Google tracked identity-change flow.

What it does:
1. Reads a queued mutation action from `actions`
2. Loads `domain_mutation_requests` and `domain_mutation_items`
3. Renames the existing room mailbox instead of deleting + recreating it
4. Updates `userPrincipalName` and primary SMTP
5. Explicitly re-adds the old email as an alias
6. Updates the canonical `inboxes` row in Supabase
7. Writes request/item/event progress back to Supabase

What it does not do:
- it does not delete the mailbox
- it does not create a replacement user
- it does not currently handle Microsoft profile-photo mutations
- it does not depend on DKIM to rename an already-existing room mailbox; DKIM is domain-level provisioning state

## Supabase Tables Used

| Table | Purpose |
|-------|---------|
| `actions` | queue rows for `provision_inbox`, `microsoft_update_inboxes`, and `microsoft_cancel_domain` |
| `domains` | canonical domain state, including `interim_status` |
| `inboxes` | canonical inbox state |
| `admin_credentials` | Microsoft admin inventory |
| `domain_admin_assignments` | which admin is responsible for the domain |
| `action_logs` | immutable worker log |
| `inbox_mutation_submissions` | top-level mutation submission |
| `domain_mutation_requests` | one active locked request per domain |
| `domain_mutation_items` | old/new snapshots per inbox |
| `domain_mutation_events` | client/admin-visible mutation timeline |
| `inbox_email_aliases` | preserved old emails after username change |

## Runtime Ownership

Do not run multiple workers on the same action type.

Recommended ownership:
- `prepare_domain` -> `AP` `DomainPreparationWorker`
- `provision_inbox` -> this folder
- `microsoft_update_inboxes` -> this folder
- `microsoft_cancel_domain` -> this folder
- Google action types -> `job-workers/pixie_google_smartlead`

The AP backend still contains in-process Microsoft worker code, but if this external PowerShell worker is active, do not run both against the same Microsoft queue actions.

## Microsoft Admin Locking

Microsoft admin automation is single-threaded per admin credential.

- only one active Microsoft action may hold a given admin at a time
- the lock is stored on `admin_credentials` with an expiring lease
- provisioning, mutation, and cancellation all acquire the lease before Exchange/Graph work
- if the admin is busy, the action is put back into `pending` with `next_retry_at` instead of failing
- Google workers are unaffected because they use separate action types and a separate admin pool

Practical effect:
- one Microsoft admin can still own many domains over time
- but only one domain action is actively processed on that admin at a time
- once a domain is assigned to an admin, retries and later mutations/cancellation prefer that same admin

Deployment/config notes:
- the production compose service is `pixie-microsoft` and should not be co-deployed inside the Google-only compose project
- one-admin-at-a-time behavior depends on the Supabase RPCs `acquire_microsoft_admin_lock`, `refresh_microsoft_admin_lock`, and `release_microsoft_admin_lock`
- Microsoft admin rows must exist in `admin_credentials` with `provider=microsoft` and `active=true`
- `WORKER_ACTION_LEASE_SECONDS` controls stale action reclaim for crashed workers; it does not disable or replace the per-admin Supabase lock
- the admin lock lease defaults to 7200 seconds in `Acquire-MicrosoftAdminLock`; actions that cannot obtain the lock are requeued without consuming an attempt

## Files

```text
pixie-microsoft-rm-creation-process/
├── README.md
├── .env.example
├── config.ps1
├── Part1-UniversalDomainSetup.ts
├── Part2-MicrosoftRoomMailbox.ps1
├── Part2-MicrosoftInboxMutations.ps1
├── Part3-MicrosoftDomainCancellation.ps1
├── run.ps1
├── supabase-migration.sql
└── package.json
```

## Setup

Prerequisites:
- PowerShell Core (`pwsh`)
- ExchangeOnlineManagement module
- Node.js 20+
- Supabase schema from `AP/supabase/`

Required env:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_GLOBAL_KEY`
- `CLOUDFLARE_EMAIL`
- `DYNADOT_API_KEY`

## Run

```bash
pwsh ./run.ps1
pwsh ./run.ps1 -DryRun
pwsh ./run.ps1 -MaxDomains 5 -Once
```

## Resume Behavior

Provisioning resume:
- `domains.interim_status` tracks the provisioning stage
- `actions.result.steps[]` tracks step checkpoints
- set the action back to `pending` to continue from the last good step

Mutation resume:
- completed item checkpoints are stored in `actions.result.steps[]`
- per-inbox status is written to `domain_mutation_items`
- old-email alias state is written to `inbox_email_aliases`
- retries continue only unfinished or failed items

Checkpoint behavior:
- checkpoint keys are stable (`load_mutation_context`, `connect_exchange_online`, `mutate_<inbox_id>`, `finalize_mutation`)
- retries update the existing checkpoint record instead of appending duplicate stale step rows
- this is what lets `admin-internal` show a clean, current worker state instead of an unreadable retry trail

## Admin Readability Contract

When ops opens `admin-internal/inbox-mutations`, the Microsoft request should be understandable without opening PowerShell logs.

For each request, the UI should be able to show:
- overall submission status
- domain request status
- worker action status and attempts
- old identity -> new identity per inbox
- checkpoint list from `actions.result.steps[]`
- failure reason at submission/request/item/event level

That readability depends on three Supabase layers staying in sync:
- `domain_mutation_requests` / `domain_mutation_items` / `domain_mutation_events`
- `actions.status` / `actions.error`
- `actions.result.steps[]` and `actions.result.summary`
