# Job Workers

This folder contains external/background workers that consume Supabase actions.

## Canonical folders

- `pixie_google_smartlead/`
  - canonical Google worker
  - owns Google provisioning and Google inbox lifecycle updates

- `hyper-tide-google-feat-add-users-smartlead/`
  - legacy Google reference folder
  - do not treat this as the production path

- `pixie-microsoft-rm-creation-process/`
  - canonical Microsoft PowerShell worker
  - owns Microsoft provisioning plus tracked room-mailbox mutations

## How this fits with `AP`

`AP` writes normalized state into Supabase and enqueues actions.

Shared queue flow:
1. `prepare_domain`
2. provider-specific action:
   - `provision_inbox` for Microsoft
   - `google_provision` for Google

Google lifecycle changes also flow through Supabase actions:
- `google_add_inboxes`
- `google_remove_inboxes`
- `google_update_inboxes`
- `google_update_profile_photos`

Microsoft lifecycle changes also flow through Supabase actions:
- `microsoft_update_inboxes`

## Worker ownership

Do not run two workers against the same action type.

Recommended ownership:
- `prepare_domain` -> `AP`
- `provision_inbox` -> `pixie-microsoft-rm-creation-process`
- `microsoft_update_inboxes` -> `pixie-microsoft-rm-creation-process`
- all `google_*` lifecycle + provisioning actions -> `pixie_google_smartlead`

## Mutation tracking

Google and Microsoft inbox identity changes no longer exist as anonymous queue rows only.

They now update:
- `inbox_mutation_submissions`
- `domain_mutation_requests`
- `domain_mutation_items`
- `domain_mutation_events`
- `inbox_email_aliases`

This lets the app and `admin-internal` show:
- old identity -> new identity
- current step
- failure reason
- per-inbox status
- worker attempt / retry state for Microsoft mutations

Microsoft-specific behavior:
- the PowerShell worker renames existing room mailboxes in place
- it explicitly preserves the old email as an alias
- it updates `domain_mutation_requests`, `domain_mutation_items`, `domain_mutation_events`, and `inbox_email_aliases`
- it persists stable checkpoint keys into `actions.result.steps[]`, so retries update the existing step record instead of appending duplicate stale steps

## Related docs

- `../README.md`
- `../AP/README.md`
- `pixie_google_smartlead/README.md`
- `pixie_google_smartlead/SUPABASE_WORKER.md`
- `pixie-microsoft-rm-creation-process/README.md`
