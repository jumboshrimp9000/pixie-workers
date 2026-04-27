# Job Workers

This folder contains external/background workers that consume Supabase actions.

## GitHub source of truth and live deploy

The production worker source of truth is:

- GitHub repo: `https://github.com/jumboshrimp9000/pixie-workers`
- Production branch: `main`
- Local repo path: `/Users/omermullick/Downloads/Projects/job-workers`

This repo deploys to live worker servers through `.github/workflows/deploy.yml`. A push to `main` runs the `Deploy pixie-workers` GitHub Actions workflow, which SSHes into the servers with `DEPLOY_SSH_KEY`, fetches `origin/main`, hard-resets the server checkout to that exact commit, removes untracked non-ignored files, and rebuilds/restarts Docker services:

- Microsoft worker server: `143.198.126.147`, path `/opt/pixie/microsoft-worker`, service `pixie-microsoft`
- Google worker server: `157.245.129.244`, path `/opt/pixie-google`, services `pixie-google-paid-worker` and `pixie-google-free-nonprofit-worker`

Do not rely on the outer `/Users/omermullick/Downloads/Projects` Git repo for worker deployment. The nested `job-workers` repo is the deployable repo; commit and push from there. Production worker servers must not carry manual local edits, because deployment intentionally overwrites tracked server changes with `origin/main` and removes untracked non-ignored files.

To verify a live deploy after pushing:

1. Check the GitHub Actions run for `Deploy pixie-workers` on `main`.
2. Confirm the run completed both `deploy-microsoft` and `deploy-google`.
3. If server access is available, confirm the relevant server path has the pushed commit with `git rev-parse HEAD` and confirm containers are running with `docker compose ps`.

## Canonical folders

- Root-level legacy scripts such as `MS-ResourceMailbox.ps1` are archived references only. Do not run them for production launch; use the canonical folders below.

- `pixie_google_smartlead/`
  - canonical Google worker folder
  - `pixie-google-paid-worker` owns paid Google provisioning and Google inbox lifecycle updates
  - `pixie-google-free-nonprofit-worker` owns free/nonprofit Google provisioning and cancellation

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
   - `google_provision` for paid Google
   - `free_google_provision` for free/nonprofit Google promo domains

Free Google promo routing is explicit. `AP` marks exactly one selected Google domain with `free_google_promo=true`, `fulfillment_process=free_google_nonprofit`, and `promo_inbox_count`. `prepare_domain` must skip PartnerHub/PartnerStage for that domain and enqueue `free_google_provision`; if the expected `free_inboxes_promo` inbox rows are missing, it fails instead of falling back to paid Google fulfillment.

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
- paid `google_*` lifecycle + provisioning actions -> `pixie-google-paid-worker`
- `free_google_provision` and `free_google_cancel_domain` -> `pixie-google-free-nonprofit-worker`

## Production compose topology

The Google production deploy runs `docker compose` from `pixie_google_smartlead/`, not from this top-level folder. That sub-compose must include both Google services:

- `pixie-google-paid-worker` executes `main.py`
- `pixie-google-free-nonprofit-worker` executes `main_nonprofit.py`

The Microsoft production deploy runs `docker compose` from `pixie-microsoft-rm-creation-process/` and starts `pixie-microsoft`.

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
