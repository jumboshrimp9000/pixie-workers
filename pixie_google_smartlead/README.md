# pixie_google_smartlead

Canonical Google fulfillment worker for Supabase actions.

## What this worker owns

Paid Google service (`pixie-google-paid-worker`, `main.py`):
- `google_provision`
- `google_add_inboxes`
- `google_remove_inboxes`
- `google_update_inboxes`
- `google_update_profile_photos`

Free/nonprofit Google service (`pixie-google-free-nonprofit-worker`, `main_nonprofit.py`):
- `free_google_provision`
- `free_google_cancel_domain`

## Fulfillment capabilities

Initial Google provisioning:
- create admin + users through PartnerHub
- persist the exact inbox password in Supabase before PartnerHub create so retries reuse the same credential
- checkpoint order creation so retries do not re-create/re-purchase licenses on PartnerHub
- add Google DNS to Cloudflare
- verify the Google domain in Admin using the manual verification flow (`Switch to manual verification`)
- allowlist required app IDs in Google Admin
- enable DKIM
- enroll inboxes into authenticator-based 2FA
- create/update 1Password items and store OTP secrets in Supabase
- upload inboxes to the selected sending tool (Instantly/Smartlead) via Playwright OAuth for Google domains
- validate each expected inbox in the sending-tool API before marking the step complete
- apply post-upload sending-tool settings from `domains.fulfillment_settings` (warmup, limits, tags, and tool-specific fields)
- require real sending-tool API keys in validation tests (placeholder/empty keys are invalid test setup)

MFA robustness:
- headless is still the default runtime mode
- if a user MFA enrollment fails in headless, worker can auto-retry that user in non-headless mode (`GOOGLE_MFA_NON_HEADLESS_FALLBACK=true`)

Google OAuth anti-regression rules:
- sign-in state handling is field-first: TOTP -> password -> email -> account chooser
- never click `data-identifier` chooser selectors before password/TOTP checks
- treat OAuth consent as non-signin state using URL path checks (ignore query-string `continue=.../consent...`)
- wait for consent page render before clicking and prefer role-based buttons over brittle XPath
- stop immediately on `Wrong password` / `Too many failed attempts` to avoid lockout loops

Sending-tool settings behavior:
- this worker only applies settings for newly processed domains (future orders)
- Instantly settings include account patch + warmup enable + tag assignment (tag create/lookup first)
- Smartlead settings include account update + warmup update + tag update
- upload validation remains strict; if settings apply fails for any expected inbox, the step fails with actionable logs

Existing-domain lifecycle:
- add inboxes
- remove inboxes
- rename users / change usernames
- upload profile photos

`google_update_inboxes` now also syncs tracked mutation state back to Supabase:
- request status
- per-item status
- mutation timeline events
- alias history rows

## Main files

- `main.py`
- `main_nonprofit.py`
- `run_google_supabase.py`
- `app/workers/google_supabase_worker.py`
- `app/workers/google_inbox_lifecycle_worker.py`
- `app/workers/nonprofit_google_provision_worker.py`
- `app/workers/nonprofit_google_cancel_worker.py`
- `app/workers/google_admin_playwright.py`
- `app/workers/onepassword_client.py`
- `app/workers/supabase_client.py`
- `SUPABASE_WORKER.md`
- `NONPROFIT_WORKER.md`

## 1Password behavior

For Google inboxes the worker:
- creates or updates the 1Password login item
- stores the TOTP secret in the item
- updates the item title on username changes
- writes previous/current identity details into notes

New item titles use the plain email with `@` converted to `-`, without the old `google-` prefix or `-at-` format.

## Debug tooling

Troubleshooting-only helper:
- `scripts/profile_photo_debug_runner.py`

This is not part of the production runtime.

## Run

Recommended (loads `./.env.worker` first):

```bash
cd job-workers/pixie_google_smartlead
./scripts/run_with_worker_env.sh python3 main.py
```

or

```bash
cd job-workers/pixie_google_smartlead
./scripts/run_with_worker_env.sh python3 run_google_supabase.py
```

Direct run (expects env already exported):

```bash
cd job-workers/pixie_google_smartlead
python3 main.py
```

or

```bash
cd job-workers/pixie_google_smartlead
python3 run_google_supabase.py
```

Production compose runs both Google services:

```bash
cd job-workers/pixie_google_smartlead
docker compose up -d --build --remove-orphans pixie-google-paid-worker pixie-google-free-nonprofit-worker
```
