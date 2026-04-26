# Nonprofit Google Worker

## Production deployment

The production Google deploy runs both compose services from this folder:

- `pixie-google-paid-worker` runs `main.py` and owns paid Google `google_*` actions.
- `pixie-google-free-nonprofit-worker` runs `main_nonprofit.py` and owns `free_google_provision` plus `free_google_cancel_domain`.

Both services read `.env.worker`. Keep the nonprofit worker enabled while the free trial offer can enqueue `free_google_provision` actions; otherwise those actions will remain pending.

Use `docker compose up -d --build --remove-orphans pixie-google-paid-worker pixie-google-free-nonprofit-worker` for production parity with the GitHub deploy workflow.

## Environment

Environment variables:

- `NONPROFIT_GOOGLE_ACTION_TYPES=free_google_provision,free_google_cancel_domain`
- `NONPROFIT_GOOGLE_POLL_SECONDS=10`
- `NONPROFIT_GOOGLE_ADMIN_OP_VAULT`
- `NONPROFIT_GOOGLE_USER_OP_VAULT=icje7jpscrdm6xtlcr252zxinq`
- `NONPROFIT_GOOGLE_PLAYWRIGHT_HEADLESS=true`
- `NONPROFIT_GOOGLE_REQUIRE_MFA_ENROLLMENT=true`

Shared variables reused from the paid Google worker:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_GLOBAL_KEY`
- `CLOUDFLARE_EMAIL`
- `OP_SERVICE_ACCOUNT_TOKEN`
- `GOOGLE_PLAYWRIGHT_CHANNEL`
- `GOOGLE_PLAYWRIGHT_CHROME_PATH`
- `GOOGLE_PLAYWRIGHT_SLOW_MO_MS`
- `SMSPOOL_API_KEY`
- `SMSPOOL_SERVICE_ID`
- any sending tool credential env vars already used by `SendingToolUploader`
- any Dynadot env vars shared by the existing paid stack if your surrounding pipeline still needs them

Notes:

- The nonprofit worker uses separate `OnePasswordCliClient` instances for admin credentials (`NONPROFIT_GOOGLE_ADMIN_OP_VAULT`) and user credentials (`NONPROFIT_GOOGLE_USER_OP_VAULT`).
- Apps Script calls follow Google Apps Script `302` redirects manually before parsing JSON.
- Provisioning failures do not release the panel assignment; retries reuse the same panel.
