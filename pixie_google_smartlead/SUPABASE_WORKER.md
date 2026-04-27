# Supabase Google Worker

This folder is Supabase-only. It processes Google provisioning actions from the `actions` table.

## Action types

Default action type for this worker:
- `google_provision`

Lifecycle action types (same process, separate worker loop):
- `google_add_inboxes`
- `google_remove_inboxes`
- `google_update_inboxes`
- `google_update_profile_photos`

Configurable with:
- `GOOGLE_WORKER_ACTION_TYPES` (comma-separated)

Recommended when AP workers are enabled:
- `GOOGLE_WORKER_ACTION_TYPES=google_provision`
- `GOOGLE_LIFECYCLE_ACTION_TYPES=google_add_inboxes,google_remove_inboxes,google_update_inboxes,google_update_profile_photos`

## Required environment variables

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `CLOUDFLARE_ACCOUNT_ID`
- `DYNADOT_API_KEY`
- `PARTNERHUB_API_KEY` (or `GOOGLE_NETSTAGER_API_KEY`)
- `OP_SERVICE_ACCOUNT_TOKEN` (for 1Password writes)
- `ONEPASSWORD_VAULT` (vault name or vault ID)

If `GOOGLE_REQUIRE_MFA_ENROLLMENT=false`, the 1Password variables can be omitted.

Cloudflare auth (one required):
- `CLOUDFLARE_API_TOKEN`
- or `CLOUDFLARE_GLOBAL_KEY` + `CLOUDFLARE_EMAIL`

## Optional environment variables

- `PARTNERHUB_API_BASE_URL` (default: `https://partnerhubapi.netstager.com/api`)
- `PARTNERHUB_PLAN_ID` (default: `94c835bb-a675-4249-8fba-e95cdb2ca4ed`)
- `GOOGLE_WORKER_POLL_SECONDS` (default: `10`)
- `GOOGLE_WORKER_BATCH_SIZE` (default: `3`)
- `GOOGLE_WORKER_MAX_RETRIES` (default: `5`)
- `GOOGLE_WORKER_DRY_RUN` (`true`/`false`, default `false`)
- `GOOGLE_WORKER_REQUIRE_CF_ACTIVE` (`true`/`false`, default `true`)
- `GOOGLE_REQUIRE_MFA_ENROLLMENT` (`true`/`false`, default `true`)
- `GOOGLE_PLAYWRIGHT_HEADLESS` (`true`/`false`, default `true`)
- `GOOGLE_MFA_NON_HEADLESS_FALLBACK` (`true`/`false`, default `true`; retries failed headless MFA users in non-headless mode)
- `GOOGLE_MFA_NON_HEADLESS_MAX_ATTEMPTS` (default `1`; retry attempts for non-headless fallback)
- `GOOGLE_PLAYWRIGHT_CHROME_PATH` (optional Chrome/Chromium executable path)
- `GOOGLE_PLAYWRIGHT_CHANNEL` (optional channel name such as `chrome`)
- `GOOGLE_PLAYWRIGHT_SLOW_MO_MS` (optional; default `0`)
- `GOOGLE_ONEPASSWORD_ITEM_PREFIX` (optional item title prefix; default empty)
- `GOOGLE_REQUIRED_ADMIN_APP_IDS` (comma-separated OAuth client IDs; defaults to Smartlead + Instantly)
- `GOOGLE_REQUIRE_ADMIN_APPS` (`true`/`false`, default `true`)
- `GOOGLE_ADMIN_APPS_ATTEMPTS` (default `3`; retries required app allowlisting inside one action attempt)
- `GOOGLE_ADMIN_APPS_RETRY_DELAY_SECONDS` (default `20`; exponential-ish delay multiplier between retries)
- `GOOGLE_REQUIRE_DKIM_ENABLED` (`true`/`false`, default `true`)
- `GOOGLE_DKIM_AUTH_ATTEMPTS` (default `5`)
- `GOOGLE_DKIM_AUTH_INTERVAL_SECONDS` (default `45`)
- `GOOGLE_DKIM_DNS_WAIT_SECONDS` (default `10`)
- `GOOGLE_LIFECYCLE_POLL_SECONDS` (default: `10`)
- `GOOGLE_LIFECYCLE_BATCH_SIZE` (default: `3`)
- `GOOGLE_LIFECYCLE_MAX_RETRIES` (default: `8`)
- `GOOGLE_MAX_INBOXES_PER_DOMAIN` (default: `5`)
- `GOOGLE_PROFILE_PHOTO_OPTIONAL` (`true`/`false`, default `false`)
- `ONEPASSWORD_OP_TIMEOUT_SECONDS` (default: `45`)
- `GOOGLE_SENDING_TOOL_USE_PLAYWRIGHT_OAUTH` (`true`/`false`, default `true`; uses Playwright OAuth upload for Google tool connections)
- `GOOGLE_SENDING_TOOL_OAUTH_REQUIRE_1PASSWORD` (`true`/`false`, default `true`; fail upload when 1Password is unavailable)
- `INSTANTLY_VALIDATION_ATTEMPTS` (default `8`)
- `INSTANTLY_VALIDATION_INTERVAL_MS` (default `5000`)
- `INSTANTLY_VALIDATION_CONCURRENCY` (default `10`)
- `INSTANTLY_OAUTH_STATUS_ATTEMPTS` (default `25`)
- `INSTANTLY_OAUTH_STATUS_INTERVAL_SECONDS` (default `3`)
- `SMARTLEAD_VALIDATION_ATTEMPTS` (default `8`)
- `SMARTLEAD_VALIDATION_INTERVAL_MS` (default `5000`)
- `SMARTLEAD_VALIDATION_CONCURRENCY` (default `6`)
- `SMARTLEAD_API_KEY` (required for strict Smartlead API validation in automated test harnesses)
- `SMARTLEAD_ENABLE_PRIVATE_VALIDATION_FALLBACK` (`true`/`false`, default `false`; only used when API validation cannot confirm all inboxes)
- `SMARTLEAD_GOOGLE_OAUTH_SESSION_MODE` (`per_inbox_login` or `single_login_popup_isolation`, default `per_inbox_login`)
  - `single_login_popup_isolation` logs into Smartlead once, then runs each inbox OAuth in an isolated browser context that reuses only Smartlead session state

## Logging and troubleshooting

- Worker writes per-action logs to Supabase `action_logs` with:
  - `event_type` (`step_started`, `step_completed`, `step_failed`, `action_failed`, etc.)
  - actionable error messages and hints
- Common error hints:
  - `Cloudflare DNS authentication failed`:
    - fix `CLOUDFLARE_API_TOKEN` permissions, or set `CLOUDFLARE_GLOBAL_KEY` + `CLOUDFLARE_EMAIL`
  - `Google Workspace does not currently support this domain name`:
    - PartnerHub rejected that domain/TLD; retry with a supported domain
  - `Cloudflare zone ... not active yet`:
    - nameserver propagation incomplete; retry later

Google OAuth troubleshooting rules:
- Sign-in loop is field-first: TOTP -> password -> email -> account chooser.
- Do not run `data-identifier` account-click selectors before password/TOTP checks.
- Treat `/consent` URL paths as non-signin pages; evaluate URL path only (not query string).
- Use explicit render wait before consent clicks, then role-based button selectors.
- Treat `Wrong password` / `Too many failed attempts` as hard-stop failures to prevent lockout loops.

## Fulfillment flow (Supabase)

For each Google action:

1. Fetch `domains` + `inboxes` from Supabase.
2. Shared domain prep (idempotent):
   - Buy domain on Dynadot when `source=buy`.
   - Ensure Cloudflare zone exists.
   - Fetch Cloudflare nameservers.
   - Move Dynadot NS to Cloudflare for purchased domains.
   - Wait for Cloudflare zone active (unless disabled via env).
3. Persist inbox passwords in Supabase before PartnerHub create:
   - use `inboxes[].password` when present
   - otherwise use `new_password` / `default_password`
   - otherwise generate a password and store it first so retries reuse the same value
4. Call PartnerHub API (`/integration/orders`) to create Google order/users.
5. Treat the first created user as the Google admin user (`userType=admin`); remaining users are normal users.
6. Fetch order details and extract DNS records.
7. Add DNS records to Cloudflare (fallback to standard Google MX/SPF/DMARC if API records are absent).
8. Resolve admin login from Supabase (`is_admin=true`, with fallback promotion when missing).
9. Verify the primary domain in Google Admin:
   - open `Manage domains` and click `Verify domain`
   - explicitly switch to manual verification (`Switch to manual verification`)
   - read Google verification TXT value and write it to Cloudflare
   - confirm verification until Admin shows verified
   - worker enforces a hard pre-upload verification gate and fails the action if verification proof is missing
10. Configure trusted OAuth app IDs in Google Admin (Smartlead + Instantly by default; optional extras from payload).
11. Enable DKIM in Google Admin:
   - fetch or generate DKIM TXT host/value
   - write DKIM TXT in Cloudflare
   - start authentication and verify enabled state
12. Enroll each created user into Google 2FA with Authenticator + store each user in 1Password.
13. Upload inboxes to sending tool when configured:
   - fetch `domain_credentials` + `sending_tool_credentials`
   - for Google domains, use Playwright OAuth upload for Instantly/Smartlead (headless by default)
   - Google OAuth state handling is strict field-order (`totp -> password -> email -> chooser`) to avoid chooser-loop regressions
   - consent detection uses URL path checks only; query-string `continue` parameters are ignored for state classification
   - each inbox OAuth upload runs in an isolated Playwright browser context to avoid session bleed on shared IPs
   - Smartlead can run either:
     - `per_inbox_login` (login each isolated context)
     - `single_login_popup_isolation` (login once, isolate per-inbox OAuth contexts with Smartlead-only storage state)
   - SMTP/IMAP upload paths are removed from worker automation
   - Smartlead validation is API-first (`/api/v1/email-accounts`) and strict per-inbox
   - placeholder keys (for example `test-no-api-key`) are invalid for strict validation and should fail test harnesses fast
   - optional private-endpoint fallback is disabled by default and only runs when `SMARTLEAD_ENABLE_PRIVATE_VALIDATION_FALLBACK=true`
   - validate each expected inbox via provider API before completing the step
   - apply post-upload settings only when `sendingToolSettings` is present, and only to validated uploaded inboxes (never before validation success)
   - Instantly tag flow:
     - list/create tag: `GET/POST /api/v2/custom-tags` (create uses `label`)
     - assign tag: `POST /api/v2/custom-tags/toggle-resource`
     - verify mapping: `GET /api/v2/custom-tag-mappings`
   - Smartlead tag flow:
     - login for JWT + canonical API key: `POST /api/auth/login`
     - list/create tag via GraphQL: `POST https://fe-gql.smartlead.ai/v1/graphql` (`getAllTags`, `createTag`)
     - assign mapping: `POST /api/v1/email-accounts/tag-mapping`
     - verify mapping via GraphQL account query (`getEmailAccountTagsAndClientById`)
   - Smartlead account settings map:
     - daily limit -> `max_email_per_day` (reflected by Smartlead as `message_per_day`)
     - signature -> `signature`
     - escaped signature line breaks (`\\n`) are normalized to real newlines before submit
   - Smartlead warmup payload uses `warmup_enabled` (not `enabled`)
14. Mark inboxes active in Supabase and store generated/assigned credentials.
15. Mark the domain active and complete the action.

For Google lifecycle actions:

1. `google_add_inboxes`
   - Validate requested inbox IDs.
   - Ensure domain remains at or below 5 inboxes.
   - Set target inboxes `provisioning`.
   - Resolve existing PartnerHub order ID for domain.
   - Increase licenses + add license users using PartnerHub endpoints:
     - `POST /integration/orders/increase-license`
     - `POST /integration/order/amendment-order-licence-users`, falling back to
       `POST /integration/orders/amendment-order-licence-users` for PartnerHub tenants where only
       the plural route is live.
   - PartnerHub order IDs are stored on `domains.partnerhub_order_id` and validated against the
     exact domain before reuse. The license-increase step is checkpointed separately from the user
     amendment step so retries do not intentionally buy seats twice after a successful increase.
   - If PartnerHub reports a business-state blocker such as no seats, bad plan/order, or insufficient
     balance, the action hard-stops with an ops-readable error instead of burning retries.
   - Enroll newly added users into 1Password + Google 2FA using Playwright.
   - Mark target inboxes `active`.
2. `google_remove_inboxes`
   - Validate target inbox IDs.
   - Prevent removing all inboxes.
   - Suspend removed users in Google Admin via Playwright.
   - Mark removed inboxes `deleted`.
3. `google_update_inboxes`
   - Apply requested name/username changes in Supabase.
   - Optionally upload profile photos in the same request when a photo URL is included.
   - Update user names/usernames in Google Admin via Playwright.
   - Update the matching 1Password item title and write previous/current identity notes.
   - Sync `inbox_mutation_submissions`, `domain_mutation_requests`, `domain_mutation_items`, `domain_mutation_events`, and `inbox_email_aliases`.
   - Return updated inboxes to `active`.
4. `google_update_profile_photos`
   - Update `profile_pic_url` in Supabase.
   - Upload profile photos in Google Admin via Playwright.

Progress is written back to `actions.result.steps[]` and `domains.interim_status`.
For tracked username/name changes, status is also written into the inbox-mutation tables so the client app and `admin-internal` can show old identity -> new identity history.

## Action payload fields (supported)

- `organization_name` (string)
- `new_password` / `default_password` (string)
- `plan_id` (string; overrides `PARTNERHUB_PLAN_ID`)
- `custom_dns_records` (array of DNS record objects)
- per-inbox rows can also carry `password`; that value wins over `new_password` / `default_password`
- optional Google Admin app config fields:
  - `bison_app_id` (string OAuth client ID)
  - `additional_tools_id` (string or list of OAuth client IDs)
  - `master_inbox_enable`, `warmy_enable`/`warmy_enabled`, `plusvibe_enable`/`plusvibe_enabled` (boolean)

Lifecycle payloads:

- `google_add_inboxes`
```json
{
  "domain": "example.com",
  "inbox_ids": ["uuid-1", "uuid-2"]
}
```

- `google_remove_inboxes`
```json
{
  "domain": "example.com",
  "inbox_ids": ["uuid-1"]
}
```

- `google_update_inboxes`
```json
{
  "domain": "example.com",
  "mutation_submission_id": "uuid-submission",
  "mutation_request_id": "uuid-request",
  "updates": [
    {
      "inbox_id": "uuid-1",
      "username": "new.username",
      "first_name": "New",
      "last_name": "Name"
    }
  ],
  "photo_updates": [
    {
      "inbox_id": "uuid-1",
      "email": "new.username@example.com",
      "profile_pic_url": "https://..."
    }
  ]
}
```

- `google_update_profile_photos`
```json
{
  "domain": "example.com",
  "updates": [
    {
      "inbox_id": "uuid-1",
      "profile_pic_url": "https://..."
    }
  ]
}
```

`custom_dns_records` item shape:

```json
{
  "type": "TXT",
  "name": "_dmarc",
  "content": "v=DMARC1; p=none",
  "ttl": 3600,
  "priority": 10
}
```

## Run

Install browser dependency once per machine:

```bash
python -m playwright install chromium
```

```bash
python main.py
```

or directly:

```bash
python run_google_supabase.py
```
