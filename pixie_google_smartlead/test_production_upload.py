"""
Test: Mimic the exact production upload path.
Reads domain, inboxes, and credentials from Supabase,
then calls _upload_domain_inboxes_to_sending_tool exactly
as the google_supabase_worker would.
"""
import os
import sys
import logging
from pathlib import Path

# Load env
env_file = Path(__file__).parent / ".env.worker"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent))

from app.workers.google_supabase_worker import GoogleSupabaseWorker

DOMAIN_ID = "df3e7522-799e-4177-a4aa-a9cd5f54c88d"
DOMAIN_NAME = "whoistherightthing.xyz"
PROVIDER = "google"

# Instantiate the worker (same as production)
worker = GoogleSupabaseWorker()
logger.info("Worker initialized. headless=%s, playwright_oauth=%s",
            worker.playwright_headless, worker.sending_tool_playwright_oauth)

# Read inboxes from Supabase (same as production)
inboxes = worker.client.get_domain_inboxes(DOMAIN_ID)
logger.info("Fetched %d inboxes from Supabase:", len(inboxes))
for inbox in inboxes:
    logger.info("  %s (status=%s, has_password=%s)",
                inbox.get("email"), inbox.get("status"), bool(inbox.get("password")))

# Read tool credentials from Supabase (same as production)
tool_bundle = worker.client.get_domain_tool_credentials(DOMAIN_ID)
if tool_bundle:
    slug = tool_bundle.get("slug")
    cred = tool_bundle.get("credential", {})
    logger.info("Tool credentials: slug=%s, username=%s, has_api_key=%s",
                slug, cred.get("username"), bool(cred.get("api_key")))
else:
    logger.error("No tool credentials found for domain!")
    sys.exit(1)

# Call the exact production method
logger.info("Calling _upload_domain_inboxes_to_sending_tool (production path)...")
result = worker._upload_domain_inboxes_to_sending_tool(
    domain_id=DOMAIN_ID,
    domain_name=DOMAIN_NAME,
    provider=PROVIDER,
    inboxes=inboxes,
    user_updates={},  # No user_updates — inboxes already have final emails/passwords
)

import json
logger.info("Upload result:")
print(json.dumps(result, indent=2, default=str))

# Summary
uploaded = result.get("uploaded_emails", [])
failed = result.get("failed_uploads", [])
logger.info("=== SUMMARY ===")
logger.info("Uploaded: %d", len(uploaded))
logger.info("Failed: %d", len(failed))
for f in failed:
    logger.error("  FAILED: %s - %s", f.get("email"), f.get("error"))
for e in uploaded:
    logger.info("  OK: %s", e)
