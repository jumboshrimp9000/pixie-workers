"""
Enroll gqa14457wa44a and gqa14457wa44b on pixiegtest3409ay73.com:
  1. Enable 2FA + store TOTP in 1Password
  2. Upload both to Smartlead
"""
import os
import sys
import logging
import json
from dataclasses import dataclass
from pathlib import Path

# Load env vars from .env.worker
env_file = Path(__file__).parent / ".env.worker"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())

# Non-headless for 2FA enrollment (Google blocks headless on myaccount.google.com)
# Smartlead upload can still use headless (OAuth flow is less restrictive)
os.environ["GOOGLE_PLAYWRIGHT_HEADLESS"] = "false"
os.environ["GOOGLE_PLAYWRIGHT_CHANNEL"] = "chrome"
os.environ["SMARTLEAD_ENABLE_PRIVATE_VALIDATION_FALLBACK"] = "true"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent))

from app.workers.google_admin_playwright import GoogleAdminPlaywrightClient, GoogleMfaUser
from app.workers.sending_tool_uploader import SendingToolUploader
from app.workers.onepassword_client import OnePasswordCliClient

# 1Password client
op_client = OnePasswordCliClient.from_env()
logger.info("1Password client ready (vault=%s)", op_client.vault)

# The two inboxes to enroll
users = [
    GoogleMfaUser(
        email="gqa14457wa44a@pixiegtest3409ay73.com",
        password="PxG!14457wa44A1",
        username="gqa14457wa44a",
    ),
    GoogleMfaUser(
        email="gqa14457wa44b@pixiegtest3409ay73.com",
        password="PxG!14457wa44B1",
        username="gqa14457wa44b",
    ),
]

# ── Step 1: Enroll 2FA + 1Password ──────────────────────────────────
logger.info("=== STEP 1: 2FA + 1Password enrollment ===")

with GoogleAdminPlaywrightClient(headless=True) as client:
    result = client.enroll_users_mfa_with_1password(users, op_client)

print("\n=== MFA ENROLLMENT RESULT ===")
print(json.dumps(result, indent=2, default=str))

if result.get("failed", 0) > 0:
    logger.error("MFA enrollment had failures — aborting Smartlead upload")
    sys.exit(1)

# Verify both items exist in 1Password
for user in users:
    item = op_client.find_google_login_item(user.email)
    if not item:
        logger.error("1Password item NOT found for %s after enrollment", user.email)
        sys.exit(1)
    totp = op_client.get_totp(item["id"])
    logger.info("Verified 1Password item for %s: id=%s, totp=%s", user.email, item["id"], totp)

# ── Step 2: Upload to Smartlead ─────────────────────────────────────
logger.info("=== STEP 2: Smartlead upload ===")

inboxes = [
    {"email": user.email, "password": user.password}
    for user in users
]

smartlead_username = "iknoor+vendors@smartlead.ai"
smartlead_password = "Test1234!"
smartlead_api_key = "test-placeholder-private-fallback"

uploader = SendingToolUploader()
logger.info("Starting Smartlead upload for %d inboxes...", len(inboxes))

upload_result = uploader.upload_and_validate(
    tool="smartlead.ai",
    api_key=smartlead_api_key,
    inboxes=inboxes,
    provider="google",
    credential={
        "username": smartlead_username,
        "password": smartlead_password,
    },
    onepassword=op_client,
    headless=True,
)

print("\n=== SMARTLEAD UPLOAD RESULT ===")
print(json.dumps(upload_result, indent=2, default=str))

uploaded = upload_result.get("uploaded_emails", [])
failed = upload_result.get("failed_uploads", [])
logger.info("=== FINAL SUMMARY ===")
logger.info("Uploaded: %d", len(uploaded))
logger.info("Failed: %d", len(failed))
for f in failed:
    logger.error("  FAILED: %s - %s", f.get("email"), f.get("error"))
for e in uploaded:
    logger.info("  OK: %s", e)

if failed:
    sys.exit(1)
