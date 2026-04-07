"""
Test: Upload ONE inbox (varun.k@pixiegtest3409ay73.com) to Smartlead
using production SendingToolUploader code. No code edits.
"""
import os
import sys
import logging
import json
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

# Headless + chrome channel
os.environ["GOOGLE_PLAYWRIGHT_HEADLESS"] = "true"
os.environ["GOOGLE_PLAYWRIGHT_CHANNEL"] = "chrome"

# Enable private validation fallback (no real API key needed)
os.environ["SMARTLEAD_ENABLE_PRIVATE_VALIDATION_FALLBACK"] = "true"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent))

from app.workers.sending_tool_uploader import SendingToolUploader
from app.workers.onepassword_client import OnePasswordCliClient

# 1Password client for TOTP
op_client = OnePasswordCliClient.from_env()
logger.info("1Password client ready (vault=%s)", op_client.vault)

# Verify 1Password item exists for this inbox
item = op_client.find_google_login_item("varun.k@pixiegtest3409ay73.com")
if item:
    logger.info("Found 1Password item: id=%s", item.get("id", ""))
    totp = op_client.get_totp(item["id"])
    logger.info("TOTP code OK: %s", totp)
else:
    logger.error("No 1Password item found for varun.k@pixiegtest3409ay73.com")
    sys.exit(1)

# Target inbox
inboxes = [
    {
        "email": "varun.k@pixiegtest3409ay73.com",
        "password": "PxG!3409ay73A1",
    },
]

# Smartlead credentials
smartlead_username = "iknoor+vendors@smartlead.ai"
smartlead_password = "Test1234!"
# Use a placeholder API key — private validation fallback handles validation
smartlead_api_key = "test-placeholder-private-fallback"

uploader = SendingToolUploader()
logger.info("Starting headless Smartlead upload for varun.k@pixiegtest3409ay73.com...")

result = uploader.upload_and_validate(
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

print("\n=== RESULT ===")
print(json.dumps(result, indent=2, default=str))

uploaded = result.get("uploaded_emails", [])
failed = result.get("failed_uploads", [])
logger.info("=== SUMMARY ===")
logger.info("Uploaded: %d", len(uploaded))
logger.info("Failed: %d", len(failed))
for f in failed:
    logger.error("  FAILED: %s - %s", f.get("email"), f.get("error"))
for e in uploaded:
    logger.info("  OK: %s", e)

if failed:
    sys.exit(1)
