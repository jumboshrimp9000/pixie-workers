"""
Test script: Upload both Google inboxes on whoistherightthing.xyz to Smartlead
in headless mode using the production code path.
"""
import os
import sys
import logging
import json

# Load env vars from .env.worker
from pathlib import Path
env_file = Path(__file__).parent / ".env.worker"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())

# Setup logging so we can see what's happening
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent))

from app.workers.sending_tool_uploader import SendingToolUploader
from app.workers.onepassword_client import OnePasswordCliClient


def _require_non_placeholder_api_key() -> str:
    candidates = [
        os.getenv("SMARTLEAD_API_KEY", "").strip(),
        os.getenv("SMARTLEAD_TEST_API_KEY", "").strip(),
    ]
    for value in candidates:
        if not value:
            continue
        lowered = value.lower()
        if lowered in {"test-no-api-key", "changeme", "replace-me"}:
            continue
        if "test-no-api-key" in lowered:
            continue
        return value
    raise RuntimeError(
        "Smartlead API key is required for deterministic validation. "
        "Set SMARTLEAD_API_KEY (or SMARTLEAD_TEST_API_KEY) to a real key."
    )

# Initialize 1Password client from env
op_client = OnePasswordCliClient.from_env()
logger.info("1Password client initialized (vault=%s)", op_client.vault)

# Test: can we find the 1Password items for these inboxes?
for email in ["michah.heathrow@whoistherightthing.xyz", "london.breed@whoistherightthing.xyz"]:
    item = op_client.find_google_login_item(email)
    if item:
        item_id = item.get("id", "")
        logger.info("Found 1Password item for %s: id=%s", email, item_id)
        totp = op_client.get_totp(item_id)
        logger.info("TOTP code for %s: %s", email, totp)
    else:
        logger.warning("No 1Password item found for %s", email)

# Smartlead test credentials
smartlead_username = "iknoor+vendors@smartlead.ai"
smartlead_password = "Test1234!"

# The inboxes to upload
inboxes = [
    {
        "email": "michah.heathrow@whoistherightthing.xyz",
        "password": "WhoIsRightThing84!",
    },
    {
        "email": "london.breed@whoistherightthing.xyz",
        "password": "WhoIsRightThing84!",
    },
]

smartlead_api_key = _require_non_placeholder_api_key()

uploader = SendingToolUploader()
logger.info("Starting headless Smartlead upload for %d inboxes...", len(inboxes))

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

logger.info("Upload result: %s", result)
print("\n=== RESULT ===")
print(json.dumps(result, indent=2, default=str))

failed = result.get("failed_uploads") or []
if failed:
    raise RuntimeError(f"Headless upload completed with failed inbox validations: {failed}")
