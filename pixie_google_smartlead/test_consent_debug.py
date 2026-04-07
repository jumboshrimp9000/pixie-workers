"""
Debug: Run one inbox through the production code with screenshots at consent stage.
Uses the FIXED production code path.
"""
import os
import sys
import logging
import time
from pathlib import Path

env_file = Path(__file__).parent / ".env.worker"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent))

from app.workers.sending_tool_uploader import SendingToolUploader
from app.workers.onepassword_client import OnePasswordCliClient

SCREENSHOT_DIR = Path(__file__).parent / "debug_screenshots"
SCREENSHOT_DIR.mkdir(exist_ok=True)

# Monkey-patch _complete_google_consent to add logging
original_consent = SendingToolUploader._complete_google_consent

step_num = 0
def ss(pg, name):
    global step_num
    step_num += 1
    path = SCREENSHOT_DIR / f"consent_{step_num:02d}_{name}.png"
    try:
        pg.screenshot(path=str(path))
        logger.info("Screenshot: %s", path.name)
    except Exception as e:
        logger.warning("Screenshot failed: %s", e)

def patched_consent(self, page):
    logger.info("=== _complete_google_consent called ===")
    logger.info("URL before consent: %s", page.url)
    ss(page, "before_consent")

    for i in range(6):
        logger.info("Consent iteration %d, URL: %s", i, page.url)
        clicked = False

        # Try Continue
        try:
            cont = self._click_any(
                page,
                ["//span[text()='Continue']", "//button//span[text()='Continue']", "//div[text()='Continue']"],
                timeout_ms=4_000,
                optional=True,
            )
            if cont:
                logger.info("Clicked 'Continue'")
                clicked = True
                time.sleep(1.0)
                ss(page, f"after_continue_{i}")
        except Exception as e:
            logger.warning("Continue click error: %s", e)

        # Try Allow
        try:
            allow = self._click_any(
                page,
                ["//span[text()='Allow']", "//button//span[text()='Allow']", "//div[text()='Allow']"],
                timeout_ms=4_000,
                optional=True,
            )
            if allow:
                logger.info("Clicked 'Allow'")
                clicked = True
                time.sleep(1.0)
                ss(page, f"after_allow_{i}")
        except Exception as e:
            logger.warning("Allow click error: %s", e)

        # Try I understand
        try:
            understand = self._click_any(
                page,
                ["//input[@value='I understand']", "//span[text()='I understand']"],
                timeout_ms=4_000,
                optional=True,
            )
            if understand:
                logger.info("Clicked 'I understand'")
                clicked = True
                time.sleep(1.0)
                ss(page, f"after_understand_{i}")
        except Exception as e:
            logger.warning("I understand click error: %s", e)

        if not clicked:
            logger.info("No consent button clicked on iteration %d", i)
            break
        time.sleep(0.8)

    logger.info("URL after consent: %s", page.url)
    ss(page, "after_consent_done")

    # Wait for potential redirect
    time.sleep(3.0)
    logger.info("URL after 3s wait: %s", page.url)
    ss(page, "after_consent_wait")

SendingToolUploader._complete_google_consent = patched_consent

# Also patch _complete_google_signin_and_consent to log post-consent state
original_signin = SendingToolUploader._complete_google_signin_and_consent

def patched_signin(self, *, page, context, email, password, onepassword):
    # Call the real (fixed) method but capture state
    clean_email = self._normalize_email(email)
    logger.info("=== Starting signin for %s ===", clean_email)

    # Run the real fixed code
    original_signin(self, page=page, context=context, email=email, password=password, onepassword=onepassword)

    logger.info("=== Signin completed for %s ===", clean_email)
    logger.info("Final URL: %s", page.url if not page.is_closed() else "CLOSED")
    ss(page, "signin_complete")

    # Check all pages in context
    for i, p in enumerate(context.pages):
        try:
            logger.info("Context page %d: url=%s closed=%s", i, p.url, p.is_closed())
        except Exception:
            logger.info("Context page %d: could not read", i)

SendingToolUploader._complete_google_signin_and_consent = patched_signin

# Now run through production path
from app.workers.google_supabase_worker import GoogleSupabaseWorker

DOMAIN_ID = "df3e7522-799e-4177-a4aa-a9cd5f54c88d"

worker = GoogleSupabaseWorker()
inboxes = worker.client.get_domain_inboxes(DOMAIN_ID)
inboxes = inboxes[:1]  # Just first inbox
logger.info("Testing with inbox: %s", inboxes[0].get("email"))

result = worker._upload_domain_inboxes_to_sending_tool(
    domain_id=DOMAIN_ID,
    domain_name="whoistherightthing.xyz",
    provider="google",
    inboxes=inboxes,
    user_updates={},
)

import json
print(json.dumps(result, indent=2, default=str))
