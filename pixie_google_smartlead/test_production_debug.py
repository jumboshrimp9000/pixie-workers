"""
Instrument the production _complete_google_signin_and_consent to find where it gets stuck.
Monkey-patches logging into the key methods.
"""
import os
import sys
import logging
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

# Monkey-patch _complete_google_signin_and_consent with verbose logging
original_complete = SendingToolUploader._complete_google_signin_and_consent

import time

def patched_complete(self, *, page, context, email, password, onepassword):
    clean_email = self._normalize_email(email)
    max_password_submissions = max(1, int(os.getenv("GOOGLE_OAUTH_MAX_PASSWORD_SUBMISSIONS", "2")))
    password_submissions = 0

    step_num = 0
    def ss(name):
        nonlocal step_num
        step_num += 1
        path = SCREENSHOT_DIR / f"prod_{step_num:02d}_{name}.png"
        try:
            page.screenshot(path=str(path))
            logger.info("Screenshot: %s", path.name)
        except Exception as e:
            logger.warning("Screenshot failed: %s", e)

    ss("initial_state")
    logger.info("URL: %s", page.url)

    for iteration in range(14):
        logger.info("=== Iteration %d ===", iteration)
        logger.info("URL: %s", page.url)

        is_prompt = self._is_google_signin_prompt(page)
        logger.info("_is_google_signin_prompt: %s", is_prompt)
        if not is_prompt:
            logger.info("Not a signin prompt, breaking loop")
            break

        # Check for account selector
        clicked_account = self._click_any(
            page,
            [
                f"//div[@data-identifier='{clean_email}']",
                f"//li[contains(., '{clean_email}')]",
                f"//div[contains(text(), '{clean_email}')]",
            ],
            timeout_ms=2_500,
            optional=True,
        )
        if clicked_account:
            logger.info("Clicked account selector for %s", clean_email)
            time.sleep(1.0)
            ss("after_account_click")
            continue

        # "Use another account"
        self._click_any(
            page,
            ["//div[text()='Use another account']", "//button//span[text()='Use another account']"],
            timeout_ms=2_500,
            optional=True,
        )

        # Email field
        has_email = self._exists(page, ["input#identifierId", "input[type='email']"], timeout_ms=2_500)
        logger.info("Has email field: %s", has_email)
        if has_email:
            logger.info("Filling email: %s", clean_email)
            self._fill_any(page, ["input#identifierId", "input[type='email']"], clean_email, timeout_ms=20_000)
            ss("email_filled")
            self._click_any(
                page,
                ["#identifierNext", "//*[@id='identifierNext']", "//span[text()='Next']"],
                timeout_ms=15_000,
            )
            time.sleep(1.0)
            ss("after_email_next")
            continue

        # Password field
        has_password = self._exists(page, ["input[name='Passwd']", "input[type='password']"], timeout_ms=2_500)
        logger.info("Has password field: %s", has_password)
        if has_password:
            logger.info("Filling password")
            self._fill_any(
                page, ["input[name='Passwd']", "input[type='password']"], password, timeout_ms=20_000
            )
            ss("password_filled")
            self._click_any(
                page,
                ["#passwordNext", "//*[@id='passwordNext']", "//span[text()='Next']"],
                timeout_ms=15_000,
            )
            password_submissions += 1
            time.sleep(2.2)
            ss("after_password_next")

            if self._exists(page, ["input[name='Passwd']", "input[type='password']"], timeout_ms=1_000):
                challenge_error = self._read_google_password_challenge_error(page)
                if challenge_error:
                    raise RuntimeError(f"Google password challenge rejected for {clean_email}: {challenge_error}")
                if password_submissions >= max_password_submissions:
                    raise RuntimeError(
                        f"Google password challenge did not advance for {clean_email} after "
                        f"{password_submissions} submission(s)"
                    )
            continue

        logger.info("No email/password field found. Trying fallback selectors...")

        # Try another way
        self._click_any(
            page,
            ["//span[text()='Try another way']", "//button//span[text()='Try another way']", "//span[contains(text(),'Try another')]"],
            timeout_ms=2_500,
            optional=True,
        )
        self._click_any(
            page,
            ["//span[contains(text(),'Enter your password')]", "//span[contains(text(),'Use your password')]", "//div[contains(text(),'Enter your password')]"],
            timeout_ms=3_000,
            optional=True,
        )

        # TOTP
        totp_selectors = ["input[name='totpPin']", "input[type='tel']", "input[inputmode='numeric']", "input[autocomplete='one-time-code']", "input[aria-label*='code']"]
        has_totp = self._exists(page, totp_selectors, timeout_ms=2_500)
        logger.info("Has TOTP field: %s", has_totp)
        if has_totp:
            code = self._get_totp_code(onepassword, clean_email)
            logger.info("TOTP code: %s", code)
            if not code:
                raise RuntimeError(f"Google requested TOTP for {clean_email} but no 1Password code available")
            self._fill_any(page, totp_selectors, code, timeout_ms=15_000)
            ss("totp_filled")
            self._click_any(
                page,
                ["#totpNext", "//span[text()='Next']", "//span[text()='Verify']", "//button//span[text()='Next']"],
                timeout_ms=10_000,
                optional=True,
            )
            time.sleep(1.0)
            ss("after_totp_next")
            continue

        ss(f"unknown_state_iter{iteration}")
        logger.warning("Unknown state at iteration %d. Page text (first 500): %s",
                       iteration, page.inner_text("body")[:500] if not page.is_closed() else "CLOSED")
        time.sleep(0.8)

    ss("pre_consent")
    self._complete_google_consent(page)
    ss("post_consent")

    if self._is_google_signin_prompt(page):
        ss("STILL_ON_SIGNIN")
        raise RuntimeError(f"Google OAuth sign-in did not complete for {clean_email}. Current URL: {page.url}")

    self._close_non_primary_pages(context, page)
    ss("final")
    logger.info("SUCCESS for %s", clean_email)

SendingToolUploader._complete_google_signin_and_consent = patched_complete

# Now run the production path
from app.workers.google_supabase_worker import GoogleSupabaseWorker

DOMAIN_ID = "df3e7522-799e-4177-a4aa-a9cd5f54c88d"

worker = GoogleSupabaseWorker()
inboxes = worker.client.get_domain_inboxes(DOMAIN_ID)
# Only test with first inbox to be faster
inboxes = inboxes[:1]
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
