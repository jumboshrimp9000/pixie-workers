"""
Debug test: Run headless Google OAuth upload for ONE inbox with screenshots.
Uses the same popup detection as the production code.
"""
import os
import sys
import time
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent))
from app.workers.onepassword_client import OnePasswordCliClient

SCREENSHOT_DIR = Path(__file__).parent / "debug_screenshots"
SCREENSHOT_DIR.mkdir(exist_ok=True)

op_client = OnePasswordCliClient.from_env()

smartlead_username = "iknoor+vendors@smartlead.ai"
smartlead_password = "Test1234!"
inbox_email = "london.breed@whoistherightthing.xyz"
inbox_password = "WhoIsRightThing84!"

step_counter = 0
def screenshot(pg, name):
    global step_counter
    step_counter += 1
    path = SCREENSHOT_DIR / f"{step_counter:02d}_{name}.png"
    try:
        pg.screenshot(path=str(path))
        logger.info("Screenshot: %s", path.name)
    except Exception as e:
        logger.warning("Screenshot failed: %s", e)


def has_google_password_rejection(pg) -> bool:
    try:
        text = (pg.inner_text("body") or "").lower()
    except Exception:
        return False
    return ("wrong password" in text) or ("too many failed attempts" in text)


def detect_oauth_page(context, fallback_page, previous_pages):
    """Same logic as production _detect_oauth_page"""
    previous_ids = {id(p) for p in previous_pages}
    end_at = time.time() + 10
    while time.time() < end_at:
        for p in context.pages:
            if id(p) not in previous_ids:
                try:
                    p.wait_for_load_state("domcontentloaded", timeout=5000)
                except Exception:
                    pass
                return p
        url = str(getattr(fallback_page, "url", "") or "").lower()
        if "accounts.google.com" in url:
            return fallback_page
        time.sleep(0.3)
    return fallback_page

from playwright.sync_api import sync_playwright

pw = sync_playwright().start()
browser = pw.chromium.launch(headless=True)
context = browser.new_context(
    viewport={"width": 1280, "height": 800},
    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
)
page = context.new_page()
page.set_default_timeout(30000)

# Step 1: Login to Smartlead
logger.info("=== Step 1: Login to Smartlead ===")
page.goto("https://app.smartlead.ai/login", wait_until="networkidle", timeout=30000)
page.fill("input[type='email'], input[placeholder*='email' i]", smartlead_username, timeout=10000)
page.fill("input[type='password'], input[placeholder*='password' i]", smartlead_password, timeout=10000)
page.click("button:has-text('Login'), button:has-text('Sign In'), button[type='submit']", timeout=10000)
page.wait_for_url("**/app/**", timeout=30000)
screenshot(page, "smartlead_logged_in")

# Step 2: Navigate to Email Accounts
logger.info("=== Step 2: Email Accounts page ===")
page.goto("https://app.smartlead.ai/app/email-accounts/emails", wait_until="domcontentloaded", timeout=30000)
time.sleep(2)
screenshot(page, "email_accounts")

# Step 3: Click Connect Mailbox
logger.info("=== Step 3: Connect Mailbox ===")
page.click("//span[text()='Connect Mailbox']", timeout=20000)
time.sleep(1)
screenshot(page, "connect_mailbox_dialog")

# Step 4: Select Smartlead Infrastructure
logger.info("=== Step 4: Smartlead Infrastructure ===")
page.click("//div[contains(@class,'infrastructure-card') and contains(@class,'smartlead-infrastructure')]", timeout=20000)
time.sleep(0.5)

# Step 5: Select Google OAuth
logger.info("=== Step 5: Google OAuth ===")
page.click("//p[normalize-space()='Google OAuth']", timeout=20000)
time.sleep(0.5)
screenshot(page, "google_oauth_selected")

# Step 6: Click Connect Account (popup detection like production)
logger.info("=== Step 6: Connect Account (detecting OAuth page) ===")
before_pages = list(context.pages)
page.click("//button[contains(.,'Connect Account')]", timeout=20000)
oauth_page = detect_oauth_page(context, page, before_pages)
is_same_page = (oauth_page is page)
logger.info("OAuth page detected. Same page? %s. URL: %s", is_same_page, oauth_page.url)
time.sleep(2)
screenshot(oauth_page, "oauth_page_initial")

# Step 7: Email entry
logger.info("=== Step 7: Email entry ===")
for _ in range(5):
    if oauth_page.locator("input#identifierId").count() > 0:
        break
    if oauth_page.locator("input[type='email']").count() > 0:
        break
    time.sleep(1)
screenshot(oauth_page, "before_email_entry")

if oauth_page.locator("input#identifierId").count() > 0:
    oauth_page.fill("input#identifierId", inbox_email, timeout=10000)
    screenshot(oauth_page, "email_filled")
    oauth_page.click("#identifierNext", timeout=15000)
    time.sleep(2)
    screenshot(oauth_page, "after_email_next")
elif oauth_page.locator("input[type='email']").count() > 0:
    oauth_page.fill("input[type='email']", inbox_email, timeout=10000)
    oauth_page.click("//span[text()='Next']", timeout=15000)
    time.sleep(2)
    screenshot(oauth_page, "after_email_next")
else:
    screenshot(oauth_page, "NO_EMAIL_FIELD")
    logger.error("No email field found! URL: %s", oauth_page.url)

# Step 8: Password entry
logger.info("=== Step 8: Password entry ===")
time.sleep(1)
screenshot(oauth_page, "before_password")

for _ in range(5):
    if oauth_page.locator("input[name='Passwd']").count() > 0:
        break
    if oauth_page.locator("input[type='password']").count() > 0:
        break
    time.sleep(1)

if oauth_page.locator("input[name='Passwd']").count() > 0:
    oauth_page.fill("input[name='Passwd']", inbox_password, timeout=10000)
    screenshot(oauth_page, "password_filled")
    oauth_page.click("#passwordNext", timeout=15000)
    time.sleep(3)
    if has_google_password_rejection(oauth_page):
        screenshot(oauth_page, "password_rejected")
        raise RuntimeError("Google rejected password challenge in headless debug run")
    screenshot(oauth_page, "after_password_next")
elif oauth_page.locator("input[type='password']").count() > 0:
    oauth_page.fill("input[type='password']", inbox_password, timeout=10000)
    oauth_page.click("//span[text()='Next']", timeout=15000)
    time.sleep(3)
    if has_google_password_rejection(oauth_page):
        screenshot(oauth_page, "password_rejected")
        raise RuntimeError("Google rejected password challenge in headless debug run")
    screenshot(oauth_page, "after_password_next")
else:
    screenshot(oauth_page, "NO_PASSWORD_FIELD")
    logger.error("No password field found! URL: %s", oauth_page.url)
    # Dump page text for debugging
    try:
        body_text = oauth_page.inner_text("body")[:3000]
        logger.info("Page body text:\n%s", body_text)
    except Exception:
        pass

# Step 9: Check state after password
logger.info("=== Step 9: Post-password state ===")
current_url = oauth_page.url
logger.info("Current URL: %s", current_url)
screenshot(oauth_page, "post_password_state")

# Check for TOTP
totp_selectors = ["input[name='totpPin']", "input[type='tel']", "input[inputmode='numeric']", "input[autocomplete='one-time-code']"]
has_totp = any(oauth_page.locator(sel).count() > 0 for sel in totp_selectors)

if has_totp:
    logger.info("=== TOTP page detected ===")
    item = op_client.find_google_login_item(inbox_email)
    if item:
        totp_code = op_client.get_totp(item.get("id", ""))
        logger.info("TOTP code: %s", totp_code)
        for sel in totp_selectors:
            if oauth_page.locator(sel).count() > 0:
                oauth_page.fill(sel, totp_code, timeout=10000)
                break
        screenshot(oauth_page, "totp_filled")
        # Try CSS first, then XPath
        try:
            oauth_page.click("#totpNext", timeout=5000)
        except Exception:
            oauth_page.click("//span[text()='Next']", timeout=5000)
        time.sleep(3)
        if has_google_password_rejection(oauth_page):
            screenshot(oauth_page, "totp_or_password_rejected")
            raise RuntimeError("Google rejected challenge after TOTP submission")
        screenshot(oauth_page, "after_totp")

# Step 10: Consent
logger.info("=== Step 10: Consent ===")
screenshot(oauth_page, "pre_consent")
current_url = oauth_page.url
logger.info("URL: %s", current_url)

try:
    oauth_page.wait_for_load_state("domcontentloaded", timeout=8_000)
except Exception:
    pass
time.sleep(1.5)

for _ in range(4):
    clicked = False
    for label in ("Continue", "Allow", "I understand"):
        try:
            btn = oauth_page.get_by_role("button", name=label, exact=True)
            if btn.count() > 0:
                btn.first.click(timeout=5_000)
                clicked = True
                time.sleep(1.5)
                screenshot(oauth_page, f"after_{label.lower().replace(' ', '_')}")
                continue
        except Exception:
            pass
        try:
            if oauth_page.locator(f"//span[text()='{label}']").count() > 0:
                oauth_page.click(f"//span[text()='{label}']", timeout=4_000)
                clicked = True
                time.sleep(1.5)
                screenshot(oauth_page, f"after_{label.lower().replace(' ', '_')}")
        except Exception:
            pass
    if not clicked:
        break

# Final
logger.info("=== FINAL STATE ===")
try:
    logger.info("OAuth page URL: %s", oauth_page.url if not oauth_page.is_closed() else "CLOSED")
except Exception:
    logger.info("OAuth page closed")
logger.info("Main page URL: %s", page.url)
screenshot(page, "final_main_page")

browser.close()
pw.stop()
logger.info("Done. Screenshots: %s", SCREENSHOT_DIR)
