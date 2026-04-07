"""
Minimal test: get to consent page, dump HTML of the Allow button area.
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
from app.workers.sending_tool_uploader import SendingToolUploader

SCREENSHOT_DIR = Path(__file__).parent / "debug_screenshots"
SCREENSHOT_DIR.mkdir(exist_ok=True)

op_client = OnePasswordCliClient.from_env()

smartlead_username = "iknoor+vendors@smartlead.ai"
smartlead_password = "Test1234!"
inbox_email = "michah.heathrow@whoistherightthing.xyz"
inbox_password = "WhoIsRightThing84!"

from playwright.sync_api import sync_playwright

pw = sync_playwright().start()
browser = pw.chromium.launch(headless=True)
context = browser.new_context(
    viewport={"width": 1280, "height": 800},
    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
)
page = context.new_page()
page.set_default_timeout(30000)

# Login to Smartlead
page.goto("https://app.smartlead.ai/login", wait_until="networkidle", timeout=30000)
page.fill("input[type='email'], input[placeholder*='email' i]", smartlead_username, timeout=10000)
page.fill("input[type='password'], input[placeholder*='password' i]", smartlead_password, timeout=10000)
page.click("button:has-text('Login'), button:has-text('Sign In'), button[type='submit']", timeout=10000)
page.wait_for_url("**/app/**", timeout=30000)
logger.info("Logged in to Smartlead")

# Navigate to email accounts, connect mailbox
page.goto("https://app.smartlead.ai/app/email-accounts/emails", wait_until="domcontentloaded", timeout=30000)
time.sleep(2)
page.click("//span[text()='Connect Mailbox']", timeout=20000)
time.sleep(1)
page.click("//div[contains(@class,'infrastructure-card') and contains(@class,'smartlead-infrastructure')]", timeout=20000)
time.sleep(0.5)
page.click("//p[normalize-space()='Google OAuth']", timeout=20000)
time.sleep(0.5)

# Click Connect Account
before_pages = list(context.pages)
page.click("//button[contains(.,'Connect Account')]", timeout=20000)

# Detect OAuth page
end_at = time.time() + 10
oauth_page = page
while time.time() < end_at:
    for p in context.pages:
        if id(p) not in {id(x) for x in before_pages}:
            try:
                p.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass
            oauth_page = p
            break
    if "accounts.google.com" in str(getattr(oauth_page, "url", "") or "").lower():
        break
    time.sleep(0.3)

logger.info("OAuth page URL: %s", oauth_page.url[:100])
time.sleep(2)

# Email
for _ in range(5):
    if oauth_page.locator("input#identifierId").count() > 0:
        break
    time.sleep(1)
oauth_page.fill("input#identifierId", inbox_email, timeout=10000)
oauth_page.click("#identifierNext", timeout=15000)
time.sleep(2)

# Password
for _ in range(5):
    if oauth_page.locator("input[name='Passwd']").count() > 0:
        break
    time.sleep(1)
oauth_page.fill("input[name='Passwd']", inbox_password, timeout=10000)
oauth_page.click("#passwordNext", timeout=15000)
time.sleep(3)

# TOTP
totp_selectors = ["input[name='totpPin']", "input[type='tel']", "input[inputmode='numeric']"]
has_totp = any(oauth_page.locator(sel).count() > 0 for sel in totp_selectors)
if has_totp:
    item = op_client.find_google_login_item(inbox_email)
    totp_code = op_client.get_totp(item.get("id", ""))
    logger.info("TOTP code: %s", totp_code)
    for sel in totp_selectors:
        if oauth_page.locator(sel).count() > 0:
            oauth_page.fill(sel, totp_code, timeout=10000)
            break
    try:
        oauth_page.click("#totpNext", timeout=5000)
    except Exception:
        oauth_page.click("xpath=//span[text()='Next']", timeout=5000)
    time.sleep(3)

# Now we should be on the consent page
logger.info("=== CONSENT PAGE ===")
logger.info("URL: %s", oauth_page.url[:100])
oauth_page.screenshot(path=str(SCREENSHOT_DIR / "html_consent_page.png"))

# Dump the HTML around the buttons
try:
    html = oauth_page.content()
    # Find the Allow button area
    import re
    # Look for Allow/Deny in the HTML
    for pattern in ['Allow', 'Deny', 'Continue', 'submit', 'button']:
        matches = re.findall(f'.{{0,200}}{pattern}.{{0,200}}', html, re.IGNORECASE)
        for m in matches[:3]:
            logger.info("HTML match for '%s': %s", pattern, m.strip()[:300])
except Exception as e:
    logger.error("Failed to dump HTML: %s", e)

# Try various selectors
test_selectors = [
    ("xpath=//button[normalize-space()='Allow']", "xpath button normalize-space"),
    ("xpath=//span[text()='Allow']", "xpath span text"),
    ("xpath=//button//span[text()='Allow']", "xpath button//span text"),
    ("xpath=//*[text()='Allow']", "xpath any text"),
    ("xpath=//button[contains(text(),'Allow')]", "xpath button contains"),
    ("text=Allow", "text selector"),
    ("button:has-text('Allow')", "css has-text"),
    ("xpath=//button[@id='submit_approve_access']", "xpath submit button"),
]

for selector, name in test_selectors:
    try:
        count = oauth_page.locator(selector).count()
        logger.info("Selector '%s' (%s): count=%d", selector, name, count)
    except Exception as e:
        logger.info("Selector '%s' (%s): ERROR %s", selector, name, e)

browser.close()
pw.stop()
