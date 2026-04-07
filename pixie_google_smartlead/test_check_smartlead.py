"""Quick headless check: are the inboxes in Smartlead?"""
import time
import logging
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

pw = sync_playwright().start()
browser = pw.chromium.launch(headless=True)
page = browser.new_page(viewport={"width": 1280, "height": 800})

# Login
page.goto("https://app.smartlead.ai/login", wait_until="networkidle", timeout=30000)
page.fill("input[type='email']", "iknoor+vendors@smartlead.ai", timeout=10000)
page.fill("input[type='password']", "Test1234!", timeout=10000)
page.click("button[type='submit']", timeout=10000)
page.wait_for_url("**/app/**", timeout=30000)
logger.info("Logged in")

# Go to email accounts
page.goto("https://app.smartlead.ai/app/email-accounts/emails", wait_until="domcontentloaded", timeout=30000)
time.sleep(3)

# Search for our emails
for email in ["michah.heathrow", "london.breed", "whoistherightthing"]:
    search_box = page.locator("input[placeholder*='Search']").first
    search_box.fill(email, timeout=5000)
    time.sleep(2)

    # Check for results
    body_text = page.inner_text("body")
    if "whoistherightthing" in body_text.lower():
        logger.info("FOUND '%s' in page!", email)
    else:
        logger.info("NOT FOUND: '%s'", email)

    # Clear search
    search_box.fill("", timeout=5000)
    time.sleep(1)

# Also get full page text to see all email accounts
rows = page.locator("table tbody tr").all()
logger.info("Total email account rows: %d", len(rows))
for i, row in enumerate(rows[:20]):
    try:
        text = row.inner_text()
        logger.info("Row %d: %s", i, text[:200])
    except Exception:
        pass

browser.close()
pw.stop()
