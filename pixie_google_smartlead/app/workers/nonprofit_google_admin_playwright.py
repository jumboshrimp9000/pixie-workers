import asyncio
import json
import logging
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlparse

from playwright.async_api import Page

from app.workers.nonprofit_google_panel_client import NonprofitGooglePanelClient
from app.workers.onepassword_client import OnePasswordCliClient


logger = logging.getLogger(__name__)


def _log(log: Optional[Callable[[str], None]], message: str) -> None:
    if callable(log):
        log(message)
    else:
        logger.info(message)


def _truthy(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


async def _visible(locator: Any, timeout_ms: int = 2000) -> bool:
    try:
        return await locator.is_visible(timeout=timeout_ms)
    except Exception:
        return False


async def _click_button(page: Page, names: list[str], *, exact: bool = False, timeout_ms: int = 1500) -> bool:
    for name in names:
        button = page.get_by_role("button", name=name, exact=exact)
        if await _visible(button.first, timeout_ms):
            await button.first.click()
            return True
    return False


def _panel_client_from_credentials(panel_credentials: Dict[str, Any]) -> NonprofitGooglePanelClient:
    return NonprofitGooglePanelClient(str(panel_credentials.get("apps_script_url") or "").strip())


def _safe_debug_slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", str(value or "").strip())[:120] or "debug"


async def _capture_debug_artifacts(
    page: Page,
    tag: str,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    debug_dir = Path(os.getenv("GOOGLE_PLAYWRIGHT_DEBUG_DIR") or "app/logs/playwright_debug")
    try:
        debug_dir.mkdir(parents=True, exist_ok=True)
        prefix = debug_dir / f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}_{_safe_debug_slug(tag)}"
        screenshot_path = f"{prefix}.png"
        html_path = f"{prefix}.html"
        text_path = f"{prefix}.txt"
        await page.screenshot(path=screenshot_path, full_page=True)
        html_path_obj = Path(html_path)
        text_path_obj = Path(text_path)
        html_path_obj.write_text(await page.content(), encoding="utf-8")
        text_path_obj.write_text(await page.text_content("body") or "", encoding="utf-8")
        result = {
            "url": page.url,
            "screenshot": screenshot_path,
            "html": html_path,
            "text": text_path,
        }
        _log(log, f"  [debug] captured {result}")
        return result
    except Exception as exc:
        return {"url": page.url, "error": f"debug_capture_failed: {exc}"}


def _generate_totp_from_secret(raw_secret: str) -> str:
    """Generate a current TOTP code from a raw base32 secret.

    Accepts secrets with optional spaces (as stored in Airtable for readability).
    Uses pyotp so we avoid hitting the 1Password rate-limited vault for admin logins.
    """
    try:
        import pyotp  # type: ignore
    except ImportError as exc:
        raise RuntimeError("pyotp is required to generate admin TOTP codes") from exc

    normalized = str(raw_secret or "").replace(" ", "").strip()
    if not normalized:
        raise RuntimeError("TOTP secret is empty")
    return pyotp.TOTP(normalized).now()


def _get_otp_from_item_reference(
    op_client: Optional[OnePasswordCliClient],
    *,
    item_id: str = "",
    item_title: str = "",
    email_fallback: str = "",
    totp_secret: str = "",
) -> str:
    # Prefer raw TOTP secret (stored on nonprofit_panels). No 1Password rate limits.
    clean_totp = str(totp_secret or "").strip()
    if clean_totp:
        return _generate_totp_from_secret(clean_totp)

    if op_client is None:
        raise RuntimeError("1Password client is required for TOTP retrieval")

    clean_item_id = str(item_id or "").strip()
    if clean_item_id:
        return op_client.get_totp(clean_item_id)

    clean_title = str(item_title or "").strip()
    if clean_title:
        rows = op_client._run_op_json(  # type: ignore[attr-defined]
            [
                "item",
                "list",
                "--vault",
                op_client.vault,
                "--categories",
                "Login",
                "--format",
                "json",
            ]
        )
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if str(row.get("title") or "").strip().lower() != clean_title.lower():
                    continue
                found_id = str(row.get("id") or "").strip()
                if found_id:
                    return op_client.get_totp(found_id)

    clean_email = str(email_fallback or "").strip().lower()
    if clean_email:
        item = op_client.find_google_login_item(clean_email)
        if item and str(item.get("id") or "").strip():
            return op_client.get_totp(str(item.get("id") or "").strip())

    raise RuntimeError("Could not resolve 1Password item for OTP retrieval")


async def handle_phone_verification(
    page: Page,
    *,
    phone_field: Any,
    log: Optional[Callable[[str], None]] = None,
) -> None:
    api_key = str(os.getenv("SMSPOOL_API_KEY") or "").strip()
    service_id = str(os.getenv("SMSPOOL_SERVICE_ID") or "395").strip()
    if not api_key:
        raise RuntimeError("Phone verification required but SMSPOOL_API_KEY is not configured")

    for attempt in range(1, 4):
        _log(log, f"    [auth] phone verification attempt {attempt}/3")
        purchase = await page.request.get(
            f"https://api.smspool.net/purchase/sms?key={api_key}&country=1&service={service_id}"
        )
        payload = await purchase.json()
        if not payload.get("success"):
            await page.wait_for_timeout(30_000)
            continue

        phone_number = str(payload.get("phonenumber") or "").strip()
        order_id = str(payload.get("order_id") or "").strip()
        if not phone_number or not order_id:
            await page.wait_for_timeout(5_000)
            continue

        await phone_field.fill(phone_number)
        await _click_button(page, ["Next"], timeout_ms=4000)
        await page.wait_for_timeout(3000)

        otp_field = page.get_by_role("textbox", name=re.compile(r"Enter code", re.I))
        if not await _visible(otp_field.first, 10_000):
            continue

        for _ in range(72):
            check = await page.request.get(f"https://api.smspool.net/sms/check?key={api_key}&orderid={order_id}")
            result = await check.json()
            if int(result.get("status") or 0) == 3 and str(result.get("sms") or "").strip():
                await otp_field.first.fill(str(result.get("sms")).strip())
                await _click_button(page, ["Next"], timeout_ms=4000)
                await page.wait_for_timeout(3000)
                return
            await page.wait_for_timeout(5000)

        await page.go_back()
        await page.wait_for_timeout(3000)

    raise RuntimeError("Phone verification failed after 3 attempts")


async def handle_interstitials(page: Page, log: Optional[Callable[[str], None]] = None) -> bool:
    try:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(500)
    except Exception:
        pass

    submit_input = page.locator('input[type="submit"]')
    if await _visible(submit_input.first, 1500):
        await submit_input.first.click()
        await page.wait_for_timeout(3000)
        return True

    accept = page.get_by_role("button", name="Accept")
    if await _visible(accept.first, 1000):
        for checkbox in await page.locator('input[type="checkbox"]').all():
            try:
                if await checkbox.is_visible() and not await checkbox.is_checked():
                    await checkbox.click(force=True)
            except Exception:
                pass
        await accept.first.click()
        await page.wait_for_timeout(3000)
        return True

    for name in ["Accept Terms of Service", "Continue", "I agree", "Done", "Confirm", "Got it", "Not now", "Dismiss", "Get started", "Next", "OK"]:
        if await _click_button(page, [name], exact=True, timeout_ms=800):
            _log(log, f"    [auth] interstitial clicked: {name}")
            await page.wait_for_timeout(3000)
            return True

    return False


async def handle_google_auth(
    page: Page,
    auth_config: Dict[str, Any],
    *,
    op_client: Optional[OnePasswordCliClient],
    log: Optional[Callable[[str], None]] = None,
) -> bool:
    email = str(auth_config.get("email") or "").strip()
    password = str(auth_config.get("password") or "").strip()
    item_id = str(auth_config.get("op_item_id") or "").strip()
    item_title = str(auth_config.get("op_item_title") or auth_config.get("totp_item_name") or "").strip()
    totp_secret = str(auth_config.get("totp_secret") or "").strip()

    last_url = ""
    for attempt in range(15):
        await page.wait_for_timeout(2000)
        url = page.url
        if url == last_url:
            await page.wait_for_timeout(2000)
        last_url = url

        if "twosvrequired" in url or "two-step-verification/enroll" in url:
            if await _visible(page.get_by_role("link", name="Do this later").first, 2000):
                await page.get_by_role("link", name="Do this later").first.click()
                await page.wait_for_timeout(3000)
                continue
            if await _visible(page.get_by_role("link", name="Enroll").first, 2000):
                await page.get_by_role("link", name="Enroll").first.click()
                await page.wait_for_timeout(3000)
                continue

        if "/signin/rejected" in url:
            raise RuntimeError("Google signin/rejected - cannot authenticate")

        if (("accounts.google.com" not in url) or ("myaccount.google.com" in url)) and "/signin/" not in url:
            return True

        use_another = page.locator("text=Use another account")
        if await _visible(use_another.first, 1500):
            await use_another.first.click()
            await page.wait_for_timeout(2000)
            continue

        email_field = page.get_by_role("textbox", name="Email or phone")
        if await _visible(email_field.first, 1500):
            await email_field.first.fill(email)
            await _click_button(page, ["Next"], timeout_ms=4000)
            await page.wait_for_timeout(3000)
            continue

        password_field = page.get_by_role("textbox", name="Enter your password")
        if not await _visible(password_field.first, 800):
            password_field = page.locator("input[name='Passwd']")
        if await _visible(password_field.first, 1500):
            await password_field.first.fill(password)
            await _click_button(page, ["Next"], timeout_ms=4000)
            await page.wait_for_timeout(3000)
            continue

        totp_field = page.get_by_role("textbox", name=re.compile(r"Enter code", re.I))
        if not await _visible(totp_field.first, 800):
            totp_field = page.locator("input[name='totpPin']")
        if await _visible(totp_field.first, 1500):
            code = await asyncio.to_thread(
                _get_otp_from_item_reference,
                op_client,
                item_id=item_id,
                item_title=item_title,
                email_fallback=email,
                totp_secret=totp_secret,
            )
            await totp_field.first.fill(code)
            await _click_button(page, ["Next"], timeout_ms=4000)
            await page.wait_for_timeout(3000)
            continue

        phone_field = page.locator('input[type="tel"]')
        if await _visible(phone_field.first, 1500):
            await handle_phone_verification(page, phone_field=phone_field.first, log=log)
            continue

        submit_input = page.locator('input[type="submit"]')
        if await _visible(submit_input.first, 1500):
            await submit_input.first.click()
            await page.wait_for_timeout(3000)
            continue

        gcp_tos = page.get_by_role("checkbox", name=re.compile(r"I agree", re.I))
        if await _visible(gcp_tos.first, 1000):
            await gcp_tos.first.click()
            await _click_button(page, ["Agree and continue"], timeout_ms=2000)
            await page.wait_for_timeout(3000)
            continue

        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)
        except Exception:
            pass

        for name in ["I understand", "Next", "Continue", "Done", "Accept", "I agree", "Confirm", "Not now", "Accept Terms of Service", "Got it", "Dismiss"]:
            if await _click_button(page, [name], timeout_ms=500):
                await page.wait_for_timeout(3000)
                break
        else:
            _log(log, f"    [auth] still waiting on Google auth flow: {url[:120]}")

    return False


async def _ensure_admin_session(
    page: Page,
    panel_credentials: Dict[str, Any],
    *,
    op_client: Optional[OnePasswordCliClient],
    log: Optional[Callable[[str], None]] = None,
) -> None:
    if op_client is None:
        maybe_client = panel_credentials.get("_op_client")
        if isinstance(maybe_client, OnePasswordCliClient):
            op_client = maybe_client

    auth_config = {
        "email": panel_credentials.get("admin_email"),
        "password": panel_credentials.get("admin_password"),
        "op_item_id": panel_credentials.get("op_item_id") or panel_credentials.get("op_item_uuid"),
        "op_item_title": panel_credentials.get("op_item_title") or panel_credentials.get("op_totp_item"),
        "totp_secret": panel_credentials.get("totp_secret") or "",
    }

    await page.goto("https://accounts.google.com/logout")
    await page.wait_for_timeout(3000)
    await page.goto("https://admin.google.com", timeout=30_000)
    await page.wait_for_timeout(3000)

    for idx in range(15):
        await page.wait_for_timeout(2000)
        url = page.url
        if "admin.google.com/ac/" in url:
            return
        if "accounts.google.com" in url or "accountchooser" in url:
            ok = await handle_google_auth(page, auth_config, op_client=op_client, log=log)
            if ok:
                continue
        await handle_interstitials(page, log=log)
        if idx > 5:
            await page.goto("https://admin.google.com/ac/home")
            await page.wait_for_timeout(3000)

    raise RuntimeError("Could not log into admin console")


def _directory_api_auth_error(error_text: str) -> bool:
    return "not authorized to access this resource/api" in str(error_text or "").lower()


async def _check_domain_verified_via_api(domain: str, panel_client: NonprofitGooglePanelClient) -> Dict[str, Any]:
    test_email = f"_verifycheck_{int(asyncio.get_running_loop().time() * 1000)}@{domain}"
    try:
        result = await asyncio.to_thread(
            panel_client.batch_create_users,
            [
                {
                    "firstName": "Verify",
                    "lastName": "Check",
                    "email": test_email,
                    "password": "TempCheck123!",
                    "orgUnitPath": "/",
                }
            ],
            True,
        )
        if list(result.get("usersCreated") or []):
            try:
                await asyncio.to_thread(panel_client.delete_user, test_email, True)
            except Exception:
                pass
            return {
                "available": True,
                "verified": True,
                "method": "temp_user_create",
                "testEmail": test_email,
            }
        error_text = str((((result.get("errors") or [{}])[0]) or {}).get("error") or "")
        if "Domain not found" in error_text:
            return {
                "available": True,
                "verified": False,
                "method": "temp_user_create",
                "error": error_text,
                "classification": "domain_not_found",
            }
        if "already exists" in error_text:
            return {
                "available": True,
                "verified": True,
                "method": "temp_user_create",
                "error": error_text,
                "classification": "test_user_already_exists",
            }
        if _directory_api_auth_error(error_text):
            return {
                "available": False,
                "verified": False,
                "method": "temp_user_create",
                "error": error_text,
                "classification": "directory_api_unauthorized",
            }
        return {
            "available": True,
            "verified": False,
            "method": "temp_user_create",
            "error": error_text or json.dumps(result)[:500],
            "classification": "inconclusive",
        }
    except Exception as exc:
        return {
            "available": False,
            "verified": False,
            "method": "temp_user_create",
            "error": str(exc),
            "classification": "api_exception",
        }


async def _scroll_admin_domain_list(page: Page) -> Dict[str, Any]:
    return await page.evaluate(
        """() => {
            const scrollables = Array.from(document.querySelectorAll('*'))
              .filter((el) => {
                const style = window.getComputedStyle(el);
                return el.scrollHeight > el.clientHeight + 20
                  && ['auto', 'scroll'].includes(style.overflowY);
              })
              .sort((a, b) => (b.clientHeight * b.clientWidth) - (a.clientHeight * a.clientWidth));
            const target = scrollables[0] || document.scrollingElement || document.documentElement;
            const before = target.scrollTop || 0;
            const maxTop = Math.max(0, target.scrollHeight - target.clientHeight);
            const delta = Math.max(350, Math.floor((target.clientHeight || 600) * 0.75));
            target.scrollTop = Math.min(maxTop, before + delta);
            target.dispatchEvent(new Event('scroll', { bubbles: true }));
            return { before, after: target.scrollTop || 0, maxTop, scrolled: (target.scrollTop || 0) > before };
        }"""
    )


async def _find_domain_row_state(page: Page, domain: str, *, click_verify: bool = False) -> Dict[str, Any]:
    clean_domain = str(domain or "").strip().lower()
    return await page.evaluate(
        """({ domain, clickVerify }) => {
            const normalize = (value) => String(value || '').replace(/\\s+/g, ' ').trim();
            const lower = (value) => normalize(value).toLowerCase();
            const inspectRow = (row, matchType) => {
              if (!row) return null;
              const text = normalize(row.textContent);
              const verified = (
                /\\bverified\\b/i.test(text)
                || /gmail\\s+activated/i.test(text)
                || /domain\\s+is\\s+ready/i.test(text)
              ) && !/not\\s+verified|unverified|verify\\s+domain/i.test(text);

              const controls = Array.from(row.querySelectorAll('button, a, [role="button"]'));
              const verifyControl = controls.find((control) => {
                const controlText = lower(control.textContent || control.getAttribute('aria-label') || '');
                return /verify|start verification|confirm|set up gmail/.test(controlText);
              });
              const domainControl = controls.find((control) => lower(control.textContent).includes(domain))
                || row.querySelector(`[data-domain-name="${domain}"]`)
                || row.querySelector('a, button, [role="button"]');

              if (clickVerify && verifyControl) {
                verifyControl.scrollIntoView({ block: 'center', inline: 'nearest' });
                verifyControl.click();
                return { found: true, clickedVerify: true, verified, matchType, text: text.slice(0, 500) };
              }
              if (clickVerify && !verified && domainControl) {
                domainControl.scrollIntoView({ block: 'center', inline: 'nearest' });
                domainControl.click();
                return { found: true, clickedRow: true, verified, matchType, text: text.slice(0, 500) };
              }
              return { found: true, verified, hasVerifyControl: Boolean(verifyControl), matchType, text: text.slice(0, 500) };
            };

            const cells = Array.from(document.querySelectorAll('td, [role="gridcell"]'));
            for (const cell of cells) {
              if (lower(cell.textContent) !== domain) continue;
              const state = inspectRow(cell.closest('tr, [role="row"], li, [role="listitem"]'), 'exact_cell');
              if (state) return state;
            }

            const rowSelectors = [
              'tr',
              '[role="row"]',
              '[data-domain-name]',
              '[data-testid*="domain" i]',
              'li',
              '[role="listitem"]'
            ];
            const rows = Array.from(document.querySelectorAll(rowSelectors.join(',')));
            for (const row of rows) {
              const text = normalize(row.textContent);
              const rowLower = text.toLowerCase();
              const attrDomain = lower(row.getAttribute('data-domain-name') || row.getAttribute('data-domain') || '');
              if (!rowLower.includes(domain) && attrDomain !== domain) continue;
              const state = inspectRow(row, attrDomain === domain ? 'domain_attribute' : 'row_text');
              if (state) return state;
            }
            return { found: false };
        }""",
        {"domain": clean_domain, "clickVerify": bool(click_verify)},
    )


async def _find_domain_in_admin_list(
    page: Page,
    domain: str,
    *,
    click_verify: bool = False,
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    clean_domain = str(domain or "").strip().lower()
    for idx in range(90):
        state = await _find_domain_row_state(page, clean_domain, click_verify=click_verify)
        if state.get("found"):
            state["scrollAttempts"] = idx
            return state
        scroll_state = await _scroll_admin_domain_list(page)
        if idx > 0 and idx % 15 == 0:
            _log(log, f"  [domains] still searching for {clean_domain}; scroll={scroll_state}")
        await page.wait_for_timeout(350)
        if isinstance(scroll_state, dict) and not scroll_state.get("scrolled"):
            if float(scroll_state.get("after") or 0) >= float(scroll_state.get("maxTop") or 0):
                break
    return {"found": False, "scrollAttempts": 90}


async def verify_domain_in_admin(
    page: Page,
    domain: str,
    panel_credentials: Dict[str, Any],
    op_client: Optional[OnePasswordCliClient],
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    clean_domain = str(domain or "").strip().lower()
    panel_client = _panel_client_from_credentials(panel_credentials)

    _log(log, f"[verify_domain] {clean_domain}")
    api_precheck = await _check_domain_verified_via_api(clean_domain, panel_client)
    if api_precheck.get("verified"):
        _log(log, "  domain already accepts users via Apps Script API; ensuring Admin console agrees")
    elif not api_precheck.get("available", True):
        _log(log, f"  API verification unavailable; continuing with Admin UI proof: {api_precheck.get('classification')}")

    await _ensure_admin_session(page, panel_credentials, op_client=op_client, log=log)
    await page.goto("https://admin.google.com/ac/domains/manage")
    await page.wait_for_timeout(5000)

    domain_state: Dict[str, Any] = {}
    search_bar = page.get_by_role("textbox", name=re.compile(r"Search", re.I)).first
    if await _visible(search_bar, 5000):
        await search_bar.click()
        await search_bar.fill(clean_domain)
        await page.wait_for_timeout(2000)
        await page.keyboard.press("Enter")
        await page.wait_for_timeout(5000)
        domain_state = await _find_domain_in_admin_list(page, clean_domain, click_verify=True, log=log)

    clicked = False
    added_to_workspace = False
    if domain_state.get("verified"):
        return {
            "verified": True,
            "method": "admin_row_verified",
            "apiPrecheck": api_precheck,
            "addedToWorkspace": False,
            "domainRow": domain_state,
        }
    if domain_state.get("clickedVerify") or domain_state.get("clickedRow"):
        clicked = True
    if not domain_state.get("found"):
        await page.goto("https://admin.google.com/ac/domains/manage")
        await page.wait_for_timeout(5000)

        domain_state = await _find_domain_in_admin_list(page, clean_domain, click_verify=True, log=log)
        if domain_state.get("verified"):
            return {
                "verified": True,
                "method": "admin_row_verified_after_scroll",
                "apiPrecheck": api_precheck,
                "addedToWorkspace": False,
                "domainRow": domain_state,
            }
        if domain_state.get("clickedVerify") or domain_state.get("clickedRow"):
            clicked = True

        if not domain_state.get("found") and not clicked:
            add_btn = page.get_by_role("button", name="Add a domain").first
            if await _visible(add_btn, 3000):
                await add_btn.click()
                await page.wait_for_timeout(3000)
                domain_input = page.get_by_role("textbox", name="Enter domain name")
                if await _visible(domain_input.first, 5000):
                    await domain_input.first.fill(clean_domain)
                    await page.wait_for_timeout(1000)
                    for selector in [
                        page.get_by_role("radio", name=re.compile(r"Secondary domain", re.I)).first,
                        page.get_by_text(re.compile(r"Secondary domain", re.I)).first,
                        page.get_by_text(re.compile(r"create user accounts", re.I)).first,
                    ]:
                        try:
                            if await _visible(selector, 1000):
                                await selector.click()
                                await page.wait_for_timeout(500)
                                break
                        except Exception:
                            pass
                    add_verify_btn = page.get_by_role(
                        "button",
                        name=re.compile(r"Add domain( & start| and start|$)|Start verification", re.I),
                    )
                    for _ in range(10):
                        try:
                            if await add_verify_btn.first.is_enabled(timeout=2000):
                                break
                        except Exception:
                            pass
                        await page.wait_for_timeout(1000)
                    await add_verify_btn.first.click()
                    await page.wait_for_timeout(5000)
                    clicked = True
                    added_to_workspace = True

        if not clicked:
            debug = await _capture_debug_artifacts(page, f"verify_not_found_{clean_domain}", log=log)
            return {
                "verified": False,
                "error": "Domain not found in admin list after Playwright search and scroll",
                "domainRow": domain_state,
                "apiPrecheck": api_precheck,
                "debug": debug,
            }

    await page.wait_for_timeout(5000)
    await _click_button(page, ["Get started"], timeout_ms=5000)
    diff_host = page.get_by_role("checkbox", name=re.compile(r"My domain uses a different", re.I))
    if await _visible(diff_host.first, 5000):
        await diff_host.first.click()
        await page.wait_for_timeout(1000)
        await _click_button(page, ["Continue"], timeout_ms=3000)
        await page.wait_for_timeout(3000)

    code_complete = page.get_by_role("checkbox", name="Code entry complete")
    if await _visible(code_complete.first, 5000):
        await code_complete.first.click()
        await page.wait_for_timeout(1000)
        confirm = page.get_by_role("button", name="Confirm").first
        for _ in range(10):
            try:
                if await confirm.is_enabled(timeout=2000):
                    break
            except Exception:
                pass
            await page.wait_for_timeout(1000)
        await confirm.click()
        await page.wait_for_timeout(5000)

    for idx in range(60):
        if idx > 0 and idx % 4 == 0:
            api_poll = await _check_domain_verified_via_api(clean_domain, panel_client)
            if api_poll.get("verified"):
                for name in ["Continue", "Done", "Next", "Finish", "Go to setup"]:
                    await _click_button(page, [name], timeout_ms=800)
                return {
                    "verified": True,
                    "method": "api_poll_temp_user",
                    "apiPrecheck": api_precheck,
                    "apiPoll": api_poll,
                    "addedToWorkspace": added_to_workspace,
                }
            if not api_poll.get("available", True):
                _log(log, f"  [verify_domain] API poll unavailable: {api_poll.get('classification')}")
            for name in ["Continue", "Done", "Next", "Finish", "Go to setup"]:
                await _click_button(page, [name], timeout_ms=800)
            await page.goto("https://admin.google.com/ac/domains/manage")
            await page.wait_for_timeout(4000)
            poll_state = await _find_domain_in_admin_list(page, clean_domain, click_verify=False, log=log)
            if poll_state.get("verified"):
                return {
                    "verified": True,
                    "method": "admin_row_poll",
                    "apiPrecheck": api_precheck,
                    "apiPoll": api_poll,
                    "addedToWorkspace": added_to_workspace,
                    "domainRow": poll_state,
                }

        text = await page.text_content("body") or ""
        if any(token in text for token in ["Domain is ready", "successfully", "Congratulations"]):
            for name in ["Continue", "Done", "Next", "Finish"]:
                await _click_button(page, [name], timeout_ms=800)
            await page.wait_for_timeout(5000)
            api_success_check = await _check_domain_verified_via_api(clean_domain, panel_client)
            if api_success_check.get("verified"):
                return {
                    "verified": True,
                    "method": "ui_success_api_confirmed",
                    "apiPrecheck": api_precheck,
                    "apiPoll": api_success_check,
                    "addedToWorkspace": added_to_workspace,
                }
            if api_success_check.get("available", True):
                _log(log, "  [verify_domain] UI reports success but API does not confirm yet; waiting")
                await page.wait_for_timeout(15_000)
                continue
            await page.goto("https://admin.google.com/ac/domains/manage")
            await page.wait_for_timeout(4000)
            success_state = await _find_domain_in_admin_list(page, clean_domain, click_verify=False, log=log)
            return {
                "verified": True,
                "method": "ui_success_api_unavailable",
                "apiPrecheck": api_precheck,
                "apiPoll": api_success_check,
                "addedToWorkspace": added_to_workspace,
                "domainRow": success_state,
            }

        if "couldn't verify" in text or "verification failed" in text:
            debug = await _capture_debug_artifacts(page, f"verify_failed_{clean_domain}", log=log)
            return {
                "verified": False,
                "error": "Google verification failed",
                "apiPrecheck": api_precheck,
                "debug": debug,
            }

        await page.wait_for_timeout(15_000)

    debug = await _capture_debug_artifacts(page, f"verify_timeout_{clean_domain}", log=log)
    return {
        "verified": False,
        "timedOut": True,
        "apiPrecheck": api_precheck,
        "debug": debug,
    }


async def enable_dkim_for_domain(
    page: Page,
    domain: str,
    panel_credentials: Dict[str, Any],
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    clean_domain = str(domain or "").strip().lower()
    await _ensure_admin_session(page, panel_credentials, op_client=None, log=log)
    await page.goto("https://admin.google.com/ac/apps/gmail/authenticateemail?hl=en")
    await page.wait_for_timeout(5000)

    listbox = page.locator("//div[@role='listbox'][contains(@aria-label,'domain')]").first
    if await _visible(listbox, 5000):
        await listbox.click()
        await page.wait_for_timeout(1500)
        selected = False
        for _ in range(40):
            option = page.get_by_role("option", name=re.compile(re.escape(clean_domain), re.I)).first
            if await _visible(option, 800):
                await option.click()
                selected = True
                break
            await page.evaluate(
                """() => {
                    const popup = document.querySelector('[role="listbox"]') || document.querySelector('[role="presentation"] [role="list"]');
                    if (popup) popup.scrollTop += 300;
                }"""
            )
            await page.wait_for_timeout(250)
        if not selected:
            matching = page.locator(f"//div[@role='option' and contains(normalize-space(), '{clean_domain}')]").first
            if await _visible(matching, 1000):
                await matching.click()
                selected = True
        if not selected:
            raise RuntimeError(f"Could not select DKIM domain {clean_domain}")

    stop_auth = page.get_by_role("button", name="Stop authentication").first
    if await _visible(stop_auth, 3000):
        return {"enabled": True, "alreadyEnabled": True}

    await _click_button(page, ["Start authentication"], timeout_ms=4000)
    await page.wait_for_timeout(2500)
    await page.reload(wait_until="domcontentloaded")
    await page.wait_for_timeout(4000)
    if await _visible(listbox, 3000):
        await listbox.click()
        option = page.get_by_role("option", name=re.compile(re.escape(clean_domain), re.I)).first
        if await _visible(option, 1000):
            await option.click()
            await page.wait_for_timeout(1000)

    status_text = (await page.text_content("body") or "").lower()
    enabled = "authenticating email with dkim" in status_text or await _visible(stop_auth, 2000)
    return {"enabled": enabled}


async def setup_user_2fa(
    page: Page,
    user_email: str,
    user_password: str,
    panel_credentials: Dict[str, Any],
    user_op_client: OnePasswordCliClient,
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    clean_email = str(user_email or "").strip().lower()
    clean_password = str(user_password or "").strip()
    if not clean_email or not clean_password:
        raise RuntimeError("user_email and user_password are required")

    existing = await asyncio.to_thread(user_op_client.find_google_login_item, clean_email)
    if existing and str(existing.get("id") or "").strip():
        try:
            await asyncio.to_thread(user_op_client.get_totp, str(existing.get("id") or "").strip())
            return {
                "success": True,
                "alreadyEnabled": True,
                "item_id": str(existing.get("id") or "").strip(),
            }
        except Exception:
            pass

    auth_config = {
        "email": clean_email,
        "password": clean_password,
        "op_item_id": str(existing.get("id") or "").strip() if existing else "",
    }

    await page.goto("https://accounts.google.com/ServiceLogin")
    await page.wait_for_timeout(3000)
    await handle_google_auth(page, auth_config, op_client=user_op_client, log=log)
    await page.wait_for_timeout(3000)

    for _ in range(5):
        body = await page.text_content("body") or ""
        if "2-Step Verification" in body or "Authenticator" in body:
            break
        for name in ["I understand", "Continue", "Accept", "Got it", "Next", "Done"]:
            if await _click_button(page, [name], timeout_ms=1200):
                await page.wait_for_timeout(3000)
                break
        submit_input = page.locator('input[type="submit"]').first
        if await _visible(submit_input, 1000):
            await submit_input.click()
            await page.wait_for_timeout(3000)

    if "twosvrequired" in page.url:
        enroll = page.get_by_role("link", name="Enroll").first
        if await _visible(enroll, 3000):
            await enroll.click()
            await page.wait_for_timeout(3000)

    if "twosv" not in page.url and "authenticator" not in page.url:
        await page.goto("https://myaccount.google.com/signinoptions/twosv")
        await page.wait_for_timeout(3000)

    body = await page.text_content("body") or ""
    turn_off = page.get_by_role("button", name="Turn off 2-Step Verification").first
    if await _visible(turn_off, 3000) or "Your account is protected" in body:
        return {"success": True, "alreadyEnabled": True}
    if "isn't allowed for this account" in body:
        return {"success": False, "notAllowed": True}

    auth_link = page.locator("a", has_text="Authenticator").first
    if await _visible(auth_link, 5000):
        await auth_link.click()
        await page.wait_for_timeout(3000)

    setup_btn = page.get_by_role("button", name="Set up authenticator").first
    if await _visible(setup_btn, 5000):
        await setup_btn.click()
        await page.wait_for_timeout(3000)

    for name in ["Can't scan it?", "Can’t scan it?"]:
        button = page.get_by_role("button", name=name).first
        if await _visible(button, 2000):
            await button.click()
            break
    fallback = page.locator('button:has-text("scan it")').first
    if await _visible(fallback, 2000):
        await fallback.click()
    await page.wait_for_timeout(2000)

    secret_text = await page.evaluate(
        """() => {
            const strongs = document.querySelectorAll("strong");
            for (const s of strongs) {
              const text = (s.textContent || "").trim();
              if (text.length > 20 && /^[a-z0-9 ]+$/i.test(text)) return text.replace(/\\s/g, "");
            }
            return null;
        }"""
    )
    if not secret_text:
        raise RuntimeError("Could not extract TOTP secret")

    item = await asyncio.to_thread(
        user_op_client.create_or_update_google_login,
        email=clean_email,
        password=clean_password,
        otp_secret=str(secret_text),
        username=clean_email,
    )
    item_id = str(item.get("id") or "").strip()

    await _click_button(page, ["Next"], timeout_ms=4000)
    await page.wait_for_timeout(2000)

    totp = await asyncio.to_thread(user_op_client.get_totp, item_id)
    code_field = page.get_by_role("textbox", name=re.compile(r"Enter code", re.I)).first
    if not await _visible(code_field, 5000):
        code_field = page.locator("input[name='totpPin']").first
    await code_field.fill(totp)
    await _click_button(page, ["Verify"], timeout_ms=4000)
    await page.wait_for_timeout(3000)

    await page.goto("https://myaccount.google.com/signinoptions/twosv")
    await page.wait_for_timeout(3000)
    await handle_google_auth(
        page,
        {
            "email": clean_email,
            "password": clean_password,
            "op_item_id": item_id,
        },
        op_client=user_op_client,
        log=log,
    )
    await page.wait_for_timeout(3000)

    turn_on = page.get_by_role("button", name="Turn on 2-Step Verification").first
    if await _visible(turn_on, 5000):
        await turn_on.click()
        await page.wait_for_timeout(3000)
        cancel = page.get_by_role("button", name="Cancel button").first
        if await _visible(cancel, 3000):
            await cancel.click()
            await page.reload(wait_until="networkidle")
            await page.wait_for_timeout(3000)
            retry = page.get_by_role("button", name="Turn on 2-Step Verification").first
            if await _visible(retry, 5000):
                await retry.click()
                await page.wait_for_timeout(3000)

    final_body = await page.text_content("body") or ""
    is_on = "Your account is protected" in final_body or "Turn off" in final_body
    return {
        "success": is_on,
        "totp_secret": str(secret_text),
        "item_id": item_id,
    }
