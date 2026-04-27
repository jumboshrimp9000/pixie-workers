import logging
import os
import re
import tempfile
import time
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, unquote

import requests
from playwright.sync_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    sync_playwright,
)

from app.workers.onepassword_client import OnePasswordCliClient


logger = logging.getLogger(__name__)


class GoogleSignInChallengeBlocked(RuntimeError):
    """Raised when Google requires a manual sign-in challenge automation cannot satisfy."""


@dataclass
class GoogleAdminUser:
    email: str
    first_name: str
    last_name: str
    password: str


@dataclass
class GoogleMfaUser:
    email: str
    password: str
    username: str = ""


class GoogleAdminPlaywrightClient:
    """
    Playwright automation for Google Admin / MyAccount operations.

    Implemented operations:
      - add_users
      - update_users
      - suspend_users
      - upload_profile_photos
      - add_trusted_apps
      - fetch_dkim_txt_record
      - start_dkim_authentication
      - enroll_users_mfa_with_1password
    """

    def __init__(
        self,
        *,
        headless: bool = True,
        timeout_seconds: int = 30,
        chromium_executable_path: Optional[str] = None,
        slow_mo_ms: int = 0,
    ):
        self.headless = headless
        self.timeout_seconds = timeout_seconds
        self.timeout_ms = max(1000, int(timeout_seconds * 1000))
        self.chromium_executable_path = (
            chromium_executable_path
            or os.getenv("GOOGLE_PLAYWRIGHT_CHROME_PATH", "").strip()
            or os.getenv("GOOGLE_SELENIUM_CHROME_BINARY", "").strip()
            or None
        )
        self.playwright_channel = os.getenv("GOOGLE_PLAYWRIGHT_CHANNEL", "").strip()
        self.slow_mo_ms = max(0, int(os.getenv("GOOGLE_PLAYWRIGHT_SLOW_MO_MS", str(slow_mo_ms or 0))))
        self.debug_enabled = str(os.getenv("GOOGLE_PLAYWRIGHT_DEBUG", "false")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.debug_dir = Path(os.getenv("GOOGLE_PLAYWRIGHT_DEBUG_DIR", "app/logs/playwright_debug"))
        self.admin_login_attempts = max(1, int(os.getenv("GOOGLE_ADMIN_LOGIN_ATTEMPTS", "4")))
        self.admin_login_retry_seconds = max(5.0, float(os.getenv("GOOGLE_ADMIN_LOGIN_RETRY_SECONDS", "30")))
        self.mfa_retry_attempts = max(1, int(os.getenv("GOOGLE_MFA_ENROLL_RETRY_ATTEMPTS", "3")))
        self.mfa_retry_delay_seconds = max(0.5, float(os.getenv("GOOGLE_MFA_ENROLL_RETRY_DELAY_SECONDS", "2")))
        self.mfa_reset_session_per_user = (
            str(os.getenv("GOOGLE_MFA_RESET_SESSION_PER_USER", "true")).strip().lower()
            in {"1", "true", "yes", "on"}
        )
        self.profile_verify_enabled = (
            str(os.getenv("GOOGLE_PROFILE_VERIFY_ENABLED", "true")).strip().lower()
            in {"1", "true", "yes", "on"}
        )
        self.profile_verify_attempts = max(1, int(os.getenv("GOOGLE_PROFILE_VERIFY_ATTEMPTS", "2")))
        self._admin_email: str = ""
        self._admin_password: str = ""
        self._admin_onepassword: Optional[OnePasswordCliClient] = None
        self._totp_item_ids_by_email: Dict[str, str] = {}
        self._totp_secrets_by_email: Dict[str, str] = {}

        self._playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

    def __enter__(self) -> "GoogleAdminPlaywrightClient":
        self._playwright = sync_playwright().start()
        launch_kwargs: Dict[str, Any] = {"headless": self.headless}
        if self.slow_mo_ms:
            launch_kwargs["slow_mo"] = self.slow_mo_ms
        if self.chromium_executable_path:
            launch_kwargs["executable_path"] = self.chromium_executable_path
        elif self.playwright_channel:
            launch_kwargs["channel"] = self.playwright_channel

        # Reduce Google bot-detection: hide Playwright automation signals
        launch_kwargs.setdefault("args", [])
        launch_kwargs["args"].extend([
            "--disable-blink-features=AutomationControlled",
        ])

        self.browser = self._playwright.chromium.launch(**launch_kwargs)
        self._create_context_and_page()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        for obj in [self.page, self.context, self.browser]:
            if obj is None:
                continue
            try:
                obj.close()
            except Exception:
                pass
        self.page = None
        self.context = None
        self.browser = None
        if self._playwright is not None:
            try:
                self._playwright.stop()
            except Exception:
                pass
            self._playwright = None

    def _create_context_and_page(self) -> None:
        browser = self._require_browser()
        self.context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        # Hide navigator.webdriver to reduce Google bot-detection
        self.context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        self.page = self.context.new_page()
        self.page.set_default_timeout(self.timeout_ms)
        self.page.set_default_navigation_timeout(max(self.timeout_ms, 60_000))

    def _reset_session(self) -> None:
        if self.context is not None:
            try:
                self.context.close()
            except Exception:
                pass
        self.context = None
        self.page = None
        self._create_context_and_page()

    # -----------------------------------------------------------------
    # Session bootstrap
    # -----------------------------------------------------------------

    def login(
        self,
        admin_email: str,
        admin_password: str,
        *,
        onepassword: Optional[OnePasswordCliClient] = None,
        totp_secret: str = "",
    ) -> None:
        self._admin_email = str(admin_email or "").strip().lower()
        self._admin_password = str(admin_password or "").strip()
        self._admin_onepassword = onepassword
        if self._admin_email and str(totp_secret or "").strip():
            self._totp_secrets_by_email[self._admin_email] = str(totp_secret or "").strip()
        last_error: Optional[Exception] = None
        for attempt in range(1, self.admin_login_attempts + 1):
            try:
                if attempt > 1:
                    logger.warning(
                        "Retrying Google Admin login for %s (attempt %d/%d)",
                        admin_email,
                        attempt,
                        self.admin_login_attempts,
                    )
                    self._reset_session()
                    time.sleep(min(120.0, self.admin_login_retry_seconds * (attempt - 1)))

                self._login_google_account(admin_email, admin_password, onepassword=onepassword)
                self._open_admin_home_page()
                return
            except GoogleSignInChallengeBlocked as exc:
                message = str(exc)
                debug_note = self._capture_debug_state(
                    f"google_admin_login_blocked_{attempt}_{self._slug(admin_email)}"
                )
                if debug_note:
                    message = f"{message} | {debug_note}"
                raise GoogleSignInChallengeBlocked(message) from exc
            except Exception as exc:
                message = str(exc)
                debug_note = self._capture_debug_state(
                    f"google_admin_login_attempt_{attempt}_{self._slug(admin_email)}"
                )
                if debug_note:
                    message = f"{message} | {debug_note}"
                last_error = RuntimeError(message)
                if attempt >= self.admin_login_attempts:
                    raise last_error
                logger.warning(
                    "Google Admin login attempt %d/%d failed for %s: %s",
                    attempt,
                    self.admin_login_attempts,
                    admin_email,
                    message,
                )

        if last_error is not None:
            raise last_error

    # -----------------------------------------------------------------
    # User MFA onboarding + 1Password enrollment
    # -----------------------------------------------------------------

    def enroll_users_mfa_with_1password(
        self,
        users: List[GoogleMfaUser],
        onepassword: OnePasswordCliClient,
        *,
        progress_hook: Optional[Callable[[str, str, Dict[str, Any]], None]] = None,
        max_attempts: Optional[int] = None,
    ) -> Dict[str, Any]:
        completed = 0
        failed = 0
        results: Dict[str, Dict[str, Any]] = {}
        attempts_limit = max(1, int(max_attempts or self.mfa_retry_attempts))

        for index, user in enumerate(users):
            email = str(user.email or "").strip().lower()
            if not email:
                failed += 1
                continue
            try:
                if self.mfa_reset_session_per_user:
                    logger.info("Resetting Playwright session before MFA enrollment for %s", email)
                    self._reset_session()

                details = self._enroll_single_user_mfa_with_retries(
                    user,
                    onepassword,
                    max_attempts=attempts_limit,
                )
                results[email] = details
                completed += 1
                if progress_hook:
                    progress_hook("completed", email, details)
            except GoogleSignInChallengeBlocked:
                raise
            except Exception as exc:
                failed += 1
                message = str(exc)
                logger.warning("MFA + 1Password enrollment failed for %s: %s", email, message)
                results[email] = {"status": "failed", "error": message}
                if progress_hook:
                    progress_hook("failed", email, {"error": message})
        return {"completed": completed, "failed": failed, "results": results}

    def _enroll_single_user_mfa_with_retries(
        self,
        user: GoogleMfaUser,
        onepassword: OnePasswordCliClient,
        *,
        max_attempts: int,
    ) -> Dict[str, Any]:
        email = str(user.email or "").strip().lower()
        last_error = "Unknown MFA enrollment error"

        for attempt in range(1, max_attempts + 1):
            logger.info("MFA enrollment attempt %d/%d for %s", attempt, max_attempts, email)
            if attempt > 1:
                self._prepare_mfa_retry_state(email, attempt)

            try:
                details = self._enroll_single_user_mfa(user, onepassword)
                details["attempt"] = attempt
                details["max_attempts"] = max_attempts
                return details
            except GoogleSignInChallengeBlocked:
                raise
            except Exception as exc:
                debug_note = self._capture_debug_state(f"mfa_attempt_{attempt}_{self._slug(email)}")
                last_error = str(exc)
                if debug_note:
                    last_error = f"{last_error} | debug={debug_note}"
                logger.warning(
                    "MFA enrollment attempt %d/%d failed for %s: %s",
                    attempt,
                    max_attempts,
                    email,
                    last_error,
                )
                if attempt < max_attempts:
                    sleep_seconds = min(10.0, self.mfa_retry_delay_seconds * attempt)
                    logger.info(
                        "Retrying MFA enrollment for %s in %.1f seconds (browser remains open)",
                        email,
                        sleep_seconds,
                    )
                    time.sleep(sleep_seconds)

        raise RuntimeError(
            f"MFA enrollment failed after {max_attempts} attempts for {email}. Last error: {last_error}"
        )

    def _prepare_mfa_retry_state(self, email: str, attempt: int) -> None:
        logger.info(
            "Preparing in-session MFA retry for %s (attempt %d): dismissing prompts and resetting page state",
            email,
            attempt,
        )
        self._dismiss_sign_in_to_chrome_prompt_if_present()
        self._dismiss_phone_prompt_if_present()
        try:
            self._goto("about:blank")
        except Exception:
            pass

    def _enroll_single_user_mfa(
        self,
        user: GoogleMfaUser,
        onepassword: OnePasswordCliClient,
    ) -> Dict[str, Any]:
        email = str(user.email or "").strip().lower()
        password = str(user.password or "").strip()
        username = str(user.username or "").strip() or email.split("@")[0]
        if not email or not password:
            raise RuntimeError("email/password required for MFA enrollment")

        self._login_google_account(email, password, onepassword=onepassword)
        self._open_2sv_page(email, password, onepassword=onepassword)

        if self._is_2sv_already_enabled():
            return {"status": "already_enabled", "email": email}

        enroll_mode = self._start_authenticator_enrollment()

        secret = ""
        item: Dict[str, Any] = {}
        item_id = ""
        if enroll_mode == "existing_authenticator":
            existing = onepassword.find_google_login_item(email)
            if not existing:
                raise RuntimeError(
                    f"Authenticator already added for {email}, but no 1Password item exists to continue."
                )
            item_id = str(existing.get("id") or "").strip()
            if not item_id:
                raise RuntimeError(f"1Password item id missing for {email}")
            item = {"id": item_id, "created": False}
            self._finalize_2sv_enablement(email=email, password=password)
        else:
            secret = self._extract_authenticator_secret()
            if not secret:
                raise RuntimeError("Failed to read authenticator secret from Google UI")

            item = onepassword.create_or_update_google_login(
                email=email,
                password=password,
                otp_secret=secret,
                username=username,
            )
            item_id = str(item.get("id") or "").strip()
            if not item_id:
                raise RuntimeError(f"1Password did not return item_id for {email}")

            self._click_any(
                [
                    "#totpNext",
                    "//div[@role='dialog']//button[@data-id='OCpkoe']",
                    "//div[@role='dialog']//button[contains(@aria-label,'Next')]",
                    "//div[@role='dialog']//span[text()='Next']",
                    "//span[text()='Next']",
                    "//button//span[text()='Next']",
                ],
                timeout_ms=15_000,
            )

            code = onepassword.get_totp(item_id)
            self._submit_totp_code(code)
            self._finalize_2sv_enablement(email=email, password=password)

        return {
            "status": "enabled",
            "email": email,
            "item_id": item_id,
            "item_created": bool(item.get("created")),
            "otp_secret": secret,
            "enroll_mode": enroll_mode,
        }

    def disable_login_challenges_for_users(
        self,
        admin_email: str,
        admin_password: str,
        target_emails: List[str],
        *,
        onepassword: Optional[OnePasswordCliClient] = None,
        totp_item_ids_by_email: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        targets = [str(email or "").strip().lower() for email in target_emails if str(email or "").strip()]
        results: Dict[str, Dict[str, Any]] = {}
        disabled = 0
        already_off = 0
        failed = 0
        if not targets:
            return {"disabled": 0, "already_off": 0, "failed": 0, "results": results}

        previous_totp_items = dict(self._totp_item_ids_by_email)
        try:
            mapped_items = {
                str(email or "").strip().lower(): str(item_id or "").strip()
                for email, item_id in (totp_item_ids_by_email or {}).items()
                if str(email or "").strip() and str(item_id or "").strip()
            }
            self._totp_item_ids_by_email.update(mapped_items)

            self._login_google_account(admin_email, admin_password, onepassword=onepassword)
            self._open_admin_home_page()
            for email in targets:
                try:
                    details = self._disable_login_challenge_for_user(email)
                    status = str(details.get("status") or "").lower()
                    if status in {"disabled", "disabled_unverified"}:
                        disabled += 1
                    elif status == "already_off":
                        already_off += 1
                    results[email] = details
                except GoogleSignInChallengeBlocked:
                    raise
                except Exception as exc:
                    failed += 1
                    debug_note = self._capture_debug_state(f"disable_login_challenge_failed_{self._slug(email)}")
                    message = str(exc)
                    if debug_note:
                        message = f"{message} | debug={debug_note}"
                    results[email] = {"status": "failed", "error": message}
        finally:
            self._totp_item_ids_by_email = previous_totp_items

        return {
            "disabled": disabled,
            "already_off": already_off,
            "failed": failed,
            "results": results,
        }

    def _disable_login_challenge_for_user(self, email: str) -> Dict[str, Any]:
        clean_email = str(email or "").strip().lower()
        if not clean_email:
            raise RuntimeError("target email is required for login-challenge disable")

        self._open_user_security_settings(clean_email)
        if self._login_challenge_already_off():
            return {"status": "already_off", "email": clean_email, "url": self._safe_url()}

        self._expand_login_challenge_control()
        if self._login_challenge_already_off():
            return {"status": "already_off", "email": clean_email, "url": self._safe_url()}

        clicked = self._click_google_action(
            [
                "//*[@role='button' and .//span[contains(normalize-space(),'Turn off for 10 mins')]]",
                "//*[@role='button' and .//span[contains(normalize-space(),'Turn off')]]",
                "//button[.//span[contains(normalize-space(),'Turn off')]]",
                "//button[contains(normalize-space(),'Turn off')]",
                "//button[contains(normalize-space(),'Disable')]",
                "//span[contains(normalize-space(),'Turn off for 10 mins')]",
                "//span[contains(normalize-space(),'Turn off for 10 minutes')]",
                "//span[contains(normalize-space(),'Turn off')]",
                "//span[contains(normalize-space(),'Disable login challenge')]",
            ],
            [
                "Turn off for 10 mins",
                "Turn off for 10 minutes",
                "Turn off login challenge",
                "Disable login challenge",
                "Turn off",
                "Disable",
            ],
            timeout_ms=12_000,
            optional=True,
        )
        if not clicked:
            raise RuntimeError(
                f"Login challenge control was not clickable for {clean_email}. url={self._safe_url()} "
                f"text={self._visible_text_excerpt(limit=700)}"
            )

        time.sleep(0.8)
        self._click_google_action(
            [
                "//div[@role='dialog']//button[.//span[contains(normalize-space(),'Turn off')]]",
                "//div[@role='dialog']//button[contains(normalize-space(),'Turn off')]",
                "//div[@role='dialog']//button[contains(normalize-space(),'Disable')]",
                "//div[@role='dialog']//button[contains(normalize-space(),'Confirm')]",
                "//button[.//span[normalize-space()='OK']]",
                "//button[.//span[normalize-space()='Done']]",
            ],
            ["Turn off", "Disable", "Confirm", "OK", "Done"],
            timeout_ms=8_000,
            optional=True,
        )
        time.sleep(1.5)

        if not self._login_challenge_already_off():
            text = self._visible_text_excerpt(limit=3500).lower()
            if not (
                "turned off" in text
                or "off for 10" in text
                or "disabled for 10" in text
                or "login challenge is off" in text
            ):
                if "turn off for 10 min" in text or "turn off identity" in text:
                    return {
                        "status": "disabled_unverified",
                        "email": clean_email,
                        "url": self._safe_url(),
                        "warning": (
                            "Clicked Google Admin login-challenge disable control, but the page did not "
                            "leave persistent success text to verify."
                        ),
                    }
                raise RuntimeError(
                    f"Login challenge disable did not produce success evidence for {clean_email}. "
                    f"url={self._safe_url()} text={self._visible_text_excerpt(limit=1200)}"
                )

        return {"status": "disabled", "email": clean_email, "url": self._safe_url()}

    def _open_user_security_settings(self, email: str) -> None:
        clean_email = str(email or "").strip().lower()
        urls = [
            f"https://admin.google.com/ac/users?query={quote(clean_email)}",
            f"https://admin.google.com/ac/users?search={quote(clean_email)}",
            "https://admin.google.com/ac/users",
        ]
        opened_user = False
        for url in urls:
            self._goto(url)
            self._reauthenticate_admin_console_if_needed()
            time.sleep(1.8)
            if clean_email in self._visible_text_excerpt(limit=1800).lower() and self._page_mentions_user_security():
                opened_user = True
                break
            if self._click_admin_user_result(clean_email):
                time.sleep(2.0)
                opened_user = True
                break

        if not opened_user:
            self._search_admin_console_for_user(clean_email)
            if self._click_admin_user_result(clean_email):
                time.sleep(2.0)
                opened_user = True

        if not opened_user:
            raise RuntimeError(f"Could not open Google Admin user page for {clean_email}")

        if self._page_mentions_user_security():
            return

        self._click_google_action(
            [
                "//a[.//*[normalize-space()='Security']]",
                "//a[contains(normalize-space(),'Security')]",
                "//button[.//*[normalize-space()='Security']]",
                "//button[contains(normalize-space(),'Security')]",
                "//*[normalize-space()='Security']",
            ],
            ["Security"],
            timeout_ms=10_000,
            optional=True,
        )
        time.sleep(1.5)
        if not self._page_mentions_user_security():
            self._click_semantic_text(["Security"], timeout_ms=6_000, optional=True)
            time.sleep(1.0)

    def _search_admin_console_for_user(self, email: str) -> None:
        page = self._require_page()
        self._goto("https://admin.google.com/ac/users")
        self._reauthenticate_admin_console_if_needed()
        time.sleep(1.0)
        filled = self._fill_any(
            [
                "input[aria-label*='Search']",
                "input[placeholder*='Search']",
                "input[type='search']",
                "input[type='text']",
            ],
            email,
            timeout_ms=8_000,
            optional=True,
        )
        if filled:
            try:
                page.keyboard.press("Enter")
            except Exception:
                pass
            time.sleep(2.0)

    def _click_admin_user_result(self, email: str) -> bool:
        clean_email = str(email or "").strip().lower()
        if not clean_email:
            return False
        try:
            return bool(
                self._require_page().evaluate(
                    """
                    (email) => {
                      const target = String(email || '').trim().toLowerCase();
                      const clickableSelector = 'a,button,[role="button"],tr,[role="row"],li';
                      const isVisible = (el) => {
                        if (!el) return false;
                        const style = window.getComputedStyle(el);
                        const rect = el.getBoundingClientRect();
                        return style.visibility !== 'hidden'
                          && style.display !== 'none'
                          && rect.width > 0
                          && rect.height > 0;
                      };

                      const textFor = (el) => [
                        el.getAttribute('aria-label'),
                        el.getAttribute('title'),
                        el.innerText,
                        el.textContent
                      ].filter(Boolean).join(' ').replace(/\\s+/g, ' ').trim();
                      const emailNodes = Array.from(document.querySelectorAll('[title],td,div,span'))
                        .filter((el) => {
                          if (!isVisible(el)) return false;
                          const title = String(el.getAttribute('title') || '').trim().toLowerCase();
                          const label = textFor(el).toLowerCase();
                          return title === target || label === target;
                        });
                      for (const node of emailNodes) {
                        const row = node.closest('tr,[role="row"]') || node;
                        const link = Array.from(row.querySelectorAll('a[href*="/ac/users/"],a[href*="./ac/users/"]'))
                          .find(isVisible);
                        if (link) {
                          link.scrollIntoView({block: 'center', inline: 'center'});
                          link.click();
                          return true;
                        }
                      }

                      const candidates = Array.from(document.querySelectorAll(clickableSelector + ',div,span'));
                      for (const el of candidates) {
                        if (!isVisible(el)) continue;
                        const label = textFor(el).toLowerCase();
                        if (!label.includes(target)) continue;
                        const clickable = el.closest(clickableSelector) || el;
                        if (!isVisible(clickable)) continue;
                        clickable.scrollIntoView({block: 'center', inline: 'center'});
                        clickable.click();
                        return true;
                      }
                      return false;
                    }
                    """,
                    clean_email,
                )
            )
        except Exception:
            return False

    def _page_mentions_user_security(self) -> bool:
        text = self._visible_text_excerpt(limit=1800).lower()
        return (
            "login challenge" in text
            or "2-step verification" in text
            or "security keys" in text
            or "recovery information" in text
            or "reset sign-in cookies" in text
        )

    def _expand_login_challenge_control(self) -> None:
        self._click_google_action(
            [
                "//*[contains(normalize-space(),'Login challenge')]",
                "//*[contains(normalize-space(),\"Verify-it's-you challenge\")]",
                "//*[contains(normalize-space(),'Verify it')]",
            ],
            ["Login challenge", "Verify-it's-you challenge", "Verify it"],
            timeout_ms=8_000,
            optional=True,
        )
        time.sleep(1.0)

    def _login_challenge_already_off(self) -> bool:
        text = self._visible_text_excerpt(limit=1200).lower()
        return (
            "login challenge" in text
            and (
                "off for 10" in text
                or "turned off" in text
                or "temporarily off" in text
                or "disabled for 10" in text
            )
        )

    def validate_inbox_login_with_2fa(
        self,
        user: GoogleMfaUser,
        onepassword: OnePasswordCliClient,
    ) -> Dict[str, Any]:
        email = str(user.email or "").strip().lower()
        password = str(user.password or "").strip()
        if not email or not password:
            raise RuntimeError("email/password required for validation")

        try:
            self._goto("about:blank")
        except Exception:
            pass
        self._goto("https://accounts.google.com/signin/v2/identifier?service=mail")

        self._complete_google_signin_flow(
            email=email,
            password=password,
            onepassword=onepassword,
            context="validate_inbox",
        )
        self._ensure_expected_google_profile(
            email,
            password,
            onepassword=onepassword,
            context="validate_inbox",
        )

        item = onepassword.find_google_login_item(email)
        item_id = str((item or {}).get("id") or "").strip()
        if not item_id:
            raise RuntimeError(f"1Password item id missing for {email}")

        self._goto("https://mail.google.com/mail/u/0/#inbox")
        self._maybe_complete_reauth(email, password, onepassword=onepassword)
        if not self._exists(["text=Inbox", "//a[contains(@href,'#inbox')]"], timeout_ms=25_000):
            debug = self._capture_debug_state(f"validate_inbox_failed_{self._slug(email)}")
            raise RuntimeError(f"Inbox did not load for {email}. {debug}")

        return {"email": email, "item_id": item_id, "validated": True}

    # -----------------------------------------------------------------
    # Add users
    # -----------------------------------------------------------------

    def add_users(self, users: List[GoogleAdminUser], domain: str) -> Dict[str, int]:
        added = 0
        failed = 0
        already_existing = 0
        domain_name = str(domain or "").strip().lower()
        if not domain_name:
            raise RuntimeError("domain is required when adding Google Admin users")
        for user in users:
            try:
                self._open_users_page()
                self._click_any(
                    [
                        "//div[@role='button' and contains(@aria-label,'Add')]",
                        "//button[contains(@aria-label,'Add user')]",
                    ]
                )
                time.sleep(1.2)

                self._click_any(
                    [
                        "//span[contains(text(),'Manage user')]",
                        "//button[.//span[contains(text(),'Manage user')]]",
                    ],
                    timeout_ms=6_000,
                    optional=True,
                )

                self._fill_any(
                    [
                        "//input[contains(@aria-label,'Primary email')]",
                        "//input[@aria-label='Primary email address']",
                    ],
                    user.email.split("@")[0],
                )
                self._fill_any(["//input[contains(@aria-label,'First name')]"], user.first_name)
                self._fill_any(["//input[contains(@aria-label,'Last name')]"], user.last_name)

                self._select_primary_email_domain(domain_name)

                self._click_any(["//label[contains(text(), 'Create password')]"], timeout_ms=4_000, optional=True)
                self._fill_any(
                    ["//input[@type='password' and contains(@aria-label,'Enter password')]"],
                    user.password,
                )
                self._click_any(
                    ["//span[text()='Ask user to change their password when they sign in']"],
                    timeout_ms=4_000,
                    optional=True,
                )

                self._click_any(
                    [
                        "//div[@role='dialog']//div[@role='button' and contains(@aria-label,'Add new user')]",
                        "//button[.//span[contains(text(),'Add new user')]]",
                        "//button[.//span[contains(text(),'Continue')]]",
                    ]
                )
                time.sleep(2.5)
                if self._add_user_dialog_has_existing_user_error():
                    if self._user_exists(user.email):
                        already_existing += 1
                    else:
                        raise RuntimeError(f"Google reported user already exists, but {user.email} was not found")
                else:
                    added += 1
            except Exception as exc:
                failed += 1
                logger.warning("Playwright add user failed for %s: %s", user.email, exc)
        return {"added": added, "failed": failed, "already_existing": already_existing}

    def _select_primary_email_domain(self, domain: str) -> None:
        domain_name = str(domain or "").strip().lower().lstrip("@")
        if not domain_name:
            raise RuntimeError("Primary email domain is required")

        page = self._require_page()
        selectors = [
            "//div[@role='combobox' and @aria-label='Primary email domain']",
            "//*[@role='combobox' and contains(@aria-label,'Primary email domain')]",
            "//button[contains(@aria-label,'Primary email domain')]",
            "//*[contains(normalize-space(),'Primary email domain')]/following::*[@role='combobox'][1]",
        ]

        def selected_text() -> str:
            return self._inner_text_any(selectors, timeout_ms=2_000, optional=True).lower()

        if domain_name in selected_text():
            return

        if not self._click_any(selectors, timeout_ms=6_000, optional=True):
            if self._exists([f"//*[contains(normalize-space(), '@{domain_name}')]"], timeout_ms=2_000):
                return
            raise RuntimeError(f"Primary email domain selector not found for @{domain_name}")

        for _ in range(80):
            for selector in [
                f"//*[@role='option' and contains(translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '@{domain_name}')]",
                f"//*[@role='option' and contains(translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{domain_name}')]",
                f"//*[(@data-value='@{domain_name}' or @data-value='{domain_name}' or @data-domain='{domain_name}') and (self::div or self::li or self::span)]",
            ]:
                option = page.locator(self._selector(selector)).first
                try:
                    option.wait_for(state="attached", timeout=400)
                    option.scroll_into_view_if_needed(timeout=1_000)
                    option.click(timeout=1_500)
                    time.sleep(0.8)
                    if domain_name in selected_text() or self._exists(
                        [
                            f"//*[contains(normalize-space(), '@{domain_name}') and @aria-selected='true']",
                            f"//*[@data-value='@{domain_name}' and @aria-selected='true']",
                            f"//*[@data-value='{domain_name}' and @aria-selected='true']",
                        ],
                        timeout_ms=1_000,
                    ):
                        return
                except Exception:
                    pass

            scroll_result = page.evaluate(
                """() => {
                    const candidates = Array.from(document.querySelectorAll(
                      '[role="listbox"], [role="menu"], [role="presentation"] [role="list"], [role="presentation"], .VfPpkd-xl07Ob-XxIAqe'
                    ));
                    const popup = candidates.find((el) => el.scrollHeight > el.clientHeight + 8)
                      || candidates.find((el) => el.querySelector && el.querySelector('[role="option"]'));
                    if (!popup) return { scrolled: false, reason: 'no-scroll-container' };
                    const before = popup.scrollTop || 0;
                    const maxTop = Math.max(0, popup.scrollHeight - popup.clientHeight);
                    const delta = Math.max(240, Math.floor((popup.clientHeight || 360) * 0.85));
                    popup.scrollTop = Math.min(maxTop, before + delta);
                    popup.dispatchEvent(new Event('scroll', { bubbles: true }));
                    return { scrolled: popup.scrollTop > before, before, after: popup.scrollTop, maxTop };
                }"""
            )
            time.sleep(0.25)
            if isinstance(scroll_result, dict) and not scroll_result.get("scrolled"):
                if float(scroll_result.get("after") or 0) >= float(scroll_result.get("maxTop") or 0):
                    break

        debug = self._capture_debug_state(f"primary_email_domain_not_selectable_{self._slug(domain_name)}")
        raise RuntimeError(f"Could not select primary email domain @{domain_name}. {debug}")

    def _add_user_dialog_has_existing_user_error(self) -> bool:
        try:
            text = self._require_page().locator("body").inner_text(timeout=2_000).lower()
        except Exception:
            return False
        return "already exists" in text or "email address is already in use" in text

    def _user_exists(self, email: str) -> bool:
        clean_email = str(email or "").strip().lower()
        if not clean_email:
            return False
        try:
            self._open_users_page()
            search = self._find_any(
                [
                    "//input[contains(@aria-label,'Search')]",
                    "//input[contains(@placeholder,'Search')]",
                    "//input[@type='search']",
                ],
                timeout_ms=8_000,
            )
            search.fill(clean_email)
            search.press("Enter")
            time.sleep(3.0)
            text = self._require_page().locator("body").inner_text(timeout=5_000).lower()
            return clean_email in text
        except Exception:
            return False

    # -----------------------------------------------------------------
    # Update users (name/username)
    # -----------------------------------------------------------------

    def update_users(self, updates: List[Dict[str, str]]) -> Dict[str, int]:
        updated = 0
        failed = 0
        username_updated = 0
        name_only_updated = 0
        for item in updates:
            old_email = str(item.get("old_email") or "").strip().lower()
            new_email = str(item.get("new_email") or old_email).strip().lower()
            first_name = str(item.get("first_name") or "").strip()
            last_name = str(item.get("last_name") or "").strip()
            if not old_email or not new_email:
                failed += 1
                continue
            try:
                username_changed = new_email != old_email
                self._open_users_page()
                time.sleep(0.8)
                try:
                    self._require_page().reload(wait_until="domcontentloaded")
                    time.sleep(0.8)
                except Exception:
                    pass

                self._open_user(old_email)
                time.sleep(0.8)
                self._click_any(
                    [
                        "//div[text()='Update user']",
                        "//span[contains(text(),'Update user')]",
                    ]
                )
                time.sleep(0.6)

                if first_name:
                    self._fill_any(["//input[contains(@aria-label,'First name')]"], first_name)
                self._fill_any(["//input[contains(@aria-label,'Last name')]"], last_name)
                if username_changed:
                    self._fill_any(
                        ["//input[contains(@aria-label,'Primary email')]"],
                        new_email.split("@")[0],
                    )

                time.sleep(0.4)
                self._click_any(
                    [
                        "//div[contains(@aria-label, 'Update user')]//span[contains(text(),'Update user')]",
                        "//button[.//span[contains(text(),'Update user')]]",
                    ]
                )
                time.sleep(1.4)
                self._click_any(["//span[text()='Done']"], timeout_ms=6_000, optional=True)
                time.sleep(0.8)

                expected_email = new_email if username_changed else old_email
                self._open_user_with_refresh(expected_email, attempts=3)
                text = self._visible_text_excerpt(limit=4000).lower()
                if expected_email not in text and expected_email.split("@")[0] not in text:
                    raise RuntimeError(f"Verification failed for {expected_email}: email not visible on user page")
                if first_name and first_name.lower() not in text:
                    raise RuntimeError(f"Verification failed for {expected_email}: first name not visible")
                if last_name and last_name.lower() not in text:
                    raise RuntimeError(f"Verification failed for {expected_email}: last name not visible")

                updated += 1
                if username_changed:
                    username_updated += 1
                else:
                    name_only_updated += 1
            except Exception as exc:
                failed += 1
                logger.warning("Playwright update user failed for %s -> %s: %s", old_email, new_email, exc)
        return {
            "updated": updated,
            "failed": failed,
            "username_updated": username_updated,
            "name_only_updated": name_only_updated,
        }

    # -----------------------------------------------------------------
    # Suspend users (safe remove path)
    # -----------------------------------------------------------------

    def suspend_users(self, emails: List[str]) -> Dict[str, int]:
        suspended = 0
        failed = 0
        for email in emails:
            target = str(email or "").strip().lower()
            if not target:
                failed += 1
                continue
            try:
                self._open_user(target)
                self._click_any(
                    [
                        "//button[contains(@aria-label,'More actions')]",
                        "//div[@role='button' and contains(@aria-label,'More')]",
                    ]
                )
                self._click_any(
                    [
                        "//span[contains(text(),'Suspend user')]",
                        "//div[contains(text(),'Suspend user')]",
                    ]
                )
                self._click_any(
                    [
                        "//button[.//span[contains(text(),'Suspend')]]",
                        "//div[@role='dialog']//span[contains(text(),'Suspend')]",
                    ],
                    timeout_ms=8_000,
                    optional=True,
                )
                suspended += 1
            except Exception as exc:
                failed += 1
                logger.warning("Playwright suspend user failed for %s: %s", target, exc)
        return {"suspended": suspended, "failed": failed}

    # -----------------------------------------------------------------
    # Profile photos
    # -----------------------------------------------------------------

    def upload_profile_photos(self, updates: List[Tuple[str, str]]) -> Dict[str, int]:
        uploaded = 0
        failed = 0
        for email, url in updates:
            local_path = None
            try:
                local_path = self._download_profile_image(url)
                if not local_path:
                    raise RuntimeError(f"Failed to download image from {url}")

                self._open_user(email)
                self._click_any(
                    ["//div[contains(@title, 'Profile photo')][@__is_owner='true']"],
                    timeout_ms=5_000,
                    optional=True,
                )

                file_input = self._find_any(
                    [
                        "//input[@type='file'][@aria-label='Change Photo']",
                        "//input[@type='file']",
                    ],
                    timeout_ms=self.timeout_ms,
                )
                file_input.set_input_files(str(local_path))
                time.sleep(4)
                uploaded += 1
            except Exception as exc:
                failed += 1
                logger.warning("Playwright profile photo upload failed for %s: %s", email, exc)
            finally:
                try:
                    if local_path and Path(local_path).exists():
                        Path(local_path).unlink()
                except Exception:
                    pass
        return {"uploaded": uploaded, "failed": failed}

    # -----------------------------------------------------------------
    # Admin app allowlisting
    # -----------------------------------------------------------------

    def add_trusted_apps(self, client_ids: List[str]) -> Dict[str, Any]:
        requested: List[str] = []
        invalid: List[str] = []
        seen: set = set()
        for raw in client_ids:
            client_id = str(raw or "").strip()
            if not client_id or client_id in seen:
                continue
            seen.add(client_id)
            if self._is_valid_google_oauth_client_id(client_id):
                requested.append(client_id)
            else:
                invalid.append(client_id)

        added: List[str] = []
        already_configured: List[str] = []
        failed: List[Dict[str, str]] = []

        for client_id in requested:
            try:
                status = self._configure_trusted_google_app(client_id)
                if status == "already_configured":
                    already_configured.append(client_id)
                else:
                    added.append(client_id)
            except Exception as exc:
                failed.append({"client_id": client_id, "error": str(exc)})
                logger.warning("Failed to allowlist Google app %s: %s", client_id, exc)

        return {
            "requested": requested,
            "added": added,
            "already_configured": already_configured,
            "invalid": invalid,
            "failed": failed,
        }

    def _open_admin_configured_apps_page(self) -> None:
        self._open_admin_console_page(
            "https://admin.google.com/ac/owl/list?tab=configuredApps",
            [
                "//span[normalize-space()='Configure new app']",
                "//button[.//span[normalize-space()='Configure new app']]",
                "//span[normalize-space()='Add app']",
                "//button[.//span[normalize-space()='Add app']]",
                "//input[@aria-label='Search for app']",
                "//*[contains(normalize-space(),'Configured apps')]",
                "//*[contains(normalize-space(),'App access control')]",
            ],
        )

    def _is_google_app_id_configured(self, client_id: str) -> bool:
        return self._exists(
            [
                f"//span[normalize-space()='{client_id}']",
                f"//*[contains(normalize-space(), '{client_id}')]",
            ],
            timeout_ms=6_000,
        )

    def _configure_trusted_google_app(self, client_id: str) -> str:
        add_entry_selectors = [
            "//span[normalize-space()='Configure new app']",
            "//button[.//span[normalize-space()='Configure new app']]",
            "//span[normalize-space()='Add app']",
            "//button[.//span[normalize-space()='Add app']]",
            "//span[normalize-space()='Add App']",
            "//button[.//span[normalize-space()='Add App']]",
            "//span[contains(normalize-space(),'Add app access')]",
            "//button[contains(@aria-label,'Add app')]",
            "//button[contains(@aria-label,'Configure')]",
            "//div[@role='button'][.//span[contains(normalize-space(),'Add app')]]",
        ]
        search_field_selectors = [
            "//section[@data-step-index='0']//input[@aria-label='Search for app']",
            "//section[@data-step-index='0']//input[contains(@aria-label,'Search for app')]",
            "//section[@data-step-index='0']//input[contains(@placeholder,'Search for app')]",
        ]
        search_button_selectors = [
            "//section[@data-step-index='0']//button[@aria-label='Search for apps']",
            "//section[@data-step-index='0']//button[.//span[normalize-space()='Search']]",
        ]
        result_selectors = [
            f"//section[@data-step-index='0']//*[not(self::input) and contains(normalize-space(), '{client_id}')]",
            f"//section[@data-step-index='0']//div[@role='option'][contains(normalize-space(), '{client_id}')]",
            f"//section[@data-step-index='0']//span[contains(normalize-space(), '{client_id}')]",
        ]
        step0_continue_selectors = [
            "//section[@data-step-index='0']//button[@aria-label='Continue']",
            "//section[@data-step-index='0']//button[.//span[normalize-space()='Continue']]",
        ]
        step1_scope_selectors = [
            "//section[@data-step-index='1']//*[@role='radio' and contains(@aria-label,'All in')]",
            "//section[@data-step-index='1']//*[contains(normalize-space(),'All in')]",
        ]
        step1_continue_selectors = [
            "//section[@data-step-index='1']//button[@aria-label='Continue']",
            "//section[@data-step-index='1']//button[.//span[normalize-space()='Continue']]",
        ]
        step2_trusted_selectors = [
            "//section[@data-step-index='2']//*[@id='trustedRadio']",
            "//section[@data-step-index='2']//input[@id='trustedRadio']",
            "//section[@data-step-index='2']//label[@for='trustedRadio']",
            "//section[@data-step-index='2']//*[@role='radio' and contains(@aria-label,'Trusted')]",
            "//section[@data-step-index='2']//*[normalize-space()='Trusted']",
        ]
        step2_trusted_checked_selectors = [
            "//section[@data-step-index='2']//*[@id='trustedRadio' and @aria-checked='true']",
            "//section[@data-step-index='2']//*[@role='radio' and @aria-labelledby='trustedLabel' and @aria-checked='true']",
            "//section[@data-step-index='2']//*[@role='radio' and contains(@aria-label,'Trusted') and @aria-checked='true']",
        ]
        step2_continue_selectors = [
            "//section[@data-step-index='2']//button[@aria-label='Continue']",
            "//section[@data-step-index='2']//button[.//span[normalize-space()='Continue']]",
        ]
        finish_selectors = [
            "//section[@data-step-index='3']//button[@aria-label='Finish']",
            "//section[@data-step-index='3']//button[.//span[normalize-space()='Finish']]",
            "//button[@aria-label='Finish']",
            "//button[.//span[normalize-space()='Finish']]",
        ]

        try:
            self._open_admin_configured_apps_page()
            if self._is_google_app_id_configured(client_id):
                return "already_configured"

            clicked = self._click_any(add_entry_selectors, timeout_ms=15_000, optional=True)
            if not clicked and not self._exists(search_field_selectors, timeout_ms=5_000):
                snippet = self._visible_text_excerpt(1200)
                raise RuntimeError(
                    f"Trusted-app entry point not found for {client_id}. page_text={snippet}"
                )
            time.sleep(1.0)
            field = self._find_any(search_field_selectors, timeout_ms=15_000)
            try:
                field.click(timeout=2_500)
            except Exception:
                pass
            field.fill("")
            try:
                field.type(client_id, delay=35)
            except TypeError:
                field.type(client_id)

            clicked_search = self._click_enabled_any(search_button_selectors, timeout_ms=10_000, optional=True)
            if not clicked_search:
                try:
                    field.press("Enter")
                except Exception:
                    self._require_page().keyboard.press("Enter")
            time.sleep(1.5)

            in_scope_step = self._exists(step1_continue_selectors + step1_scope_selectors, timeout_ms=2_000)
            if not in_scope_step:
                if not self._exists(step0_continue_selectors, timeout_ms=2_000):
                    self._click_any(result_selectors, timeout_ms=20_000)
                    time.sleep(1.2)
                in_scope_step = self._exists(step1_continue_selectors + step1_scope_selectors, timeout_ms=2_000)
            if not in_scope_step:
                self._click_enabled_any(step0_continue_selectors, timeout_ms=15_000)
                time.sleep(0.8)

            self._click_any(step1_scope_selectors, timeout_ms=8_000, optional=True)
            time.sleep(0.6)
            self._click_enabled_any(step1_continue_selectors, timeout_ms=15_000)
            time.sleep(0.8)

            self._select_google_wizard_radio(
                step2_trusted_selectors,
                step2_trusted_checked_selectors,
                timeout_ms=12_000,
            )
            time.sleep(0.8)
            self._click_enabled_any(step2_continue_selectors, timeout_ms=15_000)
            time.sleep(0.8)

            self._click_enabled_any(finish_selectors, timeout_ms=15_000)
            time.sleep(1.8)

            self._open_admin_configured_apps_page()
            if not self._is_google_app_id_configured(client_id):
                raise RuntimeError(f"Configured app {client_id} is not visible after setup")
            return "added"
        except Exception as exc:
            debug = self._capture_debug_state(f"trusted_app_{self._slug(client_id)}")
            message = str(exc)
            if debug:
                message = f"{message} | {debug}"
            raise RuntimeError(message) from exc

    # -----------------------------------------------------------------
    # Domain verification
    # -----------------------------------------------------------------

    def fetch_domain_verification_txt_record(self, domain: str) -> Dict[str, Any]:
        domain_name = str(domain or "").strip().lower()
        if not domain_name:
            raise RuntimeError("domain is required for verification")

        self._open_domain_management_page(domain_name)
        if self._is_domain_verified(domain_name):
            return {
                "domain": domain_name,
                "already_verified": True,
            }

        self._open_domain_verification_flow(domain_name)
        if self._is_domain_verified(domain_name):
            return {
                "domain": domain_name,
                "already_verified": True,
            }

        self._prepare_manual_domain_verification(domain_name)
        if self._is_domain_verified(domain_name):
            return {
                "domain": domain_name,
                "already_verified": True,
            }

        verification_value = self._read_domain_verification_value()
        if not verification_value:
            status_text = self._read_domain_verification_status_text()
            debug = self._capture_debug_state(f"domain_verification_missing_txt_{self._slug(domain_name)}")
            message = f"Failed to read google-site-verification TXT value for {domain_name}"
            if status_text:
                message = f"{message}; status_text={status_text!r}"
            if debug:
                message = f"{message} | {debug}"
            raise RuntimeError(message)

        return {
            "domain": domain_name,
            "already_verified": False,
            "record_type": "TXT",
            "record_name": "@",
            "verification_value": verification_value,
        }

    def confirm_domain_verification(
        self,
        domain: str,
        *,
        attempts: int = 12,
        sleep_seconds: float = 15.0,
    ) -> Dict[str, Any]:
        domain_name = str(domain or "").strip().lower()
        if not domain_name:
            raise RuntimeError("domain is required for verification confirmation")

        max_attempts = max(1, int(attempts))
        wait_seconds = max(0.0, float(sleep_seconds))
        last_status = ""

        for attempt in range(1, max_attempts + 1):
            self._open_domain_management_page(domain_name)
            self._open_domain_verification_flow(domain_name)
            self._prepare_manual_domain_verification(domain_name)
            if self._is_domain_verified(domain_name):
                return {
                    "domain": domain_name,
                    "verified": True,
                    "attempt": attempt,
                    "status_text": self._read_domain_verification_status_text(),
                }

            clicked = self._click_any(
                [
                    "//label[contains(normalize-space(),'Come back here and confirm once you have updated the code on your domain host')]",
                    "//*[contains(normalize-space(),'Come back here and confirm once you have updated the code on your domain host')]",
                ],
                timeout_ms=12_000,
                optional=True,
            )
            if clicked:
                time.sleep(0.8)
            self._click_any(
                [
                    "//a[@aria-label='Confirm']",
                    "//span[normalize-space()='Confirm']",
                    "//button[.//span[normalize-space()='Confirm']]",
                ],
                timeout_ms=12_000,
                optional=True,
            )
            time.sleep(1.5)

            if self._exists(
                [
                    "//h2[contains(normalize-space(),'Getting your domain ready')]",
                    "//*[contains(normalize-space(),'Google is verifying')]",
                ],
                timeout_ms=6_000,
            ):
                time.sleep(1.0)

            if self._is_domain_verified(domain_name):
                return {
                    "domain": domain_name,
                    "verified": True,
                    "attempt": attempt,
                    "status_text": self._read_domain_verification_status_text(),
                }

            last_status = self._read_domain_verification_status_text()
            if attempt < max_attempts and wait_seconds > 0:
                time.sleep(wait_seconds)

        return {
            "domain": domain_name,
            "verified": False,
            "attempt": max_attempts,
            "status_text": last_status,
        }

    def _open_domain_management_page(self, domain: str) -> None:
        self._open_admin_console_page(
            "https://admin.google.com/ac/domains/manage?hl=en",
            [
                f"//*[@data-domain-name='{domain}']",
                f"//*[contains(normalize-space(), '{domain}')]",
                "//*[contains(normalize-space(),'Domains')]",
                "//*[contains(normalize-space(),'Manage domains')]",
            ],
        )

    def _open_domain_verification_flow(self, domain: str) -> None:
        if self._exists(
            [
                f"//*[contains(normalize-space(), 'Verify you own {domain}')]",
                "//h2[contains(normalize-space(),'Add verification code')]",
                "//span[normalize-space()='Switch to manual verification']",
            ],
            timeout_ms=4_000,
        ):
            return

        clicked_verify = self._click_any(
            [
                f"//tr[.//*[contains(normalize-space(), '{domain}')]]//button[normalize-space()='Verify domain']",
                f"//*[@role='row'][.//*[contains(normalize-space(), '{domain}')]]//button[normalize-space()='Verify domain']",
                f"//*[@role='row'][.//*[contains(normalize-space(), '{domain}')]]//button[.//*[normalize-space()='Verify domain']]",
                "//button[normalize-space()='Verify domain']",
                "//button[.//span[normalize-space()='Verify domain']]",
            ],
            timeout_ms=12_000,
            optional=True,
        )
        if not clicked_verify:
            # Fallback for legacy table variants that require opening the row first.
            self._click_any(
                [
                    f"//a[@data-domain-name='{domain}']",
                    f"//*[@data-domain-name='{domain}']",
                    f"//*[contains(normalize-space(), '{domain}')]",
                ],
                timeout_ms=8_000,
                optional=True,
            )
            time.sleep(0.8)
            self._click_any(
                [
                    "//button[normalize-space()='Verify domain']",
                    "//button[.//span[normalize-space()='Verify domain']]",
                ],
                timeout_ms=10_000,
                optional=True,
            )
        time.sleep(1.2)

    def _prepare_manual_domain_verification(self, domain: str) -> None:
        domain_name = str(domain or "").strip().lower()
        if not domain_name:
            return
        if self._is_domain_verified(domain_name):
            return

        # Setup flow starts with "Get started" on many fresh tenants.
        self._click_any(
            [
                "//a[@aria-label='Get started']",
                "//span[normalize-space()='Get started']",
                "//button[.//span[normalize-space()='Get started']]",
            ],
            timeout_ms=10_000,
            optional=True,
        )
        time.sleep(0.8)
        if self._is_domain_verified(domain_name):
            return

        # Keep this explicit to avoid brittle host-specific auto flows.
        switched = self._click_any(
            [
                "//span[normalize-space()='Switch to manual verification']",
                "//button[.//span[normalize-space()='Switch to manual verification']]",
            ],
            timeout_ms=12_000,
            optional=True,
        )
        if switched:
            time.sleep(0.8)

        self._click_any(
            [
                "//label[contains(normalize-space(),'My domain uses a different host')]",
                "//*[@role='checkbox' and contains(@aria-label,'different host')]",
                "//*[contains(normalize-space(),'My domain uses a different host')]",
            ],
            timeout_ms=8_000,
            optional=True,
        )
        time.sleep(0.5)

        self._click_any(
            [
                "//button[normalize-space()='Continue']",
                "//button[.//span[normalize-space()='Continue']]",
                "//span[normalize-space()='Continue']",
            ],
            timeout_ms=10_000,
            optional=True,
        )
        time.sleep(0.8)

        # Ensure TXT option is selected before reading value/confirming.
        self._click_any(
            [
                "//button[.//span[normalize-space()='TXT record']]",
                "//span[normalize-space()='TXT record']",
            ],
            timeout_ms=8_000,
            optional=True,
        )
        time.sleep(0.5)

    def _is_domain_verified(self, domain: str) -> bool:
        domain_name = str(domain or "").strip().lower()
        if self._exists(
            [
                f"//h3[contains(normalize-space(), 'You verified') and contains(normalize-space(), '{domain_name}')]",
                f"//tr[.//*[contains(normalize-space(), '{domain_name}')]]//*[normalize-space()='Verified']",
                f"//*[@role='row'][.//*[contains(normalize-space(), '{domain_name}')]]//*[normalize-space()='Verified']",
                "//h2[contains(normalize-space(), 'all set')]",
                "//h2[contains(normalize-space(), 'You’re all set')]",
                "//h2[contains(normalize-space(), \"You're all set\")]",
                "//h2[contains(normalize-space(), 'Gmail is activated')]",
                "//span[contains(normalize-space(), 'Set up DKIM')]",
            ],
            timeout_ms=6_000,
        ):
            return True

        try:
            body_text = self._require_page().locator("body").inner_text(timeout=5_000)
        except Exception:
            return False

        normalized = " ".join(str(body_text or "").split()).lower()
        return (
            domain_name in normalized
            and "verified" in normalized
            and (
                "gmail activated" in normalized
                or "you verified" in normalized
                or "you're all set" in normalized
                or "you’re all set" in normalized
            )
        )

    def _read_domain_verification_value(self) -> str:
        value = self._inner_text_any(
            [
                "//input[@id='Value' and @readonly and contains(@value,'google-site-verification=')]",
            ],
            timeout_ms=8_000,
            optional=True,
        )
        if value:
            return str(value).strip()

        try:
            locator = self._find_any(
                [
                    "//input[@id='Value' and @readonly and contains(@value,'google-site-verification=')]",
                    "//input[@readonly and contains(@value,'google-site-verification=')]",
                ],
                timeout_ms=8_000,
            )
            input_value = str(locator.input_value(timeout=5_000) or "").strip()
            if input_value:
                return input_value
        except Exception:
            pass

        try:
            locator = self._find_any(
                [
                    "//button[contains(@aria-label,'google-site-verification=')]",
                ],
                timeout_ms=6_000,
            )
            aria_label = str(locator.get_attribute("aria-label") or "").strip()
            if aria_label.lower().startswith("copy "):
                aria_label = aria_label[5:].strip()
            if aria_label:
                return aria_label
        except Exception:
            pass

        try:
            page = self._require_page()
            value = page.evaluate(
                """
                () => {
                  const selectors = [
                    "input[readonly][id='Value']",
                    "input[readonly][value^='google-site-verification=']",
                  ];
                  for (const selector of selectors) {
                    const element = document.querySelector(selector);
                    if (element && element.value) return element.value;
                  }
                  const copyButton = document.querySelector("button[aria-label*='google-site-verification=']");
                  if (!copyButton) return "";
                  const label = copyButton.getAttribute("aria-label") || "";
                  return label.replace(/^Copy\\s+/i, "");
                }
                """
            )
            return str(value or "").strip()
        except Exception:
            return ""

    def _read_domain_verification_status_text(self) -> str:
        return self._inner_text_any(
            [
                "//h2[contains(normalize-space(), 'Getting your domain ready')]",
                "//h2[contains(normalize-space(), 'all set')]",
                "//h2[contains(normalize-space(), 'You’re all set')]",
                "//h2[contains(normalize-space(), \"You're all set\")]",
                "//h2[contains(normalize-space(), 'Gmail is activated')]",
                f"//h3[contains(normalize-space(), 'You verified')]",
            ],
            timeout_ms=6_000,
            optional=True,
        )

    def _open_admin_home_page(self) -> None:
        self._open_admin_console_page(
            "https://admin.google.com/ac/home",
            [
                "//*[contains(normalize-space(),'Admin')]",
                "//*[contains(normalize-space(),'Dashboard')]",
                "//*[contains(normalize-space(),'Home')]",
                "//a[contains(@href,'/ac/domains/manage')]",
                "//a[contains(@href,'/ac/owl/list')]",
            ],
            allow_directory_fallback=True,
        )

    # -----------------------------------------------------------------
    # DKIM setup/authentication
    # -----------------------------------------------------------------

    def fetch_dkim_txt_record(self, domain: str) -> Dict[str, Any]:
        domain_name = str(domain or "").strip().lower()
        if not domain_name:
            raise RuntimeError("domain is required for DKIM setup")

        self._open_dkim_page()
        self._select_dkim_domain(domain_name)
        if self._is_dkim_enabled():
            return {
                "domain": domain_name,
                "already_enabled": True,
            }

        host, value = self._read_dkim_record(optional=True)
        generated = False
        if not host or not value:
            self._generate_dkim_record()
            generated = True
            time.sleep(2.0)
            self._open_dkim_page()
            self._select_dkim_domain(domain_name)
            host, value = self._read_dkim_record(optional=False)

        if not host or not value:
            raise RuntimeError("Failed to read DKIM DNS host/value from Google Admin")

        return {
            "domain": domain_name,
            "already_enabled": False,
            "generated_record": generated,
            "dns_host": host,
            "dns_value": value,
        }

    def start_dkim_authentication(
        self,
        domain: str,
        *,
        attempts: int = 5,
        sleep_seconds: float = 60.0,
    ) -> Dict[str, Any]:
        domain_name = str(domain or "").strip().lower()
        if not domain_name:
            raise RuntimeError("domain is required for DKIM authentication")

        max_attempts = max(1, int(attempts))
        wait_seconds = max(0.0, float(sleep_seconds))
        status_text = ""

        for attempt in range(1, max_attempts + 1):
            self._open_dkim_page()
            self._select_dkim_domain(domain_name)
            if self._is_dkim_enabled():
                return {
                    "domain": domain_name,
                    "enabled": True,
                    "attempt": attempt,
                    "already_enabled": attempt == 1,
                    "status_text": self._read_dkim_status_text(),
                }

            self._dismiss_dkim_overlays()
            self._click_start_dkim_authentication_dom() or self._click_any(
                [
                    "//div[@role='button'][.//span[translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='start authentication']]",
                    "//button[.//span[translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='start authentication']]",
                ],
                timeout_ms=12_000,
                optional=True,
            )
            time.sleep(1.8)

            self._open_dkim_page()
            self._select_dkim_domain(domain_name)
            status_text = self._read_dkim_status_text()
            if self._is_dkim_enabled():
                return {
                    "domain": domain_name,
                    "enabled": True,
                    "attempt": attempt,
                    "already_enabled": False,
                    "status_text": status_text,
                }

            if attempt < max_attempts and wait_seconds > 0:
                time.sleep(wait_seconds)

        return {
            "domain": domain_name,
            "enabled": False,
            "attempt": max_attempts,
            "status_text": status_text,
        }

    def _open_dkim_page(self) -> None:
        self._open_admin_console_page(
            "https://admin.google.com/ac/apps/gmail/authenticateemail?hl=en",
            [
                "//span[normalize-space()='Generate new record']",
                "//button[.//span[normalize-space()='Generate new record']]",
                "//span[normalize-space()='Start authentication']",
                "//button[.//span[normalize-space()='Start authentication']]",
                "//span[normalize-space()='Stop authentication']",
                "//button[.//span[normalize-space()='Stop authentication']]",
                "//*[contains(normalize-space(),'Authenticate email')]",
                "//div[@role='listbox'][contains(@aria-label,'domain')]",
            ],
        )

    def _select_dkim_domain(self, domain: str) -> None:
        domain_name = str(domain or "").strip().lower()
        if not domain_name:
            raise RuntimeError("DKIM domain is required")
        page = self._require_page()

        def selected_text() -> str:
            return self._inner_text_any(
                [
                    "//div[@role='listbox' and contains(@aria-label,'Selected domain')]",
                    "//div[@role='listbox'][contains(@aria-label,'domain')]",
                    "//*[@role='combobox' and contains(@aria-label,'Selected domain')]",
                    "//*[@role='combobox' and contains(@aria-label,'domain')]",
                    "//button[contains(@aria-label,'Selected domain')]",
                    "//button[contains(@aria-label,'domain')]",
                ],
                timeout_ms=2_000,
                optional=True,
            ).lower()

        if domain_name in selected_text():
            return

        self._click_any(
            [
                "//div[@role='listbox' and contains(@aria-label,'Selected domain')]",
                "//div[@role='listbox'][contains(@aria-label,'domain')]",
                "//*[@role='combobox' and contains(@aria-label,'Selected domain')]",
                "//*[@role='combobox' and contains(@aria-label,'domain')]",
                "//button[contains(@aria-label,'Selected domain')]",
                "//button[contains(@aria-label,'domain')]",
            ],
            timeout_ms=8_000,
            optional=True,
        )
        clicked = self._click_any(
            [
                f"//*[@role='option' and @data-value='{domain_name}']",
                f"//*[@role='option' and normalize-space()='{domain_name}']",
                f"//*[@role='option' and contains(normalize-space(), '{domain_name}')]",
            ],
            timeout_ms=8_000,
            optional=True,
        )
        if clicked:
            time.sleep(0.8)
            if domain_name in selected_text():
                return

        for _ in range(120):
            for selector in [
                f"//*[@role='option' and @data-value='{domain_name}']",
                f"//*[@role='option' and contains(translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{domain_name}')]",
                f"//*[(@data-value='{domain_name}' or @data-domain='{domain_name}') and (self::div or self::li or self::span)]",
            ]:
                option = page.locator(self._selector(selector)).first
                try:
                    option.wait_for(state="attached", timeout=350)
                    option.scroll_into_view_if_needed(timeout=1_000)
                    option.click(timeout=1_500)
                    time.sleep(0.8)
                    if domain_name in selected_text():
                        return
                except Exception:
                    pass

            scroll_result = page.evaluate(
                """() => {
                    const candidates = Array.from(document.querySelectorAll(
                      '[role="listbox"], [role="menu"], [role="presentation"] [role="list"], [role="presentation"], .VfPpkd-xl07Ob-XxIAqe'
                    ));
                    const popup = candidates.find((el) => el.scrollHeight > el.clientHeight + 8)
                      || candidates.find((el) => el.querySelector && el.querySelector('[role="option"]'));
                    if (!popup) return { scrolled: false, reason: 'no-scroll-container' };
                    const before = popup.scrollTop || 0;
                    const maxTop = Math.max(0, popup.scrollHeight - popup.clientHeight);
                    const delta = Math.max(260, Math.floor((popup.clientHeight || 420) * 0.85));
                    popup.scrollTop = Math.min(maxTop, before + delta);
                    popup.dispatchEvent(new Event('scroll', { bubbles: true }));
                    return { scrolled: popup.scrollTop > before, before, after: popup.scrollTop, maxTop };
                }"""
            )
            time.sleep(0.25)
            if isinstance(scroll_result, dict) and not scroll_result.get("scrolled"):
                if float(scroll_result.get("after") or 0) >= float(scroll_result.get("maxTop") or 0):
                    break

        debug = self._capture_debug_state(f"dkim_domain_not_selectable_{self._slug(domain_name)}")
        raise RuntimeError(f"Could not select DKIM domain {domain_name}. {debug}")

    def _is_dkim_enabled(self) -> bool:
        if self._exists(
            [
                "//div[@role='button']//span[translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='stop authentication']",
                "//button[.//span[translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='stop authentication']]",
                "//span[translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='stop authentication']",
            ],
            timeout_ms=6_000,
        ):
            return True

        status_text = self._read_dkim_status_text().lower()
        return "authenticating email with dkim" in status_text

    def _dismiss_dkim_overlays(self) -> None:
        self._click_dkim_text_control_dom("got it") or self._click_any(
            [
                "//span[translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='got it']",
                "//button[.//span[translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='got it']]",
            ],
            timeout_ms=2_000,
            optional=True,
        )
        time.sleep(0.4)

    def _click_start_dkim_authentication_dom(self) -> bool:
        return self._click_dkim_text_control_dom("start authentication")

    def _click_dkim_text_control_dom(self, label: str) -> bool:
        page = self._require_page()
        expected_label = str(label or "").strip().lower()
        if not expected_label:
            return False
        try:
            return bool(
                page.evaluate(
                    """({ expectedLabel }) => {
                      const visible = (el) => {
                        const style = window.getComputedStyle(el);
                        const rect = el.getBoundingClientRect();
                        return style.visibility !== 'hidden'
                          && style.display !== 'none'
                          && rect.width > 0
                          && rect.height > 0;
                      };
                      const clickTarget = (target) => {
                        target.scrollIntoView({ block: 'center', inline: 'nearest' });
                        target.click();
                        return true;
                      };
                      const controls = Array.from(document.querySelectorAll('button, [role="button"]'));
                      for (const el of controls) {
                        const text = (el.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                        if (!text.includes(expectedLabel) || !visible(el)) continue;
                        if (el.disabled || el.getAttribute('aria-disabled') === 'true') continue;
                        return clickTarget(el);
                      }
                      const labels = Array.from(document.querySelectorAll('span, div'));
                      for (const el of labels) {
                        const text = (el.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                        if (text !== expectedLabel || !visible(el)) continue;
                        const target = el.closest('button, [role="button"]') || el;
                        if (target.disabled || target.getAttribute('aria-disabled') === 'true') continue;
                        return clickTarget(target);
                      }
                      return false;
                    }""",
                    {"expectedLabel": expected_label},
                )
            )
        except Exception:
            return False

    def _generate_dkim_record(self) -> None:
        self._click_any(
            [
                "//span[normalize-space()='Generate new record']",
                "//button[.//span[normalize-space()='Generate new record']]",
            ],
            timeout_ms=12_000,
        )
        time.sleep(1.0)
        self._click_any(
            [
                "//span[normalize-space()='Generate']",
                "//button[.//span[normalize-space()='Generate']]",
            ],
            timeout_ms=12_000,
        )

    def _read_dkim_record(self, *, optional: bool = False) -> Tuple[str, str]:
        host = self._inner_text_any(
            [
                "//div[contains(normalize-space(),'DNS Host name')]/strong",
                "//div[contains(text(),'DNS Host name')]/strong",
            ],
            timeout_ms=10_000,
            optional=optional,
        )
        value = self._inner_text_any(
            [
                "//div[contains(normalize-space(),'TXT record value')]/strong",
                "//div[contains(text(),'TXT record value')]/strong",
            ],
            timeout_ms=10_000,
            optional=optional,
        )

        clean_host = str(host or "").strip().rstrip(".")
        clean_value = re.sub(r"\s+", " ", str(value or "").strip())
        if clean_value.startswith("\"") and clean_value.endswith("\"") and len(clean_value) >= 2:
            clean_value = clean_value[1:-1]
        return clean_host, clean_value

    def _read_dkim_status_text(self) -> str:
        return self._inner_text_any(
            [
                "//div[contains(normalize-space(),'Status:')]",
                "//*[contains(normalize-space(),'Status:')]",
            ],
            timeout_ms=5_000,
            optional=True,
        )

    @staticmethod
    def _is_valid_google_oauth_client_id(client_id: str) -> bool:
        value = str(client_id or "").strip().lower()
        return bool(value and value.endswith(".apps.googleusercontent.com"))

    # -----------------------------------------------------------------
    # MFA + 1Password internals
    # -----------------------------------------------------------------

    def _login_google_account(
        self,
        email: str,
        password: str,
        *,
        onepassword: Optional[OnePasswordCliClient] = None,
        verify_profile: bool = True,
    ) -> None:
        clean_email = str(email or "").strip().lower()
        signin_url = "https://accounts.google.com/signin/v2/identifier"
        if clean_email:
            signin_url = f"{signin_url}?Email={quote(clean_email)}"
        self._goto(signin_url)
        time.sleep(1)
        self._complete_google_signin_flow(
            email=clean_email or email,
            password=password,
            onepassword=onepassword,
            context="login",
        )
        self._dismiss_sign_in_to_chrome_prompt_if_present()
        self._drain_google_login_interstitials(clean_email or email, onepassword)
        if verify_profile:
            self._ensure_expected_google_profile(
                clean_email or email,
                password,
                onepassword=onepassword,
                context="login",
            )

    def _drain_google_login_interstitials(
        self,
        email: str,
        onepassword: Optional[OnePasswordCliClient],
    ) -> None:
        # New Google accounts can bounce through multiple interstitial pages
        # (speedbump, terms, promo pages). Handle known safe prompts only.
        for _ in range(6):
            handled = False
            if self._dismiss_sign_in_to_chrome_prompt_if_present():
                handled = True
            if self._accept_new_account_terms_if_present():
                handled = True
            if self._accept_pending_terms_of_service_if_present():
                handled = True
            self._maybe_complete_totp_challenge(email, onepassword)
            if self._dismiss_common_interstitials():
                handled = True
            if not handled:
                break
            time.sleep(0.8)

    def _open_2sv_page(
        self,
        email: str,
        password: str,
        *,
        onepassword: Optional[OnePasswordCliClient] = None,
    ) -> None:
        self._goto("https://myaccount.google.com/signinoptions/two-step-verification")
        self._accept_pending_terms_of_service_if_present()
        self._maybe_complete_reauth(email, password, onepassword=onepassword)
        self._ensure_expected_google_profile(email, password, onepassword=onepassword, context="2sv")
        self._goto("https://myaccount.google.com/signinoptions/two-step-verification")
        self._accept_pending_terms_of_service_if_present()
        self._dismiss_sign_in_to_chrome_prompt_if_present()

        self._dismiss_phone_prompt_if_present()
        # Use only the Authenticator row from the 2SV page (never phone setup).
        click = self._click_any(
            [
                "//a[contains(@href,'two-step-verification/authenticator')]",
                "//li[.//div[normalize-space()='Authenticator']]",
                "//span[normalize-space()='Authenticator']",
                "//div[normalize-space()='Authenticator']",
            ],
            timeout_ms=12_000,
            optional=True,
        )
        if not click and "two-step-verification/authenticator" not in self._safe_url().lower():
            self._goto("https://myaccount.google.com/two-step-verification/authenticator")

        self._accept_pending_terms_of_service_if_present()
        self._dismiss_sign_in_to_chrome_prompt_if_present()
        self._dismiss_phone_prompt_if_present()

    def _maybe_complete_reauth(
        self,
        email: str,
        password: str,
        *,
        onepassword: Optional[OnePasswordCliClient] = None,
    ) -> None:
        current_url = self._safe_url().lower()
        if "accounts.google.com" not in current_url and "challenge" not in current_url and not self._has_google_sign_in_prompt():
            return
        self._complete_google_signin_flow(
            email=email,
            password=password,
            onepassword=onepassword or self._admin_onepassword,
            context="reauth",
        )
        self._drain_google_login_interstitials(email, onepassword or self._admin_onepassword)

    def _complete_google_signin_flow(
        self,
        *,
        email: str,
        password: str,
        onepassword: Optional[OnePasswordCliClient],
        context: str,
    ) -> None:
        clean_email = str(email or "").strip().lower()
        email_selectors = ["input#identifierId", "input[type='email']"]
        password_selectors = [
            "input[name='Passwd']",
            "input[type='password']:not([name='hiddenPassword'])",
        ]
        totp_selectors = [
            "input[name='totpPin']",
            "input[name='Pin']",
            "input#totpPin",
            "input[aria-label*='code']",
            "//input[contains(translate(@aria-label, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'code')]",
            "input[inputmode='numeric']",
            "input[type='tel'][autocomplete='one-time-code']",
            "input[type='tel']",
        ]

        for _ in range(10):
            self._raise_if_google_signin_rejected(email=email)
            self._raise_if_google_signin_challenge_blocked(email=email, context=context)
            if not self._has_google_sign_in_prompt():
                return

            # Google changes routing frequently; classify by visible controls, not URL.
            # Keep this order aligned with the worker policy so an account chooser can
            # never steal focus before a password or TOTP challenge is handled.
            if self._exists(totp_selectors, timeout_ms=1_500):
                self._maybe_complete_totp_challenge(email, onepassword)
                time.sleep(0.8)
                continue

            if self._exists(password_selectors, timeout_ms=2_500):
                self._fill_any(password_selectors, password, timeout_ms=22_000)
                self._click_google_action(
                    ["#passwordNext", "//*[@id='passwordNext']", "//span[text()='Next']"],
                    ["Next", "Continue"],
                    timeout_ms=10_000,
                )
                time.sleep(1.0)
                continue

            if self._exists(email_selectors, timeout_ms=2_500):
                self._fill_any(email_selectors, clean_email or email, timeout_ms=16_000)
                self._click_google_action(
                    ["#identifierNext", "//*[@id='identifierNext']", "//span[text()='Next']"],
                    ["Next", "Continue"],
                    timeout_ms=10_000,
                )
                time.sleep(1.0)
                continue

            try_password = self._click_google_action(
                [
                    "//span[text()='Try another way']",
                    "//button//span[text()='Try another way']",
                    "//span[contains(text(),'Try another')]",
                ],
                ["Try another way", "Try another", "More ways"],
                timeout_ms=3_500,
                optional=True,
            )
            if try_password:
                time.sleep(0.8)
                continue

            use_password = self._click_google_action(
                [
                    "//span[contains(text(),'Enter your password')]",
                    "//span[contains(text(),'Use your password')]",
                    "//div[contains(text(),'Enter your password')]",
                ],
                ["Enter your password", "Use your password", "Password"],
                timeout_ms=4_000,
                optional=True,
            )
            if use_password:
                time.sleep(0.8)
                continue

            if self._handle_google_account_chooser(clean_email):
                time.sleep(0.8)
                continue

            time.sleep(0.8)

        if self._has_google_sign_in_prompt():
            self._raise_if_google_signin_challenge_blocked(email=email, context=context)
            debug = self._capture_debug_state(f"google_signin_not_completed_{context}_{self._slug(email)}")
            message = f"Google sign-in did not complete for {email} during {context}. url={self._safe_url()}"
            if debug:
                message = f"{message} {debug}"
            raise RuntimeError(message)

    def _handle_google_account_chooser(self, email: str) -> bool:
        clean_email = str(email or "").strip().lower()
        chooser_visible = self._exists(
            [
                "//*[contains(normalize-space(),'Choose an account')]",
                "//*[contains(normalize-space(),'Use another account')]",
                "[data-identifier]",
            ],
            timeout_ms=1_500,
        )
        if not chooser_visible:
            return False

        if clean_email:
            clicked_expected = self._click_expected_google_account(clean_email, timeout_ms=3_000)
            if clicked_expected:
                logger.info("Selected expected Google account %s from account chooser", clean_email)
                return True

        clicked_other = self._click_google_action(
            [
                "//div[text()='Use another account']",
                "//button//span[text()='Use another account']",
                "//span[text()='Use another account']",
            ],
            ["Use another account", "Add account", "Sign in with another account"],
            timeout_ms=3_000,
            optional=True,
        )
        if clicked_other:
            logger.info("Account chooser did not expose expected Google account %s; choosing alternate sign-in", clean_email)
            return True

        debug = self._capture_debug_state(f"google_account_chooser_unhandled_{self._slug(clean_email)}")
        message = (
            f"Google account chooser was shown but expected account {clean_email or 'unknown'} "
            "was not selectable and 'Use another account' was unavailable."
        )
        if debug:
            message = f"{message} {debug}"
        raise RuntimeError(message)

    def _raise_if_google_signin_challenge_blocked(
        self,
        *,
        email: Optional[str] = None,
        context: str = "login",
    ) -> None:
        current_url = self._safe_url()
        current_path = current_url.split("?", 1)[0].lower()
        clean_email = str(email or "").strip().lower() or "admin account"
        excerpt = self._visible_text_excerpt(limit=700)
        normalized_excerpt = excerpt.lower()
        phone_challenge = (
            "enter a phone number to get a text message with a verification code" in normalized_excerpt
            or (
                "verify it" in normalized_excerpt
                and "phone number" in normalized_excerpt
                and "text message" in normalized_excerpt
            )
        )
        if phone_challenge:
            text_suffix = f" page_text={excerpt}" if excerpt else ""
            raise GoogleSignInChallengeBlocked(
                "Google sign-in challenge blocked automation. "
                f"failing_step={context}; "
                "why=Google requires phone-number SMS verification for this account, "
                "which the worker cannot complete safely; "
                f"current_state=google_phone_verification_challenge for {clean_email}; "
                "next_action=Complete the Google phone verification manually in a trusted browser "
                "or use a different Google account/domain that does not trigger SMS verification, then retry. "
                f"url={current_url}{text_suffix}"
            )

        if "/speedbump/gaplustos" not in current_path:
            return

        if (
            "welcome to your new account" in normalized_excerpt
            and "organization administrator manages this account" in normalized_excerpt
        ):
            clicked = self._click_google_action(
                [
                    "input[value='I understand']",
                    "//input[@value='I understand']",
                    "//button[normalize-space()='I understand']",
                    "//button[.//span[normalize-space()='I understand']]",
                    "//span[normalize-space()='I understand']",
                ],
                ["I understand", "Understand", "Continue"],
                timeout_ms=8_000,
                optional=True,
            )
            if clicked:
                logger.info("Accepted Google new-account welcome speedbump for %s during %s", clean_email, context)
                time.sleep(2.0)
                return

        text_suffix = f" page_text={excerpt}" if excerpt else ""
        raise GoogleSignInChallengeBlocked(
            "Google sign-in challenge blocked automation. "
            "failing_step=login_admin_console; "
            "why=Google redirected the admin account to accounts.google.com/speedbump/gaplustos, "
            "a manual sign-in challenge that the worker cannot complete safely; "
            f"current_state=admin_login_challenge_blocked during {context} for {clean_email}; "
            "next_action=Open the Google Admin account in a trusted manual browser session, clear the "
            "Google sign-in challenge or replace the admin credentials/account, then retry Google provisioning. "
            f"url={current_url}{text_suffix}"
        )

    def _raise_if_google_signin_rejected(self, *, email: Optional[str] = None) -> None:
        current_url = self._safe_url().lower()
        excerpt = self._visible_text_excerpt(limit=1400)
        normalized_excerpt = excerpt.lower()
        detail = excerpt or current_url
        clean_email = str(email or "").strip().lower()

        if "wrong password" in normalized_excerpt or "couldn’t find your google account" in normalized_excerpt or "couldn't find your google account" in normalized_excerpt:
            raise RuntimeError(
                f"Google rejected sign-in for {clean_email or 'account'}: wrong email or password. {detail}"
            )
        if "too many failed attempts" in normalized_excerpt or "try again later" in normalized_excerpt:
            raise RuntimeError(
                f"Google rejected sign-in for {clean_email or 'account'}: too many failed attempts. {detail}"
            )

        if "accounts.google.com/v3/signin/rejected" not in current_url and "couldn’t sign you in" not in normalized_excerpt and "couldn't sign you in" not in normalized_excerpt:
            return

        if "your account has not been verified" in normalized_excerpt:
            raise RuntimeError(
                f"Google rejected sign-in for {clean_email or 'account'}: account is not verified yet. {detail}"
            )
        if "ask your google workspace administrator" in normalized_excerpt:
            raise RuntimeError(
                f"Google rejected sign-in for {clean_email or 'account'}: workspace/domain setup is incomplete. {detail}"
            )
        raise RuntimeError(f"Google rejected sign-in for {clean_email or 'account'}. {detail}")

    def _dismiss_common_interstitials(self) -> bool:
        clicked_any = False
        for _ in range(3):
            clicked = self._click_any(
                [
                    "input[value='I understand']",
                    "//input[@value='I understand']",
                    "//span[text()='Not now']",
                    "//button//span[text()='Not now']",
                    "//span[text()='Skip']",
                    "//button//span[text()='Skip']",
                    "//span[text()='I understand']",
                    "//button//span[text()='I understand']",
                ],
                timeout_ms=3_000,
                optional=True,
            )
            if not clicked:
                return clicked_any
            clicked_any = True
            time.sleep(0.8)
        return clicked_any

    def _accept_new_account_terms_if_present(self) -> bool:
        clicked = self._click_any(
            [
                "input[value='I understand']",
                "//input[@value='I understand']",
                "//button[.//span[text()='I understand']]",
                "//span[text()='I understand']",
            ],
            timeout_ms=8_000,
            optional=True,
        )
        if clicked:
            time.sleep(2)
            return True
        return False

    def _accept_pending_terms_of_service_if_present(self) -> bool:
        # Admin billing interstitial: /ac/billing/interstitial/pendingtermsofservices
        current_url = self._safe_url().lower()
        maybe_terms_page = "pendingtermsofservices" in current_url
        if not maybe_terms_page:
            maybe_terms_page = self._exists(
                [
                    "//span[contains(translate(normalize-space(.), 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'ACCEPT TERMS OF SERVICE')]",
                    "//button[.//span[contains(translate(normalize-space(.), 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'ACCEPT TERMS OF SERVICE')]]",
                ],
                timeout_ms=1_500,
            )
        if not maybe_terms_page:
            return False

        try:
            self._require_page().evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.5)
        except Exception:
            pass

        clicked = self._click_any(
            [
                "//button[.//span[contains(translate(normalize-space(.), 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'ACCEPT TERMS OF SERVICE')]]",
                "//span[contains(translate(normalize-space(.), 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'ACCEPT TERMS OF SERVICE')]",
                "//button[contains(@aria-label, 'Accept Terms of Service')]",
            ],
            timeout_ms=10_000,
            optional=True,
        )
        if clicked:
            time.sleep(2.0)
            return True
        return False

    def _is_2sv_already_enabled(self) -> bool:
        selectors = [
            "//span[text()='Turn off']",
            "//button//span[text()='Turn off']",
            "//span[contains(text(),'Manage 2-Step Verification')]",
            "//button//span[contains(text(),'Manage 2-Step Verification')]",
            "//div[@data-label='On' and @data-value='true']",
            "//div[contains(text(),'2-Step Verification is on')]",
        ]

        page = self._require_page()
        for selector in selectors:
            locator = page.locator(self._selector(selector))
            try:
                if locator.count() <= 0:
                    continue
                candidate = locator.first
                try:
                    if candidate.is_visible():
                        return True
                except Exception:
                    # Some nodes are not directly visible but still indicate enabled state.
                    return True
            except Exception:
                continue

        text = self._visible_text_excerpt(limit=3000).lower()
        if "2-step verification is on" in text:
            return True
        if "manage 2-step verification" in text:
            return True
        if "turn off" in text and "2-step verification" in text:
            return True
        return False

    def _start_authenticator_enrollment(self) -> str:
        if "two-step-verification/authenticator" not in self._safe_url().lower():
            self._goto("https://myaccount.google.com/two-step-verification/authenticator")
            time.sleep(1.0)

        self._dismiss_sign_in_to_chrome_prompt_if_present()
        self._dismiss_phone_prompt_if_present()

        # Authenticator can already be connected from a previous partial run.
        if self._exists(
            [
                "//div[contains(text(),'Your authenticator')]",
                "//span[contains(text(),'Added just now')]",
                "//span[contains(text(),'Added ') and contains(text(),'ago')]",
                "//span[contains(text(),'Change authenticator app')]",
            ],
            timeout_ms=4_000,
        ) and not self._exists(
            [
                "//span[normalize-space()='Set up authenticator']",
                "//button[.//span[normalize-space()='Set up authenticator']]",
            ],
            timeout_ms=2_000,
        ):
            return "existing_authenticator"

        clicked = self._click_any(
            [
                "//button[.//span[normalize-space()='Set up authenticator']]",
                "//span[normalize-space()='Set up authenticator']",
            ],
            timeout_ms=15_000,
            optional=True,
        )

        if not clicked:
            current_url = self._safe_url().lower()
            if "two-step-verification/authenticator" in current_url and self._exists(
                ["//span[contains(text(),'Set up authenticator')]"],
                timeout_ms=2_000,
            ):
                clicked = "page:set-up-authenticator"

        if not clicked:
            snippet = self._visible_text_excerpt()
            raise RuntimeError(f"Authenticator entry point not found. url={self._safe_url()} text={snippet}")
        if not self._exists(["//div[@role='dialog']//h2[contains(text(),'Set up authenticator app')]"], timeout_ms=5_000):
            self._click_any(
                [
                    "//button[.//span[normalize-space()='Set up authenticator']]",
                    "//span[normalize-space()='Set up authenticator']",
                ],
                timeout_ms=10_000,
                optional=True,
            )

        self._click_any(
            [
                "//div[@role='dialog']//span[contains(text(),\"Can't scan it?\")]",
                "//div[@role='dialog']//span[contains(text(),'scan it?')]",
                "//span[contains(text(),\"Can't scan it?\")]",
                "//span[contains(text(),'scan it?')]",
            ],
            timeout_ms=15_000,
            optional=False,
        )
        if not self._exists(["//div[@role='dialog'][.//text()[contains(.,'Enter your email address and this key')]]"], timeout_ms=8_000):
            raise RuntimeError("Authenticator secret modal did not show after clicking \"Can't scan it?\"")
        return "new_setup"

    def _dismiss_phone_prompt_if_present(self) -> bool:
        clicked = self._click_any(
            [
                "//button[.//span[text()='Cancel']]",
                "//span[text()='Cancel']",
                "//div[@role='dialog']//button[contains(.,'Cancel')]",
            ],
            timeout_ms=1_500,
            optional=True,
        )
        if clicked:
            time.sleep(1)
            return True
        return False

    def _dismiss_sign_in_to_chrome_prompt_if_present(self) -> bool:
        clicked = self._click_any(
            [
                "//button[.//span[text()='Use Chrome Without an Account']]",
                "//span[text()='Use Chrome Without an Account']",
            ],
            timeout_ms=1_500,
            optional=True,
        )
        if clicked:
            time.sleep(1)
            return True
        return False

    def _extract_authenticator_secret(self) -> str:
        page = self._require_page()
        regex = re.compile(r"(?:[A-Z2-7]{4}\s*){4,}")
        uri_regex = re.compile(r"otpauth://totp/[^\"'\\s>]+", re.IGNORECASE)
        modal_text = ""
        try:
            modal_text = page.locator("xpath=//div[@role='dialog']").first.inner_text(timeout=4_000)
        except Exception:
            modal_text = ""

        if modal_text:
            key_hint = re.search(
                r"Enter your email address and this key.*?:\s*([a-zA-Z2-7 ]{16,})",
                modal_text,
                flags=re.IGNORECASE,
            )
            if key_hint:
                candidate = re.sub(r"[^A-Za-z2-7]", "", key_hint.group(1)).upper()
                if len(candidate) >= 16:
                    return candidate

        for selector in ["strong", "code", "div", "span"]:
            try:
                values = page.locator(selector).all_inner_texts()
            except Exception:
                values = []
            for text in values[:300]:
                clean = str(text or "").strip().upper()
                if not clean:
                    continue
                match = regex.search(clean)
                if not match:
                    continue
                secret = match.group(0).replace(" ", "").strip()
                if len(secret) >= 16:
                    return secret

        content = str(page.content() or "")
        uri_match = uri_regex.search(content)
        if uri_match:
            uri = uri_match.group(0)
            secret_match = re.search(r"[?&]secret=([A-Z2-7%]+)", uri, flags=re.IGNORECASE)
            if secret_match:
                raw = secret_match.group(1)
                decoded = unquote(raw).upper()
                secret = decoded.replace(" ", "").strip()
                if len(secret) >= 16:
                    return secret

        source_match = regex.search(content.upper())
        if source_match:
            secret = source_match.group(0).replace(" ", "").strip()
            if len(secret) >= 16:
                return secret
        return ""

    def _submit_totp_code(self, code: str) -> None:
        clean = str(code or "").strip()
        if not clean:
            raise RuntimeError("TOTP code is empty")

        field = self._find_any(
            [
                "input[name='totpPin']",
                "input[name='Pin']",
                "input#totpPin",
                "input[aria-label*='code']",
                "//input[contains(translate(@aria-label, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'code')]",
                "input[inputmode='numeric']",
                "input[type='tel'][autocomplete='one-time-code']",
                "input[type='tel']",
                "input[type='text']",
            ],
            timeout_ms=20_000,
        )
        field.fill("")
        field.type(clean)

        clicked = self._click_google_action(
            [
                "#totpNext",
                "//button[@type='submit']",
                "//input[@type='submit']",
                "//span[text()='Verify']",
                "//button//span[text()='Verify']",
                "//span[text()='Next']",
                "//button//span[text()='Next']",
                "//button[contains(.,'Continue')]",
                "//span[contains(text(),'Continue')]",
                "//button[contains(.,'Done')]",
                "//span[contains(text(),'Done')]",
            ],
            ["Verify", "Next", "Continue", "Done"],
            timeout_ms=12_000,
            optional=True,
        )
        if not clicked:
            try:
                field.press("Enter")
            except Exception:
                try:
                    self._require_page().keyboard.press("Enter")
                except Exception:
                    pass
        time.sleep(1.5)

    def _finalize_2sv_enablement(
        self,
        *,
        email: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        primary_enable_selectors = [
            "//a[@aria-label='Turn on 2-Step Verification']",
            "//button[@aria-label='Turn on 2-Step Verification']",
            "//span[normalize-space()='Turn on 2-Step Verification']",
            "//button//span[normalize-space()='Turn on 2-Step Verification']",
        ]
        secondary_confirm_selectors = [
            "//span[normalize-space()='Turn on']/ancestor::button[1]",
            "//span[normalize-space()='Turn on']/ancestor::a[1]",
            "//button[@aria-label='Turn on']",
            "//a[@aria-label='Turn on']",
            "//span[normalize-space()='Turn on']",
            "//div[@role='dialog']//span[normalize-space()='Turn on 2-Step Verification']",
            "//div[@role='dialog']//button[.//span[normalize-space()='Turn on 2-Step Verification']]",
            "//div[@role='dialog']//span[normalize-space()='Turn on']",
            "//div[@role='dialog']//button[.//span[normalize-space()='Turn on']]",
        ]
        candidates = [
            "https://myaccount.google.com/signinoptions/twosv",
            "https://myaccount.google.com/signinoptions/two-step-verification",
        ]

        for _ in range(3):
            for url in candidates:
                try:
                    self._goto(url)
                except Exception:
                    continue

                if email and password:
                    self._maybe_complete_reauth(email, password)
                self._accept_pending_terms_of_service_if_present()

                if self._is_2sv_already_enabled():
                    return

                # Step 1: explicit primary action on twosv page.
                clicked_primary = self._click_any(
                    primary_enable_selectors,
                    timeout_ms=4_000,
                    optional=True,
                )
                if clicked_primary:
                    time.sleep(0.8)
                # Step 2: explicit follow-up action on next page/dialog.
                clicked_secondary = self._click_any(
                    secondary_confirm_selectors,
                    timeout_ms=2_500,
                    optional=True,
                )
                if clicked_secondary:
                    time.sleep(0.8)

                if self._is_2sv_already_enabled():
                    return

            # If Google stays on authenticator details, click turn-on there as well.
            try:
                self._goto("https://myaccount.google.com/two-step-verification/authenticator")
                self._click_any(primary_enable_selectors, timeout_ms=2_500, optional=True)
                time.sleep(0.6)
                self._click_any(secondary_confirm_selectors, timeout_ms=2_500, optional=True)
                time.sleep(0.6)
            except Exception:
                pass

            if self._is_2sv_already_enabled():
                return

        debug = self._capture_debug_state("2sv_enablement_failed")
        raise RuntimeError(f"Google did not confirm 2-Step Verification enabled state. {debug}")

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _require_browser(self) -> Browser:
        if self.browser is None:
            raise RuntimeError("Playwright browser not initialized")
        return self.browser

    def _require_page(self) -> Page:
        if self.page is None:
            raise RuntimeError("Playwright page not initialized")
        return self.page

    def _safe_url(self) -> str:
        try:
            return str(self._require_page().url or "")
        except Exception:
            return ""

    def _capture_debug_state(self, tag: str) -> str:
        if not self.debug_enabled:
            return ""
        try:
            self.debug_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            slug = self._slug(tag)
            prefix = self.debug_dir / f"{stamp}_{slug}"
            page = self._require_page()
            screenshot_path = str(prefix) + ".png"
            html_path = str(prefix) + ".html"
            txt_path = str(prefix) + ".txt"
            page.screenshot(path=screenshot_path, full_page=True)
            Path(html_path).write_text(page.content(), encoding="utf-8")
            Path(txt_path).write_text(self._visible_text_excerpt(), encoding="utf-8")
            return f"url={self._safe_url()} screenshot={screenshot_path} html={html_path} text={txt_path}"
        except Exception as exc:
            return f"url={self._safe_url()} debug_capture_failed={exc}"

    def _visible_text_excerpt(self, limit: int = 1800) -> str:
        try:
            page = self._require_page()
            raw = page.locator("body").inner_text(timeout=4_000)
            clean = re.sub(r"\\s+", " ", str(raw or "")).strip()
            return clean[:limit]
        except Exception:
            return ""

    def _ensure_expected_google_profile(
        self,
        email: str,
        password: str,
        *,
        onepassword: Optional[OnePasswordCliClient],
        context: str,
    ) -> None:
        if not self.profile_verify_enabled:
            return

        expected = str(email or "").strip().lower()
        if not expected:
            return

        observed_emails: List[str] = []
        last_state = ""
        for attempt in range(1, self.profile_verify_attempts + 1):
            if attempt > 1:
                logger.warning(
                    "Resetting Google browser context after profile mismatch for %s during %s",
                    expected,
                    context,
                )
                self._reset_session()

            self._goto(f"https://myaccount.google.com/?authuser={quote(expected)}")
            time.sleep(1.0)

            if self._has_google_sign_in_prompt():
                self._complete_google_signin_flow(
                    email=expected,
                    password=password,
                    onepassword=onepassword,
                    context=f"profile_check_{context}",
                )
                self._drain_google_login_interstitials(expected, onepassword)
                self._goto(f"https://myaccount.google.com/?authuser={quote(expected)}")
                time.sleep(1.0)

            self._dismiss_sign_in_to_chrome_prompt_if_present()
            observed_emails = self._read_visible_google_profile_emails()
            if expected in observed_emails:
                logger.info("Verified Google browser profile %s during %s", expected, context)
                return

            self._open_google_account_menu()
            observed_emails = self._read_visible_google_profile_emails()
            if expected in observed_emails and len(observed_emails) == 1:
                logger.info("Verified Google browser profile %s from account menu during %s", expected, context)
                return

            last_state = ", ".join(observed_emails) if observed_emails else "no visible Google account email"

        debug = self._capture_debug_state(f"google_profile_check_failed_{context}_{self._slug(expected)}")
        raise RuntimeError(
            "Google profile check failed. "
            f"failing_step=profile_check; why=browser session is not visibly signed in as expected account; "
            f"current_state=expected {expected}, observed {last_state}; "
            "next_action=retry with a fresh browser session or manually inspect Google account chooser state. "
            f"{debug}"
        )

    def _open_google_account_menu(self) -> bool:
        return bool(
            self._click_any(
                [
                    "a[aria-label*='Google Account']",
                    "button[aria-label*='Google Account']",
                    "[aria-label*='Google Account']",
                    "a[href*='SignOutOptions']",
                    "a[href*='ManageAccount']",
                ],
                timeout_ms=3_000,
                optional=True,
            )
        )

    def _read_visible_google_profile_emails(self) -> List[str]:
        try:
            emails = self._require_page().evaluate(
                """
                () => {
                  const emailRegex = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}/ig;
                  const values = [];
                  const add = (value) => {
                    const text = String(value || '');
                    const matches = text.match(emailRegex) || [];
                    for (const match of matches) values.push(match.toLowerCase());
                  };
                  add(document.body ? document.body.innerText : '');
                  for (const el of Array.from(document.querySelectorAll('*'))) {
                    add(el.getAttribute('aria-label'));
                    add(el.getAttribute('title'));
                    add(el.getAttribute('data-email'));
                    add(el.getAttribute('data-identifier'));
                  }
                  return Array.from(new Set(values));
                }
                """
            )
            if not isinstance(emails, list):
                return []
            return sorted({str(email or "").strip().lower() for email in emails if str(email or "").strip()})
        except Exception:
            return []

    def _click_google_action(
        self,
        selectors: Iterable[str],
        text_variants: Iterable[str],
        *,
        timeout_ms: int = 20_000,
        optional: bool = False,
    ):
        try:
            return self._click_any(selectors, timeout_ms=timeout_ms, optional=False)
        except Exception as selector_exc:
            clicked = self._click_semantic_text(text_variants, timeout_ms=timeout_ms, optional=True)
            if clicked:
                return clicked
            if optional:
                return None
            raise selector_exc

    def _click_expected_google_account(self, email: str, *, timeout_ms: int = 3_000) -> bool:
        clean_email = str(email or "").strip().lower()
        if not clean_email:
            return False

        deadline = time.time() + max(0.5, float(timeout_ms) / 1000.0)
        while time.time() < deadline:
            try:
                clicked = self._require_page().evaluate(
                    """
                    (email) => {
                      const target = String(email || '').trim().toLowerCase();
                      if (!target) return false;
                      const clickableSelector = [
                        '[data-identifier]',
                        '[data-email]',
                        '[role="button"]',
                        'button',
                        'a'
                      ].join(',');
                      const isVisible = (el) => {
                        if (!el) return false;
                        const style = window.getComputedStyle(el);
                        const rect = el.getBoundingClientRect();
                        return style.visibility !== 'hidden'
                          && style.display !== 'none'
                          && rect.width > 0
                          && rect.height > 0;
                      };
                      const labelFor = (el) => [
                        el.getAttribute('data-identifier'),
                        el.getAttribute('data-email'),
                        el.getAttribute('aria-label'),
                        el.getAttribute('title'),
                        el.innerText,
                        el.textContent
                      ].filter(Boolean).join(' ').replace(/\\s+/g, ' ').trim().toLowerCase();
                      const candidates = Array.from(document.querySelectorAll(clickableSelector));
                      for (const el of candidates) {
                        if (!isVisible(el)) continue;
                        if (!labelFor(el).includes(target)) continue;
                        el.scrollIntoView({block: 'center', inline: 'center'});
                        el.click();
                        return true;
                      }
                      return false;
                    }
                    """,
                    clean_email,
                )
                if bool(clicked):
                    return True
            except Exception:
                pass
            time.sleep(0.2)
        return False

    def _click_semantic_text(
        self,
        text_variants: Iterable[str],
        *,
        timeout_ms: int = 20_000,
        optional: bool = False,
    ):
        variants = [str(text or "").strip().lower() for text in text_variants if str(text or "").strip()]
        if not variants:
            if optional:
                return None
            raise RuntimeError("No semantic text variants provided")

        deadline = time.time() + max(0.5, float(timeout_ms) / 1000.0)
        while time.time() < deadline:
            try:
                clicked = self._require_page().evaluate(
                    """
                    (variants) => {
                      const needles = (variants || [])
                        .map((value) => String(value || '').trim().toLowerCase())
                        .filter(Boolean);
                      if (!needles.length) return null;

                      const clickableSelector = [
                        'button',
                        '[role="button"]',
                        'a',
                        'input[type="button"]',
                        'input[type="submit"]',
                        '[data-identifier]',
                        '[aria-label]'
                      ].join(',');
                      const broadSelector = clickableSelector + ',span,div';
                      const isVisible = (el) => {
                        if (!el || el === document.body || el === document.documentElement) return false;
                        const style = window.getComputedStyle(el);
                        const rect = el.getBoundingClientRect();
                        return style.visibility !== 'hidden'
                          && style.display !== 'none'
                          && style.pointerEvents !== 'none'
                          && rect.width > 0
                          && rect.height > 0;
                      };
                      const textFor = (el) => [
                        el.getAttribute('aria-label'),
                        el.getAttribute('title'),
                        el.getAttribute('value'),
                        el.innerText,
                        el.textContent
                      ].filter(Boolean).join(' ').replace(/\\s+/g, ' ').trim();
                      const clickableFor = (el) => el.closest(clickableSelector) || el;
                      const candidates = Array.from(document.querySelectorAll(broadSelector));
                      for (const el of candidates) {
                        if (!isVisible(el)) continue;
                        const label = textFor(el);
                        if (!label || label.length > 180) continue;
                        const normalized = label.toLowerCase();
                        const matched = needles.some((needle) => normalized === needle || normalized.includes(needle));
                        if (!matched) continue;
                        const clickable = clickableFor(el);
                        if (!isVisible(clickable)) continue;
                        clickable.scrollIntoView({block: 'center', inline: 'center'});
                        clickable.click();
                        return label;
                      }
                      return null;
                    }
                    """,
                    variants,
                )
                if clicked:
                    return f"semantic_text={clicked}"
            except Exception:
                pass
            time.sleep(0.2)

        if optional:
            return None
        raise RuntimeError(f"Unable to click semantic action with text variants: {variants}")

    @staticmethod
    def _slug(value: str) -> str:
        text = str(value or "").strip().lower()
        safe = []
        for ch in text:
            if ch.isalnum():
                safe.append(ch)
            else:
                safe.append("_")
        return "".join(safe)[:80] or "debug"

    def _maybe_complete_totp_challenge(
        self,
        email: str,
        onepassword: Optional[OnePasswordCliClient],
    ) -> None:
        if not self._exists(
            [
                "input[name='totpPin']",
                "input#totpPin",
                "input[aria-label*='code']",
                "//input[contains(translate(@aria-label, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'code')]",
                "input[inputmode='numeric']",
                "input[type='tel'][autocomplete='one-time-code']",
                "input[type='tel']",
            ],
            timeout_ms=4_000,
        ):
            return
        try:
            clean_email = str(email or "").strip().lower()
            raw_secret = str(self._totp_secrets_by_email.get(clean_email) or "").strip()
            if raw_secret:
                code = self._generate_totp_from_secret(raw_secret)
                self._submit_totp_code(code)
                return
            if onepassword is None:
                return
            item_id = str(self._totp_item_ids_by_email.get(clean_email) or "").strip()
            if not item_id:
                item = onepassword.find_google_login_item(email)
                if not item:
                    return
                item_id = str(item.get("id") or "").strip()
            if not item_id:
                return
            code = onepassword.get_totp(item_id)
            self._submit_totp_code(code)
        except Exception as exc:
            logger.warning("Failed to complete existing TOTP challenge for %s: %s", email, exc)

    @staticmethod
    def _generate_totp_from_secret(raw_secret: str) -> str:
        try:
            import pyotp  # type: ignore
        except ImportError as exc:
            raise RuntimeError("pyotp is required to generate Google TOTP codes") from exc
        normalized = str(raw_secret or "").replace(" ", "").strip()
        if not normalized:
            raise RuntimeError("TOTP secret is empty")
        return pyotp.TOTP(normalized).now()

    def _goto(self, url: str) -> None:
        page = self._require_page()
        page.goto(url, wait_until="domcontentloaded")

    def _has_google_sign_in_prompt(self) -> bool:
        current_url = self._safe_url().lower()
        if "accounts.google.com" in current_url:
            # Do not treat OAuth consent pages as active sign-in prompts.
            # Use path-only matching so querystring `continue=.../consent...`
            # does not create false positives on password pages.
            path = current_url.split("?", 1)[0]
            if "/consent" in path:
                return False
            return True
        return self._exists(
            [
                "input#identifierId",
                "input[type='email']",
                "input[name='Passwd']",
                "//h1[contains(normalize-space(),'Sign in')]",
                "//*[contains(normalize-space(),'Use your Google Account')]",
                "//*[contains(normalize-space(),'Choose an account')]",
            ],
            timeout_ms=1_500,
        )

    def _reauthenticate_admin_console_if_needed(self) -> bool:
        if not self._admin_email or not self._admin_password:
            return False
        if not self._has_google_sign_in_prompt():
            return False

        logger.info(
            "Admin console page redirected to Google sign-in for %s; reauthenticating session",
            self._admin_email,
        )
        self._complete_google_signin_flow(
            email=self._admin_email,
            password=self._admin_password,
            onepassword=self._admin_onepassword,
            context="admin_console_reauth",
        )
        self._drain_google_login_interstitials(self._admin_email, self._admin_onepassword)
        return True

    def _open_admin_console_page(
        self,
        target_url: str,
        ready_selectors: List[str],
        *,
        allow_directory_fallback: bool = False,
    ) -> None:
        for attempt in range(1, 4):
            self._goto(target_url)
            time.sleep(1.0)
            self._dismiss_sign_in_to_chrome_prompt_if_present()
            self._dismiss_phone_prompt_if_present()
            self._accept_pending_terms_of_service_if_present()

            if self._reauthenticate_admin_console_if_needed():
                time.sleep(1.0)
                self._goto(target_url)
                time.sleep(1.0)
                self._dismiss_sign_in_to_chrome_prompt_if_present()
                self._dismiss_phone_prompt_if_present()
                self._accept_pending_terms_of_service_if_present()

            if self._exists(ready_selectors, timeout_ms=12_000):
                return

            current_url = self._safe_url().lower()
            if allow_directory_fallback and "accounts.google.com" not in current_url:
                directory_loaded = self._exists(
                    [
                        "//span[contains(text(),'Directory')]",
                        "//span[contains(text(),'Admin')]",
                        "//a[contains(@href,'/ac/owl')]",
                    ],
                    timeout_ms=4_000,
                )
                if directory_loaded:
                    return

            if attempt < 3:
                time.sleep(float(attempt))

        snippet = self._visible_text_excerpt(800)
        debug = self._capture_debug_state("open_admin_console_page_failed")
        extras = []
        if snippet:
            extras.append(f"text={snippet}")
        if debug:
            extras.append(debug)
        extra_text = f" | {' | '.join(extras)}" if extras else ""
        raise RuntimeError(f"Google Admin page did not load. url={self._safe_url()}{extra_text}")

    def _open_users_page(self) -> None:
        self._open_admin_console_page(
            "https://admin.google.com/ac/users",
            [
                "//span[contains(text(),'Users')]",
                "//*[contains(text(),'Users') or @aria-label='Users']",
                "//a[contains(@href,'/ac/users')]",
            ],
            allow_directory_fallback=True,
        )

    def _open_user(self, email: str) -> None:
        self._open_users_page()

        clicked = self._click_any(
            [
                f"//div[text()='{email}']/../../../td[2]//a",
                f"//a[contains(., '{email}')]",
                f"text={email}",
            ],
            timeout_ms=self.timeout_ms,
            optional=True,
        )
        if clicked:
            time.sleep(1.2)
            return

        if self._fill_any(
            [
                "input[type='search']",
                "input[aria-label*='Search']",
            ],
            email,
            timeout_ms=5_000,
            optional=True,
        ):
            page = self._require_page()
            page.keyboard.press("Enter")
            self._click_any([f"//a[contains(., '{email}')]", f"text={email}"], timeout_ms=10_000)
            time.sleep(1.2)
            return

        raise RuntimeError(f"Unable to open user profile for {email}")

    def _open_user_with_refresh(self, email: str, attempts: int = 3) -> None:
        target = str(email or "").strip().lower()
        if not target:
            raise RuntimeError("email is required")
        last_error: Optional[Exception] = None
        for attempt in range(1, max(1, attempts) + 1):
            try:
                self._open_user(target)
                return
            except Exception as exc:
                last_error = exc
                if attempt >= attempts:
                    break
                try:
                    self._open_users_page()
                    self._require_page().reload(wait_until="domcontentloaded")
                except Exception:
                    pass
                time.sleep(min(3.0, 0.8 * attempt))
        raise RuntimeError(f"Unable to open user profile for {target} after {attempts} attempts ({last_error})")

    def _reset_password(self, email: str, password: str) -> None:
        self._open_user(email)
        self._click_any(
            [
                "//div[@role='button' and @aria-label='Reset password']",
                "//button[contains(@aria-label,'Reset password')]",
                "//span[contains(text(),'Reset password')]",
            ]
        )
        self._click_any(["//label[contains(text(), 'Create password')]"], timeout_ms=10_000, optional=True)
        self._fill_any(["//input[@type='password' and contains(@aria-label,'Enter password')]"], password)
        self._click_any(["//span[text()='Ask user to change their password when they sign in']"], timeout_ms=8_000, optional=True)
        self._click_any(
            [
                "//div[@role='button' and .//span[text()='Reset']]",
                "//button[.//span[text()='Reset']]",
            ],
            timeout_ms=10_000,
            optional=True,
        )

    def _selector(self, locator: str) -> str:
        text = str(locator or "").strip()
        if text.startswith("//"):
            return f"xpath={text}"
        if text.startswith("("):
            return f"xpath={text}"
        return text

    def _find_any(self, selectors: Iterable[str], timeout_ms: int = 20_000):
        page = self._require_page()
        options = list(selectors)
        if not options:
            raise RuntimeError("No selectors provided")
        last_error: Optional[Exception] = None
        per_selector_timeout = max(200, int(timeout_ms / max(1, len(options))))
        for selector in options:
            normalized = self._selector(selector)
            locator = page.locator(normalized)
            try:
                locator.first.wait_for(state="attached", timeout=per_selector_timeout)
                count = min(locator.count(), 12)
                if count <= 0:
                    count = 1
                for idx in range(count):
                    candidate = locator.nth(idx)
                    try:
                        candidate.wait_for(state="visible", timeout=min(per_selector_timeout, 1_500))
                        return candidate
                    except Exception as cand_exc:
                        last_error = cand_exc
            except Exception as exc:
                last_error = exc
        raise RuntimeError(f"None of the selectors matched: {options} ({last_error})")

    def _click_any(
        self,
        selectors: Iterable[str],
        timeout_ms: int = 20_000,
        optional: bool = False,
    ):
        page = self._require_page()
        options = list(selectors)
        if not options:
            if optional:
                return None
            raise RuntimeError("No selectors provided")

        # Build a combined locator that watches ALL selectors in parallel,
        # giving the FULL timeout to the combined wait instead of splitting
        # it across selectors sequentially.
        combined = page.locator(self._selector(options[0]))
        for sel in options[1:]:
            combined = combined.or_(page.locator(self._selector(sel)))

        # Phase 1: Wait for ANY selector to appear (full timeout)
        try:
            combined.first.wait_for(
                state="attached",
                timeout=min(timeout_ms, 1_200) if optional else timeout_ms,
            )
        except Exception as exc:
            if optional:
                return None
            raise RuntimeError(f"Unable to click any selector {options} ({exc})")

        # Phase 2: Now that something is rendered, try each selector
        # individually with a short timeout to find the clickable one.
        last_error: Optional[Exception] = None
        per_selector_timeout = max(3_000, int(timeout_ms / max(1, len(options))))
        for selector in options:
            normalized = self._selector(selector)
            locator = page.locator(normalized)
            try:
                locator.first.wait_for(state="attached", timeout=per_selector_timeout)
                count = min(locator.count(), 12)
                if count <= 0:
                    count = 1
                for idx in range(count):
                    candidate = locator.nth(idx)
                    try:
                        candidate.wait_for(state="visible", timeout=min(per_selector_timeout, 4_000))
                        candidate.scroll_into_view_if_needed(timeout=1_000)
                        candidate.click(timeout=per_selector_timeout)
                        return f"{normalized}[{idx}]"
                    except Exception as cand_exc:
                        if not optional:
                            try:
                                candidate.click(timeout=min(timeout_ms, 2_500), force=True)
                                return f"{normalized}[{idx}]"
                            except Exception:
                                pass
                        last_error = cand_exc
            except Exception as exc:
                last_error = exc
        if optional:
            return None
        raise RuntimeError(f"Unable to click any selector {options} ({last_error})")

    def _enabled_locator_any(self, selectors: Iterable[str], timeout_ms: int = 20_000):
        page = self._require_page()
        options = list(selectors)
        deadline = time.time() + max(0.5, float(timeout_ms) / 1000.0)
        last_error: Optional[Exception] = None
        while time.time() < deadline:
            remaining_ms = max(250, int((deadline - time.time()) * 1000))
            for selector in options:
                normalized = self._selector(selector)
                locator = page.locator(normalized)
                try:
                    locator.first.wait_for(state="attached", timeout=min(remaining_ms, 1_000))
                    count = min(locator.count(), 12)
                    if count <= 0:
                        count = 1
                    for idx in range(count):
                        candidate = locator.nth(idx)
                        try:
                            candidate.wait_for(state="visible", timeout=min(remaining_ms, 800))
                            aria_disabled = str(candidate.get_attribute("aria-disabled") or "").strip().lower()
                            disabled_attr = candidate.get_attribute("disabled")
                            if disabled_attr is not None or aria_disabled == "true":
                                continue
                            if not candidate.is_enabled():
                                continue
                            return candidate
                        except Exception as cand_exc:
                            last_error = cand_exc
                except Exception as exc:
                    last_error = exc
            time.sleep(0.25)
        raise RuntimeError(f"None of the selectors became enabled: {options} ({last_error})")

    def _click_enabled_any(
        self,
        selectors: Iterable[str],
        timeout_ms: int = 20_000,
        optional: bool = False,
    ):
        options = list(selectors)
        try:
            locator = self._enabled_locator_any(options, timeout_ms=timeout_ms)
            locator.scroll_into_view_if_needed(timeout=1_000)
            locator.click(timeout=min(timeout_ms, 3_000))
            return "enabled"
        except Exception as exc:
            if optional:
                return None
            raise RuntimeError(f"Unable to click enabled selector {options} ({exc})") from exc

    def _select_google_wizard_radio(
        self,
        selectors: Iterable[str],
        checked_selectors: Iterable[str],
        timeout_ms: int = 12_000,
    ) -> None:
        deadline = time.time() + max(0.5, float(timeout_ms) / 1000.0)
        last_error: Optional[Exception] = None
        while time.time() < deadline:
            if self._exists(checked_selectors, timeout_ms=500):
                return

            try:
                self._click_any(selectors, timeout_ms=1_500, optional=False)
            except Exception as exc:
                last_error = exc

            if self._exists(checked_selectors, timeout_ms=800):
                return

            try:
                locator = self._find_any(selectors, timeout_ms=1_000)
                locator.evaluate(
                    """(element) => {
                        element.click();
                        element.dispatchEvent(new MouseEvent('click', { bubbles: true }));
                    }"""
                )
            except Exception as exc:
                last_error = exc

            if self._exists(checked_selectors, timeout_ms=800):
                return
            time.sleep(0.25)

        raise RuntimeError(f"Unable to select wizard radio {list(selectors)} ({last_error})")

    def _fill_any(
        self,
        selectors: Iterable[str],
        value: str,
        timeout_ms: int = 20_000,
        optional: bool = False,
    ) -> bool:
        page = self._require_page()
        options = list(selectors)
        if not options:
            if optional:
                return False
            raise RuntimeError("No selectors provided")
        last_error: Optional[Exception] = None
        per_selector_timeout = max(250, int(timeout_ms / max(1, len(options))))
        for selector in options:
            normalized = self._selector(selector)
            locator = page.locator(normalized)
            try:
                try:
                    locator.first.wait_for(state="attached", timeout=min(per_selector_timeout, 8_000))
                except Exception as attach_exc:
                    last_error = attach_exc

                count = min(locator.count(), 12)
                if count <= 0:
                    continue

                for idx in range(count):
                    candidate = locator.nth(idx)
                    try:
                        visible = False
                        try:
                            candidate.wait_for(state="visible", timeout=min(per_selector_timeout, 8_000))
                            visible = True
                        except Exception:
                            try:
                                visible = bool(candidate.is_visible())
                            except Exception:
                                visible = False
                        if not visible:
                            continue

                        candidate.scroll_into_view_if_needed(timeout=1_000)
                        filled = False
                        try:
                            candidate.fill("")
                            candidate.type(str(value or ""))
                            filled = True
                        except Exception:
                            try:
                                candidate.click(timeout=min(per_selector_timeout, 1_500))
                            except Exception:
                                try:
                                    candidate.click(timeout=min(per_selector_timeout, 1_500), force=True)
                                except Exception:
                                    pass
                            try:
                                candidate.fill("")
                                candidate.type(str(value or ""))
                                filled = True
                            except Exception:
                                pass
                        if not filled:
                            candidate.evaluate(
                                """([el, nextValue]) => {
                                    el.focus();
                                    el.value = '';
                                    el.dispatchEvent(new Event('input', { bubbles: true }));
                                    el.value = nextValue;
                                    el.dispatchEvent(new Event('input', { bubbles: true }));
                                    el.dispatchEvent(new Event('change', { bubbles: true }));
                                }""",
                                [str(value or "")],
                            )
                        return True
                    except Exception as cand_exc:
                        last_error = cand_exc
            except Exception as exc:
                last_error = exc
        if optional:
            return False
        raise RuntimeError(f"Unable to fill any selector {options} ({last_error})")

    def _inner_text_any(
        self,
        selectors: Iterable[str],
        timeout_ms: int = 20_000,
        optional: bool = False,
    ) -> str:
        try:
            locator = self._find_any(selectors, timeout_ms=timeout_ms)
            return str(locator.inner_text(timeout=timeout_ms) or "").strip()
        except Exception:
            if optional:
                return ""
            raise

    def _exists(self, selectors: Iterable[str], timeout_ms: int = 2_000) -> bool:
        options = list(selectors)
        if not options:
            return False
        per_selector_timeout = max(150, int(timeout_ms / max(1, len(options))))
        for selector in options:
            normalized = self._selector(selector)
            locator = self._require_page().locator(normalized)
            try:
                locator.first.wait_for(state="attached", timeout=per_selector_timeout)
                count = min(locator.count(), 12)
                if count <= 0:
                    count = 1
                for idx in range(count):
                    candidate = locator.nth(idx)
                    try:
                        candidate.wait_for(state="visible", timeout=min(per_selector_timeout, 4_000))
                        return True
                    except Exception:
                        continue
            except Exception:
                continue
        return False

    def _download_profile_image(self, url: str) -> Optional[str]:
        clean = str(url or "").strip()
        if not clean or not clean.lower().startswith(("http://", "https://")):
            return None

        if "drive.google.com" in clean and "/file/d/" in clean:
            file_id = clean.split("/file/d/")[1].split("/")[0]
            clean = f"https://drive.google.com/uc?export=download&id={file_id}"

        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(clean, headers=headers, timeout=30)
        response.raise_for_status()

        suffix = ".jpg"
        content_type = str(response.headers.get("Content-Type") or "").lower()
        if "png" in content_type:
            suffix = ".png"
        elif "webp" in content_type:
            suffix = ".webp"

        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(response.content)
            return tmp.name
