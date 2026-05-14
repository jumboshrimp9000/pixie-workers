import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import requests


logger = logging.getLogger(__name__)


class SendingToolUploader:
    def __init__(self, timeout_seconds: int = 25):
        self.timeout_seconds = timeout_seconds
        self.playwright_browser_channel = str(os.getenv("GOOGLE_PLAYWRIGHT_CHANNEL", "") or "").strip()
        self.playwright_browser_executable_path = (
            str(os.getenv("GOOGLE_PLAYWRIGHT_CHROME_PATH", "") or "").strip()
            or str(os.getenv("GOOGLE_SELENIUM_CHROME_BINARY", "") or "").strip()
        )
        self.playwright_slow_mo_ms = max(0, int(os.getenv("GOOGLE_PLAYWRIGHT_SLOW_MO_MS", "0")))

    def _launch_playwright_browser(self, playwright: Any, *, headless: bool):
        launch_kwargs: Dict[str, Any] = {"headless": headless}
        if self.playwright_slow_mo_ms:
            launch_kwargs["slow_mo"] = self.playwright_slow_mo_ms
        if self.playwright_browser_executable_path:
            launch_kwargs["executable_path"] = self.playwright_browser_executable_path
        elif self.playwright_browser_channel:
            launch_kwargs["channel"] = self.playwright_browser_channel

        logger.info(
            "[SendingToolUploader] Launching Playwright browser (headless=%s, channel=%s, executable_path_set=%s)",
            headless,
            self.playwright_browser_channel or "chromium-default",
            bool(self.playwright_browser_executable_path),
        )
        return playwright.chromium.launch(**launch_kwargs)

    def upload_and_validate(
        self,
        *,
        tool: str,
        api_key: str,
        inboxes: List[Dict[str, Any]],
        provider: str = "",
        credential: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None,
        onepassword: Any = None,
        headless: Optional[bool] = None,
        use_playwright_oauth: bool = True,
    ) -> Dict[str, Any]:
        normalized_tool = self._normalize_tool(tool)
        provider_name = str(provider or "").strip().lower()
        cred = credential or {}
        use_oauth_browser = (
            use_playwright_oauth
            and provider_name == "google"
            and normalized_tool in {"instantly.ai", "smartlead.ai", "amplemarket", "email-bison"}
        )
        resolved_headless = self._as_bool(
            os.getenv("GOOGLE_PLAYWRIGHT_HEADLESS", "true"),
            default=True,
        ) if headless is None else bool(headless)

        if normalized_tool == "instantly.ai":
            if use_oauth_browser:
                result = self._upload_instantly_via_oauth(
                    api_key=api_key,
                    inboxes=inboxes,
                    onepassword=onepassword,
                    headless=resolved_headless,
                )
            else:
                result = self._upload_instantly_api_only(api_key=api_key, inboxes=inboxes)
            return self._apply_instantly_settings(
                api_key=api_key,
                result=result,
                settings=self._normalize_settings(settings),
            )

        if normalized_tool == "smartlead.ai":
            if use_oauth_browser:
                result = self._upload_smartlead_via_oauth(
                    api_key=api_key,
                    smartlead_username=str(cred.get("username") or "").strip(),
                    smartlead_password=str(cred.get("password") or "").strip(),
                    inboxes=inboxes,
                    onepassword=onepassword,
                    headless=resolved_headless,
                )
            else:
                result = self._upload_smartlead_api_only(
                    api_key=api_key,
                    smartlead_username=str(cred.get("username") or "").strip(),
                    smartlead_password=str(cred.get("password") or "").strip(),
                    inboxes=inboxes,
                )
            return self._apply_smartlead_settings(
                api_key=api_key,
                result=result,
                settings=self._normalize_settings(settings),
                credential=cred,
            )

        if normalized_tool == "amplemarket":
            return self._upload_amplemarket(
                api_key=api_key,
                amplemarket_username=str(cred.get("username") or "").strip(),
                amplemarket_password=str(cred.get("password") or "").strip(),
                inboxes=inboxes,
                provider=provider_name,
                settings=self._normalize_settings(settings),
                onepassword=onepassword,
                headless=resolved_headless,
                use_browser=use_playwright_oauth,
            )

        if normalized_tool == "email-bison":
            if provider_name == "google" and use_playwright_oauth:
                return self._upload_email_bison_google_via_oauth(
                    credential=cred,
                    inboxes=inboxes,
                    onepassword=onepassword,
                    headless=resolved_headless,
                    api_key=api_key,
                    settings=self._normalize_settings(settings),
                )
            return {
                "tool": "email-bison",
                "total_candidates": len(inboxes),
                "uploaded_emails": [],
                "failed_uploads": [
                    {
                        "email": str(row.get("email") or "").strip().lower(),
                        "error": "Email Bison automated upload currently supports Google OAuth inboxes only",
                    }
                    for row in inboxes
                    if str(row.get("email") or "").strip()
                ],
                "skipped_already_uploaded": 0,
            }

        return {
            "tool": normalized_tool or str(tool or ""),
            "total_candidates": len(inboxes),
            "uploaded_emails": [],
            "failed_uploads": [
                {
                    "email": str(row.get("email") or "").strip().lower(),
                    "error": f"Automated upload not supported for tool '{tool}'",
                }
                for row in inboxes
                if str(row.get("email") or "").strip()
            ],
            "skipped_already_uploaded": 0,
        }

    def _upload_instantly_via_oauth(
        self,
        *,
        api_key: str,
        inboxes: List[Dict[str, Any]],
        onepassword: Any,
        headless: bool,
    ) -> Dict[str, Any]:
        targets = [self._normalize_email(row.get("email")) for row in inboxes]
        targets = [email for email in targets if email]
        target_set = set(targets)
        provisional_errors: Dict[str, str] = {}

        if not target_set:
            return {
                "tool": "instantly.ai",
                "total_candidates": 0,
                "uploaded_emails": [],
                "failed_uploads": [],
                "skipped_already_uploaded": 0,
            }

        logger.info(
            "[SendingToolUploader:Instantly] Using Playwright OAuth upload flow for %s inbox(es) (headless=%s)",
            len(target_set),
            headless,
        )
        logger.info(
            "[SendingToolUploader:Instantly] OAuth upload uses isolated Playwright contexts per inbox."
        )

        browser = None
        playwright = None
        try:
            from playwright.sync_api import sync_playwright

            playwright = sync_playwright().start()
            browser = self._launch_playwright_browser(playwright, headless=headless)

            total_inboxes = len(inboxes)
            for index, inbox in enumerate(inboxes, start=1):
                email = self._normalize_email(inbox.get("email"))
                password = str(inbox.get("password") or "").strip()
                if not email:
                    continue
                if not password:
                    provisional_errors[email] = "Missing inbox password"
                    continue
                context = None
                page = None
                started_at = time.time()
                logger.info(
                    "[SendingToolUploader:Instantly] [%s/%s] Starting OAuth for %s",
                    index,
                    total_inboxes,
                    email,
                )
                try:
                    context, page = self._create_oauth_context_page(browser)
                    auth_url, session_id = self._instantly_init_oauth_session(api_key)
                    page.goto(auth_url, wait_until="domcontentloaded")
                    self._complete_google_signin_and_consent(
                        page=page,
                        context=context,
                        email=email,
                        password=password,
                        onepassword=onepassword,
                    )
                    ok, status_error = self._poll_instantly_oauth_status(api_key, session_id)
                    if not ok:
                        provisional_errors[email] = status_error or "Instantly OAuth session failed"
                        logger.warning(
                            "[SendingToolUploader:Instantly] [%s/%s] OAuth status failed for %s (%.1fs): %s",
                            index,
                            total_inboxes,
                            email,
                            time.time() - started_at,
                            provisional_errors[email],
                        )
                    else:
                        logger.info(
                            "[SendingToolUploader:Instantly] [%s/%s] OAuth accepted for %s (%.1fs)",
                            index,
                            total_inboxes,
                            email,
                            time.time() - started_at,
                        )
                except Exception as exc:
                    provisional_errors[email] = str(exc)
                    logger.warning(
                        "[SendingToolUploader:Instantly] [%s/%s] OAuth failed for %s (%.1fs): %s",
                        index,
                        total_inboxes,
                        email,
                        time.time() - started_at,
                        exc,
                    )
                finally:
                    if context is not None and page is not None:
                        self._close_non_primary_pages(context, page)
                    self._close_oauth_context_page(context=context, page=page)
        except Exception as exc:
            message = f"Playwright Instantly OAuth upload failed: {exc}"
            logger.warning("[SendingToolUploader:Instantly] %s", message)
            for email in target_set:
                provisional_errors.setdefault(email, message)
        finally:
            if browser is not None:
                try:
                    browser.close()
                except Exception:
                    pass
            if playwright is not None:
                try:
                    playwright.stop()
                except Exception:
                    pass

        attempts = max(1, int(os.getenv("INSTANTLY_VALIDATION_ATTEMPTS", "8")))
        interval_seconds = max(1.0, float(os.getenv("INSTANTLY_VALIDATION_INTERVAL_MS", "5000")) / 1000.0)
        concurrency = max(1, int(os.getenv("INSTANTLY_VALIDATION_CONCURRENCY", "10")))
        present = self._wait_for_account_presence(
            targets=target_set,
            attempts=attempts,
            interval_seconds=interval_seconds,
            checker=lambda email: self._check_instantly_account(api_key=api_key, email=email),
            concurrency=concurrency,
            tool_label="Instantly",
        )
        return self._build_result(
            tool="instantly.ai",
            inboxes=inboxes,
            present=present,
            provisional_errors=provisional_errors,
            missing_error_prefix="Instantly account not found after validation",
            total_candidates=len(target_set),
        )

    def _upload_smartlead_via_oauth(
        self,
        *,
        api_key: str,
        smartlead_username: str,
        smartlead_password: str,
        inboxes: List[Dict[str, Any]],
        onepassword: Any,
        headless: bool,
    ) -> Dict[str, Any]:
        targets = [self._normalize_email(row.get("email")) for row in inboxes]
        targets = [email for email in targets if email]
        target_set = set(targets)
        provisional_errors: Dict[str, str] = {}

        if not target_set:
            return {
                "tool": "smartlead.ai",
                "total_candidates": 0,
                "uploaded_emails": [],
                "failed_uploads": [],
                "skipped_already_uploaded": 0,
            }
        if not smartlead_username or not smartlead_password:
            return {
                "tool": "smartlead.ai",
                "total_candidates": len(target_set),
                "uploaded_emails": [],
                "failed_uploads": [
                    {
                        "email": email,
                        "error": "Missing Smartlead username/password required for Playwright OAuth upload",
                    }
                    for email in sorted(target_set)
                ],
                "skipped_already_uploaded": 0,
            }

        logger.info(
            "[SendingToolUploader:Smartlead] Using Playwright OAuth upload flow for %s inbox(es) (headless=%s)",
            len(target_set),
            headless,
        )
        logger.info(
            "[SendingToolUploader:Smartlead] OAuth upload uses isolated Playwright contexts per inbox."
        )
        session_mode = str(
            os.getenv("SMARTLEAD_GOOGLE_OAUTH_SESSION_MODE", "per_inbox_login") or "per_inbox_login"
        ).strip().lower()
        if session_mode in {"single_login_isolated_context", "single_login_popup_isolation"}:
            logger.info(
                "[SendingToolUploader:Smartlead] Using single-login mode with isolated OAuth contexts (mode=%s).",
                session_mode,
            )
            return self._upload_smartlead_via_oauth_single_login_isolated(
                api_key=api_key,
                smartlead_username=smartlead_username,
                smartlead_password=smartlead_password,
                inboxes=inboxes,
                onepassword=onepassword,
                headless=headless,
            )

        browser = None
        playwright = None
        try:
            from playwright.sync_api import sync_playwright

            playwright = sync_playwright().start()
            browser = self._launch_playwright_browser(playwright, headless=headless)

            total_inboxes = len(inboxes)
            for index, inbox in enumerate(inboxes, start=1):
                email = self._normalize_email(inbox.get("email"))
                password = str(inbox.get("password") or "").strip()
                if not email:
                    continue
                if not password:
                    provisional_errors[email] = "Missing inbox password"
                    continue

                context = None
                page = None
                started_at = time.time()
                logger.info(
                    "[SendingToolUploader:Smartlead] [%s/%s] Starting OAuth for %s",
                    index,
                    total_inboxes,
                    email,
                )
                try:
                    context, page = self._create_oauth_context_page(browser)
                    self._smartlead_login(page, smartlead_username, smartlead_password)
                    oauth_page, already_exists = self._start_smartlead_google_oauth_for_user(
                        page=page,
                        context=context,
                        email=email,
                    )
                    if already_exists:
                        logger.info(
                            "[SendingToolUploader:Smartlead] [%s/%s] Already connected: %s",
                            index,
                            total_inboxes,
                            email,
                        )
                        continue
                    self._complete_google_signin_and_consent(
                        page=oauth_page,
                        context=context,
                        email=email,
                        password=password,
                        onepassword=onepassword,
                    )
                    self._finalize_smartlead_connection(page=page, context=context, oauth_page=oauth_page)
                    logger.info(
                        "[SendingToolUploader:Smartlead] [%s/%s] OAuth complete for %s (%.1fs)",
                        index,
                        total_inboxes,
                        email,
                        time.time() - started_at,
                    )
                except Exception as exc:
                    provisional_errors[email] = str(exc)
                    logger.warning(
                        "[SendingToolUploader:Smartlead] [%s/%s] OAuth failed for %s (%.1fs): %s",
                        index,
                        total_inboxes,
                        email,
                        time.time() - started_at,
                        exc,
                    )
                finally:
                    if context is not None and page is not None:
                        self._close_non_primary_pages(context, page)
                    self._close_oauth_context_page(context=context, page=page)
        except Exception as exc:
            message = f"Playwright Smartlead OAuth upload failed: {exc}"
            logger.warning("[SendingToolUploader:Smartlead] %s", message)
            for email in target_set:
                provisional_errors.setdefault(email, message)
        finally:
            if browser is not None:
                try:
                    browser.close()
                except Exception:
                    pass
            if playwright is not None:
                try:
                    playwright.stop()
                except Exception:
                    pass

        attempts = max(1, int(os.getenv("SMARTLEAD_VALIDATION_ATTEMPTS", "8")))
        interval_seconds = max(1.0, float(os.getenv("SMARTLEAD_VALIDATION_INTERVAL_MS", "5000")) / 1000.0)
        concurrency = max(1, int(os.getenv("SMARTLEAD_VALIDATION_CONCURRENCY", "6")))
        present = self._wait_for_smartlead_accounts(
            targets=target_set,
            api_key=api_key,
            smartlead_username=smartlead_username,
            smartlead_password=smartlead_password,
            attempts=attempts,
            interval_seconds=interval_seconds,
            concurrency=concurrency,
        )
        return self._build_result(
            tool="smartlead.ai",
            inboxes=inboxes,
            present=present,
            provisional_errors=provisional_errors,
            missing_error_prefix="Smartlead account not found after validation",
            total_candidates=len(target_set),
        )

    def _upload_smartlead_via_oauth_single_login_isolated(
        self,
        *,
        api_key: str,
        smartlead_username: str,
        smartlead_password: str,
        inboxes: List[Dict[str, Any]],
        onepassword: Any,
        headless: bool,
    ) -> Dict[str, Any]:
        targets = [self._normalize_email(row.get("email")) for row in inboxes]
        targets = [email for email in targets if email]
        target_set = set(targets)
        provisional_errors: Dict[str, str] = {}

        if not target_set:
            return {
                "tool": "smartlead.ai",
                "total_candidates": 0,
                "uploaded_emails": [],
                "failed_uploads": [],
                "skipped_already_uploaded": 0,
            }

        browser = None
        context = None
        page = None
        playwright = None
        try:
            from playwright.sync_api import sync_playwright

            playwright = sync_playwright().start()
            browser = self._launch_playwright_browser(playwright, headless=headless)
            context, page = self._create_oauth_context_page(browser)
            self._smartlead_login(page, smartlead_username, smartlead_password)

            total_inboxes = len(inboxes)
            for index, inbox in enumerate(inboxes, start=1):
                email = self._normalize_email(inbox.get("email"))
                password = str(inbox.get("password") or "").strip()
                if not email:
                    continue
                if not password:
                    provisional_errors[email] = "Missing inbox password"
                    continue

                started_at = time.time()
                logger.info(
                    "[SendingToolUploader:Smartlead] [%s/%s] Starting OAuth for %s (single-login mode)",
                    index,
                    total_inboxes,
                    email,
                )
                try:
                    # Keep Smartlead login session, but clear Google state before each OAuth run.
                    self._clear_google_session_state(context)
                    oauth_page, already_exists = self._start_smartlead_google_oauth_for_user(
                        page=page,
                        context=context,
                        email=email,
                    )
                    if already_exists:
                        logger.info(
                            "[SendingToolUploader:Smartlead] [%s/%s] Already connected: %s",
                            index,
                            total_inboxes,
                            email,
                        )
                        continue
                    self._complete_google_signin_and_consent(
                        page=oauth_page,
                        context=context,
                        email=email,
                        password=password,
                        onepassword=onepassword,
                    )
                    self._finalize_smartlead_connection(page=page, context=context, oauth_page=oauth_page)
                    logger.info(
                        "[SendingToolUploader:Smartlead] [%s/%s] OAuth complete for %s (%.1fs)",
                        index,
                        total_inboxes,
                        email,
                        time.time() - started_at,
                    )
                except Exception as exc:
                    provisional_errors[email] = str(exc)
                    logger.warning(
                        "[SendingToolUploader:Smartlead] [%s/%s] OAuth failed for %s (%.1fs): %s",
                        index,
                        total_inboxes,
                        email,
                        time.time() - started_at,
                        exc,
                    )
                finally:
                    self._close_non_primary_pages(context, page)
        except Exception as exc:
            message = f"Playwright Smartlead OAuth upload (single-login isolated mode) failed: {exc}"
            logger.warning("[SendingToolUploader:Smartlead] %s", message)
            for email in target_set:
                provisional_errors.setdefault(email, message)
        finally:
            self._close_oauth_context_page(context=context, page=page)
            if browser is not None:
                try:
                    browser.close()
                except Exception:
                    pass
            if playwright is not None:
                try:
                    playwright.stop()
                except Exception:
                    pass

        attempts = max(1, int(os.getenv("SMARTLEAD_VALIDATION_ATTEMPTS", "8")))
        interval_seconds = max(1.0, float(os.getenv("SMARTLEAD_VALIDATION_INTERVAL_MS", "5000")) / 1000.0)
        concurrency = max(1, int(os.getenv("SMARTLEAD_VALIDATION_CONCURRENCY", "6")))
        present = self._wait_for_smartlead_accounts(
            targets=target_set,
            api_key=api_key,
            smartlead_username=smartlead_username,
            smartlead_password=smartlead_password,
            attempts=attempts,
            interval_seconds=interval_seconds,
            concurrency=concurrency,
        )
        return self._build_result(
            tool="smartlead.ai",
            inboxes=inboxes,
            present=present,
            provisional_errors=provisional_errors,
            missing_error_prefix="Smartlead account not found after validation",
            total_candidates=len(target_set),
        )

    def _upload_instantly_api_only(self, *, api_key: str, inboxes: List[Dict[str, Any]]) -> Dict[str, Any]:
        targets = [self._normalize_email(row.get("email")) for row in inboxes]
        targets = [email for email in targets if email]
        target_set = set(targets)
        provisional_errors: Dict[str, str] = {}
        logger.info(
            "[SendingToolUploader:Instantly] Validation-only mode (SMTP/IMAP upload removed)."
        )

        attempts = max(1, int(os.getenv("INSTANTLY_VALIDATION_ATTEMPTS", "8")))
        interval_seconds = max(1.0, float(os.getenv("INSTANTLY_VALIDATION_INTERVAL_MS", "5000")) / 1000.0)
        concurrency = max(1, int(os.getenv("INSTANTLY_VALIDATION_CONCURRENCY", "10")))
        present = self._wait_for_account_presence(
            targets=target_set,
            attempts=attempts,
            interval_seconds=interval_seconds,
            checker=lambda email: self._check_instantly_account(api_key=api_key, email=email),
            concurrency=concurrency,
            tool_label="Instantly",
        )
        return self._build_result(
            tool="instantly.ai",
            inboxes=inboxes,
            present=present,
            provisional_errors=provisional_errors,
            missing_error_prefix="Instantly account not found after validation",
            total_candidates=len(target_set),
        )

    def _upload_smartlead_api_only(
        self,
        *,
        api_key: str,
        smartlead_username: str,
        smartlead_password: str,
        inboxes: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        targets = [self._normalize_email(row.get("email")) for row in inboxes]
        targets = [email for email in targets if email]
        target_set = set(targets)
        provisional_errors: Dict[str, str] = {}
        logger.info(
            "[SendingToolUploader:Smartlead] Validation-only mode (SMTP/IMAP upload removed)."
        )

        attempts = max(1, int(os.getenv("SMARTLEAD_VALIDATION_ATTEMPTS", "8")))
        interval_seconds = max(1.0, float(os.getenv("SMARTLEAD_VALIDATION_INTERVAL_MS", "5000")) / 1000.0)
        concurrency = max(1, int(os.getenv("SMARTLEAD_VALIDATION_CONCURRENCY", "6")))
        present = self._wait_for_smartlead_accounts(
            targets=target_set,
            api_key=api_key,
            smartlead_username=smartlead_username,
            smartlead_password=smartlead_password,
            attempts=attempts,
            interval_seconds=interval_seconds,
            concurrency=concurrency,
        )
        return self._build_result(
            tool="smartlead.ai",
            inboxes=inboxes,
            present=present,
            provisional_errors=provisional_errors,
            missing_error_prefix="Smartlead account not found after validation",
            total_candidates=len(target_set),
        )

    def _upload_email_bison_google_via_oauth(
        self,
        *,
        credential: Dict[str, Any],
        inboxes: List[Dict[str, Any]],
        onepassword: Any,
        headless: bool,
        api_key: str,
        settings: Dict[str, Any],
    ) -> Dict[str, Any]:
        targets = [self._normalize_email(row.get("email")) for row in inboxes]
        targets = [email for email in targets if email]
        target_set = set(targets)
        present: Set[str] = set()
        provisional_errors: Dict[str, str] = {}
        skipped_already_uploaded = 0

        base_url = self._normalize_email_bison_base_url(credential.get("tool_url") or credential.get("url"))
        workspace_name = str(
            credential.get("workspace_name")
            or credential.get("workspace")
            or (credential.get("extra_fields") or {}).get("workspace")
            or ""
        ).strip()
        bison_username = str(credential.get("username") or "").strip()
        bison_password = str(credential.get("password") or "").strip()

        if not target_set:
            return {
                "tool": "email-bison",
                "total_candidates": 0,
                "uploaded_emails": [],
                "failed_uploads": [],
                "skipped_already_uploaded": 0,
            }
        if not base_url:
            return self._failed_result("email-bison", target_set, "Missing Email Bison instance URL")
        if not bison_username or not bison_password:
            return self._failed_result("email-bison", target_set, "Missing Email Bison username/password")

        logger.info(
            "[SendingToolUploader:EmailBison] Using Playwright Google OAuth upload for %s inbox(es) (headless=%s)",
            len(target_set),
            headless,
        )

        browser = None
        context = None
        page = None
        playwright = None
        try:
            from playwright.sync_api import sync_playwright

            playwright = sync_playwright().start()
            browser = self._launch_playwright_browser(playwright, headless=headless)
            context, page = self._create_oauth_context_page(browser)
            self._email_bison_login(
                page=page,
                base_url=base_url,
                username=bison_username,
                password=bison_password,
            )
            self._ensure_email_bison_workspace(page=page, expected_workspace=workspace_name)

            total_inboxes = len(inboxes)
            for index, inbox in enumerate(inboxes, start=1):
                email = self._normalize_email(inbox.get("email"))
                password = str(inbox.get("password") or "").strip()
                otp_secret = str(inbox.get("otp_secret") or "").strip()
                if not email:
                    continue
                if not password:
                    provisional_errors[email] = "Missing inbox password"
                    continue

                started_at = time.time()
                logger.info(
                    "[SendingToolUploader:EmailBison] [%s/%s] Starting Google OAuth for %s",
                    index,
                    total_inboxes,
                    email,
                )
                try:
                    self._ensure_email_bison_workspace(page=page, expected_workspace=workspace_name)
                    if self._email_bison_sender_visible(page=page, base_url=base_url, email=email):
                        present.add(email)
                        skipped_already_uploaded += 1
                        logger.info(
                            "[SendingToolUploader:EmailBison] [%s/%s] Already connected: %s",
                            index,
                            total_inboxes,
                            email,
                        )
                        continue

                    # Keep the Email Bison session, but remove Google cookies/storage before
                    # every account so a prior inbox cannot be silently reused on the same IP.
                    self._clear_google_session_state(context)
                    self._start_email_bison_google_oauth(
                        page=page,
                        base_url=base_url,
                        inbox=inbox,
                    )
                    self._email_bison_capture(page=page, email=email, label="oauth-started")
                    self._complete_google_signin_and_consent(
                        page=page,
                        context=context,
                        email=email,
                        password=password,
                        onepassword=onepassword,
                        otp_secret=otp_secret,
                    )
                    self._wait_for_url_contains(page, ["/sender-emails"], timeout_seconds=35)
                    self._email_bison_capture(page=page, email=email, label="after-oauth-callback")
                    self._ensure_email_bison_workspace(page=page, expected_workspace=workspace_name)
                    if self._email_bison_oauth_success_for_email(page_url=str(getattr(page, "url", "") or ""), email=email):
                        present.add(email)
                        logger.info(
                            "[SendingToolUploader:EmailBison] [%s/%s] OAuth callback confirmed %s (%.1fs)",
                            index,
                            total_inboxes,
                            email,
                            time.time() - started_at,
                        )
                    elif self._email_bison_sender_visible(page=page, base_url=base_url, email=email):
                        present.add(email)
                        logger.info(
                            "[SendingToolUploader:EmailBison] [%s/%s] OAuth complete for %s (%.1fs)",
                            index,
                            total_inboxes,
                            email,
                            time.time() - started_at,
                        )
                    else:
                        provisional_errors[email] = "Email Bison did not show the inbox after OAuth callback"
                except Exception as exc:
                    try:
                        self._email_bison_capture(page=page, email=email, label="failure")
                    except Exception:
                        pass
                    provisional_errors[email] = str(exc)
                    logger.warning(
                        "[SendingToolUploader:EmailBison] [%s/%s] OAuth failed for %s (%.1fs): %s",
                        index,
                        total_inboxes,
                        email,
                        time.time() - started_at,
                        exc,
                    )
                finally:
                    try:
                        self._clear_google_session_state(context)
                    except Exception:
                        pass
                    try:
                        page.bring_to_front()
                    except Exception:
                        pass
        except Exception as exc:
            message = f"Playwright Email Bison Google OAuth upload failed: {exc}"
            logger.warning("[SendingToolUploader:EmailBison] %s", message)
            for email in target_set:
                provisional_errors.setdefault(email, message)
        finally:
            self._close_oauth_context_page(context=context, page=page)
            if browser is not None:
                try:
                    browser.close()
                except Exception:
                    pass
            if playwright is not None:
                try:
                    playwright.stop()
                except Exception:
                    pass

        result = self._build_result(
            tool="email-bison",
            inboxes=inboxes,
            present=present,
            provisional_errors=provisional_errors,
            missing_error_prefix="Email Bison account not found after validation",
            total_candidates=len(target_set),
        )
        result["skipped_already_uploaded"] = skipped_already_uploaded
        result = self._apply_email_bison_settings(
            api_key=api_key,
            base_url=base_url,
            result=result,
            settings=settings,
        )
        return result

    def _apply_email_bison_settings(
        self,
        *,
        api_key: str,
        base_url: str,
        result: Dict[str, Any],
        settings: Dict[str, Any],
    ) -> Dict[str, Any]:
        uploaded = [
            self._normalize_email(email)
            for email in (result.get("uploaded_emails") or [])
            if self._normalize_email(email)
        ]
        if not uploaded:
            return result
        if not self._has_email_bison_settings_to_apply(settings):
            result["settings_required"] = False
            result["settings_skipped"] = True
            return result

        result["settings_required"] = True
        if not str(api_key or "").strip():
            failed = list(result.get("failed_uploads") or [])
            for email in uploaded:
                failed.append({
                    "email": email,
                    "error": "Email Bison Workspace API key is required to apply requested sending settings",
                })
            result["failed_uploads"] = failed
            result["uploaded_emails"] = []
            result["settings_validated"] = False
            return result

        account_by_email: Dict[str, Dict[str, Any]] = {}
        failed = list(result.get("failed_uploads") or [])
        failed_emails: Set[str] = set()
        for email in uploaded:
            try:
                account = self._fetch_email_bison_sender_email(api_key=api_key, base_url=base_url, email=email)
                if not account:
                    failed.append({"email": email, "error": "Email Bison sender email was not found after OAuth upload"})
                    failed_emails.add(email)
                    continue
                if not self._email_bison_sender_ready(account):
                    failed.append({
                        "email": email,
                        "error": f"Email Bison sender email is {account.get('status') or 'not connected'} after upload",
                    })
                    failed_emails.add(email)
                    continue
                account_by_email[email] = account
            except Exception as exc:
                failed.append({"email": email, "error": f"Email Bison sender lookup failed: {exc}"})
                failed_emails.add(email)

        if account_by_email:
            setting_failures = self._patch_email_bison_settings(
                api_key=api_key,
                base_url=base_url,
                account_by_email=account_by_email,
                settings=settings,
            )
            for email, error in setting_failures.items():
                failed.append({"email": email, "error": error})
                failed_emails.add(email)

        result["failed_uploads"] = failed
        result["uploaded_emails"] = [email for email in uploaded if email not in failed_emails]
        result["settings_validated"] = not failed_emails and len(account_by_email) == len(uploaded)
        if result["settings_validated"]:
            result["applied_settings"] = self._email_bison_applied_settings_summary(settings)
        return result

    def _email_bison_login(self, *, page: Any, base_url: str, username: str, password: str) -> None:
        page.goto(f"{base_url}/login", wait_until="domcontentloaded", timeout=60_000)
        if not self._exists(page, ["input[name='email']", "input[type='email']"], timeout_ms=3_000):
            if "/dashboard" in str(getattr(page, "url", "") or "") or self._exists(page, ["text='Sender Emails'"], timeout_ms=2_000):
                return
        self._fill_any(page, ["input[name='email']", "input[type='email']"], username, timeout_ms=15_000)
        self._fill_any(page, ["input[name='password']", "input[type='password']"], password, timeout_ms=15_000)
        clicked = self._click_any(
            page,
            [
                "button[type='submit']",
                "button:has-text('Login')",
                "button:has-text('Log in')",
                "xpath=//button[contains(.,'Login')]",
            ],
            timeout_ms=10_000,
            optional=True,
        )
        if not clicked:
            raise RuntimeError("Email Bison login button was not found")
        if not self._wait_for_url_contains(page, ["/dashboard", "/sender-emails"], timeout_seconds=60):
            body = self._safe_body_excerpt(page, limit=500)
            raise RuntimeError(f"Email Bison login did not reach the app. url={getattr(page, 'url', '')} body={body}")

    def _ensure_email_bison_workspace(self, *, page: Any, expected_workspace: str) -> None:
        expected = str(expected_workspace or "").strip()
        if not expected:
            return
        body = self._safe_body_excerpt(page, limit=8_000)
        if self._contains_normalized(body, expected):
            return

        workspace_xpath = self._xpath_literal(expected)
        opened = self._click_any(
            page,
            [
                f"button:has-text('{expected}')",
                "button.group\\/select-button",
                "xpath=//button[contains(@class,'select-button')]",
            ],
            timeout_ms=5_000,
            optional=True,
        )
        if not opened:
            raise RuntimeError(f"Email Bison workspace selector was not found; expected workspace: {expected}")

        try:
            self._fill_any(
                page,
                [
                    "input[placeholder*='Search workspaces' i]",
                    "input[type='search']",
                    "input[placeholder*='Search' i]",
                ],
                expected,
                timeout_ms=3_000,
            )
            time.sleep(0.5)
        except Exception:
            pass

        clicked = self._click_any(
            page,
            [
                f"xpath=//*[@role='option' and contains(normalize-space(), {workspace_xpath})]",
                f"xpath=//*[@role='menuitem' and contains(normalize-space(), {workspace_xpath})]",
                f"xpath=//button[contains(normalize-space(), {workspace_xpath})]",
                f"xpath=//a[contains(normalize-space(), {workspace_xpath})]",
                f"xpath=//div[contains(normalize-space(), {workspace_xpath})]",
            ],
            timeout_ms=5_000,
            optional=True,
        )
        if clicked:
            try:
                page.wait_for_load_state("domcontentloaded", timeout=8_000)
            except Exception:
                pass
            time.sleep(1.0)

        body = self._safe_body_excerpt(page, limit=8_000)
        if not self._contains_normalized(body, expected):
            raise RuntimeError(f"Email Bison is not in the expected workspace: {expected}")

    def _start_email_bison_google_oauth(self, *, page: Any, base_url: str, inbox: Dict[str, Any]) -> None:
        email = self._normalize_email(inbox.get("email"))
        display_name = self._display_name_for_inbox(inbox)
        page.goto(f"{base_url}/sender-email-connect/google/oauth", wait_until="domcontentloaded", timeout=60_000)
        self._fill_any(
            page,
            ["input[name='name']"],
            display_name,
            timeout_ms=15_000,
        )
        self._fill_any(
            page,
            ["input[name='email']"],
            email,
            timeout_ms=15_000,
        )
        clicked = False
        for selector in (
            "a[wire\\:click\\.prevent='submit']",
            "a:has-text('Sign in with Google')",
            "button:has-text('Sign in with Google')",
            "xpath=//a[contains(normalize-space(),'Sign in with Google')]",
            "xpath=//button[contains(normalize-space(),'Sign in with Google')]",
        ):
            try:
                loc = page.locator(self._selector(selector)).first
                loc.wait_for(state="visible", timeout=5_000)
                loc.click(timeout=8_000, force=True)
                clicked = True
                break
            except Exception:
                continue
        if not clicked:
            raise RuntimeError("Email Bison Google sign-in button was not found")
        end_at = time.time() + 20
        while time.time() < end_at:
            try:
                page.wait_for_load_state("domcontentloaded", timeout=3_000)
            except Exception:
                pass
            url = str(getattr(page, "url", "") or "").lower()
            if "accounts.google.com" in url or self._is_sending_tool_oauth_callback_url(url):
                return
            if "/sender-emails" in url and "sender-email-connect/google/oauth" not in url:
                return
            time.sleep(0.75)
        body = self._safe_body_excerpt(page, limit=1_000)
        raise RuntimeError(
            "Email Bison did not open Google OAuth after clicking Sign in with Google. "
            f"url={str(getattr(page, 'url', '') or '')} body={body}"
        )

    def _email_bison_sender_visible(self, *, page: Any, base_url: str, email: str) -> bool:
        clean_email = self._normalize_email(email)
        if not clean_email:
            return False
        try:
            page.goto(f"{base_url}/sender-emails", wait_until="domcontentloaded", timeout=60_000)
        except Exception:
            return False
        time.sleep(1.5)
        if clean_email in self._safe_body_excerpt(page, limit=10_000).lower():
            return True

        # Best-effort search/filter support. Email Bison has changed this surface
        # a few times, so try generic visible search/email inputs without making
        # validation depend on any single selector.
        try:
            self._click_any(
                page,
                ["button:has-text('Toggle Filters')", "xpath=//*[contains(normalize-space(),'Toggle Filters')]"],
                timeout_ms=2_500,
                optional=True,
            )
            filled = False
            for selector in (
                "input[type='search']",
                "input[placeholder*='Search' i]",
                "input[placeholder*='Email' i]",
                "xpath=//label[contains(.,'Email')]/following::input[1]",
            ):
                try:
                    loc = page.locator(selector).first
                    loc.wait_for(state="visible", timeout=2_500)
                    loc.fill(clean_email)
                    filled = True
                    break
                except Exception:
                    continue
            if filled:
                try:
                    page.keyboard.press("Enter")
                except Exception:
                    pass
                time.sleep(2.0)
                return clean_email in self._safe_body_excerpt(page, limit=10_000).lower()
        except Exception:
            pass
        return False

    @staticmethod
    def _email_bison_oauth_success_for_email(*, page_url: str, email: str) -> bool:
        clean_email = SendingToolUploader._normalize_email(email)
        if not clean_email:
            return False
        try:
            from urllib.parse import parse_qs, unquote, urlparse

            parsed = urlparse(str(page_url or ""))
            if not str(parsed.path or "").rstrip("/").endswith("/sender-emails"):
                return False
            query = parse_qs(parsed.query or "")
            success = str((query.get("success") or [""])[0]).strip().lower()
            callback_email = SendingToolUploader._normalize_email(unquote(str((query.get("email") or [""])[0])))
            account_type = str((query.get("account_type") or [""])[0]).strip().lower()
            return success in {"1", "true", "yes"} and callback_email == clean_email and account_type == "google"
        except Exception:
            return False

    def _email_bison_capture(self, *, page: Any, email: str, label: str) -> None:
        debug_dir = str(os.getenv("EMAIL_BISON_DEBUG_DIR") or "/tmp/email-bison-uploader").strip()
        if not debug_dir:
            return
        safe_email = re.sub(r"[^a-z0-9_.@-]+", "_", self._normalize_email(email), flags=re.I).strip("_") or "unknown"
        safe_label = re.sub(r"[^a-z0-9_.-]+", "_", str(label or "checkpoint"), flags=re.I).strip("_")
        timestamp = time.strftime("%Y%m%d%H%M%S")
        folder = os.path.join(debug_dir, safe_email)
        try:
            os.makedirs(folder, exist_ok=True)
            page.screenshot(path=os.path.join(folder, f"{timestamp}-{safe_label}.png"), full_page=True)
            with open(os.path.join(folder, f"{timestamp}-{safe_label}.txt"), "w", encoding="utf-8") as handle:
                handle.write(f"url={str(getattr(page, 'url', '') or '')}\n\n")
                handle.write(self._safe_body_excerpt(page, limit=30_000))
        except Exception:
            pass

    @staticmethod
    def _normalize_email_bison_base_url(raw_url: Any) -> str:
        text = str(raw_url or "").strip()
        if not text:
            return ""
        if not re.match(r"^[a-z][a-z0-9+.-]*://", text, re.I):
            text = f"https://{text}"
        return text.rstrip("/")

    @staticmethod
    def _display_name_for_inbox(inbox: Dict[str, Any]) -> str:
        first = str(inbox.get("first_name") or "").strip()
        last = str(inbox.get("last_name") or "").strip()
        display = f"{first} {last}".strip()
        email = str(inbox.get("email") or "").strip()
        return display or email.split("@", 1)[0].replace(".", " ").title() or email

    @staticmethod
    def _failed_result(tool: str, targets: Set[str], message: str) -> Dict[str, Any]:
        return {
            "tool": tool,
            "total_candidates": len(targets),
            "uploaded_emails": [],
            "failed_uploads": [
                {"email": email, "error": message}
                for email in sorted(targets)
            ],
            "skipped_already_uploaded": 0,
        }

    def _fetch_email_bison_sender_email(self, *, api_key: str, base_url: str, email: str) -> Optional[Dict[str, Any]]:
        normalized = self._normalize_email(email)
        if not normalized:
            return None
        headers = self._email_bison_headers(api_key)
        try:
            response = requests.get(
                f"{base_url}/api/sender-emails/{requests.utils.quote(normalized, safe='')}",
                headers=headers,
                timeout=self.timeout_seconds,
            )
            if response.status_code and response.status_code != 404:
                response.raise_for_status()
            if response.status_code != 404:
                for row in self._email_bison_sender_rows(self._json_or_empty(response)):
                    account = self._normalize_email_bison_sender(row, fallback_email=normalized)
                    if account and self._normalize_email(account.get("email")) == normalized:
                        return account
        except requests.HTTPError:
            raise
        except Exception:
            pass

        response = requests.get(
            f"{base_url}/api/sender-emails",
            headers=headers,
            params={"search": normalized},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        for row in self._email_bison_sender_rows(self._json_or_empty(response)):
            account = self._normalize_email_bison_sender(row)
            if account and self._normalize_email(account.get("email")) == normalized:
                return account
        return None

    def _patch_email_bison_settings(
        self,
        *,
        api_key: str,
        base_url: str,
        account_by_email: Dict[str, Dict[str, Any]],
        settings: Dict[str, Any],
    ) -> Dict[str, str]:
        failures: Dict[str, str] = {}
        entries = list(account_by_email.items())
        ids: List[int] = []
        for email, account in entries:
            try:
                account_id = int(account.get("id") or 0)
            except Exception:
                account_id = 0
            if account_id <= 0:
                failures[email] = "Email Bison sender email ID was missing from the API response"
            else:
                ids.append(account_id)
        if not ids:
            return failures

        headers = self._email_bison_headers(api_key)

        def fail_all(message: str) -> None:
            for email, _account in entries:
                failures.setdefault(email, message)

        try:
            if settings.get("dailyLimit") is not None:
                response = requests.patch(
                    f"{base_url}/api/sender-emails/daily-limits/bulk",
                    headers=headers,
                    json={
                        "sender_email_ids": ids,
                        "daily_limit": max(0, int(round(float(settings.get("dailyLimit"))))),
                    },
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()

            if settings.get("signature") is not None:
                response = requests.patch(
                    f"{base_url}/api/sender-emails/signatures/bulk",
                    headers=headers,
                    json={
                        "sender_email_ids": ids,
                        "email_signature": str(settings.get("signature") or ""),
                    },
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()

            if settings.get("enableWarmup") is not None:
                action = "enable" if bool(settings.get("enableWarmup")) else "disable"
                response = requests.patch(
                    f"{base_url}/api/warmup/sender-emails/{action}",
                    headers=headers,
                    json={"sender_email_ids": ids},
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()

            if settings.get("bisonWarmupDailyLimit") is not None:
                response = requests.patch(
                    f"{base_url}/api/warmup/sender-emails/update-daily-warmup-limits",
                    headers=headers,
                    json={
                        "sender_email_ids": ids,
                        "daily_limit": max(0, int(round(float(settings.get("bisonWarmupDailyLimit"))))),
                    },
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()

            tags = self._unique_tags(settings)
            if tags:
                tag_ids = self._resolve_email_bison_tag_ids(api_key=api_key, base_url=base_url, tag_names=tags)
                if tag_ids:
                    response = requests.post(
                        f"{base_url}/api/tags/attach-to-sender-emails",
                        headers=headers,
                        json={"tag_ids": tag_ids, "sender_email_ids": ids, "skip_webhooks": True},
                        timeout=self.timeout_seconds,
                    )
                    response.raise_for_status()
        except Exception as exc:
            fail_all(f"Email Bison settings update failed: {exc}")
        return failures

    def _resolve_email_bison_tag_ids(self, *, api_key: str, base_url: str, tag_names: List[str]) -> List[int]:
        ids: List[int] = []
        headers = self._email_bison_headers(api_key)
        for tag_name in tag_names:
            clean_name = str(tag_name or "").strip()
            if not clean_name:
                continue
            found_id = 0
            try:
                response = requests.get(
                    f"{base_url}/api/tags",
                    headers=headers,
                    params={"search": clean_name},
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                for row in self._email_bison_sender_rows(self._json_or_empty(response)):
                    name = str(row.get("name") or row.get("label") or "").strip()
                    if name.lower() == clean_name.lower():
                        tag_id = int(row.get("id") or 0)
                        if tag_id > 0:
                            found_id = tag_id
                            break
                if found_id > 0:
                    ids.append(found_id)
                    continue
            except Exception:
                pass
            response = requests.post(
                f"{base_url}/api/tags",
                headers=headers,
                json={"name": clean_name},
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            payload = self._json_or_empty(response)
            rows = self._email_bison_sender_rows(payload)
            tag_id = 0
            if rows:
                tag_id = int(rows[0].get("id") or 0)
            elif isinstance(payload, dict):
                tag_id = int(payload.get("id") or payload.get("tag_id") or 0)
            if tag_id <= 0:
                raise RuntimeError(f"Email Bison tag creation did not return an id for {clean_name}")
            ids.append(tag_id)
        return ids

    @staticmethod
    def _email_bison_headers(api_key: str) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {str(api_key or '').strip()}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _email_bison_sender_rows(data: Any) -> List[Dict[str, Any]]:
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        if isinstance(data, dict):
            for key in ("data", "sender_emails", "senderEmails", "tags"):
                value = data.get(key)
                if isinstance(value, list):
                    return [row for row in value if isinstance(row, dict)]
                if isinstance(value, dict):
                    return [value]
            return [data]
        return []

    def _normalize_email_bison_sender(self, row: Any, *, fallback_email: str = "") -> Optional[Dict[str, Any]]:
        if not isinstance(row, dict):
            return None
        email = self._normalize_email(
            row.get("email") or row.get("sender_email") or row.get("senderEmail") or row.get("username") or fallback_email
        )
        if not email:
            return None
        return {
            "id": str(row.get("id") or row.get("sender_email_id") or row.get("senderEmailId") or "").strip(),
            "email": email,
            "status": str(row.get("status") or "").strip(),
            "raw": row,
        }

    @staticmethod
    def _email_bison_sender_ready(account: Dict[str, Any]) -> bool:
        status = str(account.get("status") or "").strip().lower()
        return not status or status in {"connected", "active", "ready", "enabled"}

    @staticmethod
    def _has_email_bison_settings_to_apply(settings: Dict[str, Any]) -> bool:
        if not settings.get("applyRequested"):
            return False
        return any(
            [
                settings.get("dailyLimit") is not None,
                settings.get("enableWarmup") is not None,
                settings.get("bisonWarmupDailyLimit") is not None,
                settings.get("signature") is not None,
                bool(settings.get("tag")),
                bool(settings.get("tags")),
            ]
        )

    @staticmethod
    def _email_bison_applied_settings_summary(settings: Dict[str, Any]) -> List[str]:
        labels: List[str] = []
        if settings.get("dailyLimit") is not None:
            labels.append("dailyLimit")
        if settings.get("enableWarmup") is not None:
            labels.append("warmup")
        if settings.get("bisonWarmupDailyLimit") is not None:
            labels.append("bisonWarmupDailyLimit")
        if settings.get("signature") is not None:
            labels.append("signature")
        if settings.get("tag") or settings.get("tags"):
            labels.append("tags")
        return labels

    def _upload_amplemarket(
        self,
        *,
        api_key: str,
        amplemarket_username: str,
        amplemarket_password: str,
        inboxes: List[Dict[str, Any]],
        provider: str,
        settings: Dict[str, Any],
        onepassword: Any,
        headless: bool,
        use_browser: bool,
    ) -> Dict[str, Any]:
        targets = [self._normalize_email(row.get("email")) for row in inboxes]
        targets = [email for email in targets if email]
        target_set = set(targets)
        failed_uploads: List[Dict[str, str]] = []
        uploaded_emails: Set[str] = set()

        if not str(api_key or "").strip():
            return {
                "tool": "amplemarket",
                "total_candidates": len(target_set),
                "uploaded_emails": [],
                "failed_uploads": [
                    {"email": email, "error": "Missing Amplemarket API key"}
                    for email in sorted(target_set)
                ],
                "skipped_already_uploaded": 0,
            }

        if not str(amplemarket_username or "").strip() or not str(amplemarket_password or "").strip():
            return {
                "tool": "amplemarket",
                "total_candidates": len(target_set),
                "uploaded_emails": [],
                "failed_uploads": [
                    {"email": email, "error": "Missing Amplemarket username/password required for mailbox connection"}
                    for email in sorted(target_set)
                ],
                "skipped_already_uploaded": 0,
            }

        try:
            mailboxes = self._fetch_amplemarket_mailboxes(api_key=api_key)
        except Exception as exc:
            return {
                "tool": "amplemarket",
                "total_candidates": len(target_set),
                "uploaded_emails": [],
                "failed_uploads": [
                    {"email": email, "error": f"Amplemarket mailbox lookup failed ({exc})"}
                    for email in sorted(target_set)
                ],
                "skipped_already_uploaded": 0,
            }

        mailbox_by_email = self._index_amplemarket_mailboxes(mailboxes)
        browser_errors: Dict[str, str] = {}
        inboxes_needing_browser = [
            row
            for row in inboxes
            if self._normalize_email(row.get("email"))
            and (
                self._normalize_email(row.get("email")) not in mailbox_by_email
                or not self._amplemarket_mailbox_active(
                    mailbox_by_email.get(self._normalize_email(row.get("email")))
                )
            )
        ]
        if inboxes_needing_browser and use_browser:
            browser_errors = self._upload_amplemarket_via_browser(
                username=amplemarket_username,
                password=amplemarket_password,
                inboxes=inboxes_needing_browser,
                provider=provider,
                onepassword=onepassword,
                headless=headless,
            )
            try:
                mailboxes = self._fetch_amplemarket_mailboxes(api_key=api_key)
                mailbox_by_email = self._index_amplemarket_mailboxes(mailboxes)
            except Exception as exc:
                for row in inboxes_needing_browser:
                    email = self._normalize_email(row.get("email"))
                    if email and email not in browser_errors:
                        browser_errors[email] = f"Amplemarket mailbox lookup failed after browser upload ({exc})"
        elif inboxes_needing_browser:
            for row in inboxes_needing_browser:
                email = self._normalize_email(row.get("email"))
                if email:
                    browser_errors[email] = "Amplemarket browser mailbox upload is disabled"

        apply_daily_limit = settings.get("applyRequested") is True and settings.get("dailyLimit") is not None
        seen_failed: Set[str] = set()
        for email in targets:
            mailbox = mailbox_by_email.get(email)
            if not mailbox:
                if email not in seen_failed:
                    browser_error = browser_errors.get(email)
                    failed_uploads.append({
                        "email": email,
                        "error": (
                            f"Amplemarket mailbox was not found after browser upload ({browser_error})"
                            if browser_error
                            else "Amplemarket mailbox was not found after browser upload. Confirm the mailbox can be added in Amplemarket Account Settings > Mailboxes."
                        ),
                    })
                    seen_failed.add(email)
                continue

            status = str(mailbox.get("status") or "").strip().lower()
            if not self._amplemarket_mailbox_active(mailbox):
                if email not in seen_failed:
                    browser_error = browser_errors.get(email)
                    failed_uploads.append({
                        "email": email,
                        "error": (
                            f"Amplemarket mailbox is {status or 'not active'} after browser upload ({browser_error})"
                            if browser_error
                            else f"Amplemarket mailbox is {status or 'not active'}; reconnect or activate it before retrying."
                        ),
                    })
                    seen_failed.add(email)
                continue

            if apply_daily_limit:
                try:
                    self._apply_amplemarket_mailbox_settings(
                        api_key=api_key,
                        mailbox=mailbox,
                        settings=settings,
                    )
                except Exception as exc:
                    if email not in seen_failed:
                        failed_uploads.append({
                            "email": email,
                            "error": f"Amplemarket settings apply failed ({exc})",
                        })
                        seen_failed.add(email)
                    continue

            uploaded_emails.add(email)

        return {
            "tool": "amplemarket",
            "total_candidates": len(target_set),
            "uploaded_emails": sorted(uploaded_emails),
            "failed_uploads": failed_uploads,
            "skipped_already_uploaded": 0,
        }

    def _index_amplemarket_mailboxes(self, mailboxes: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        mailbox_by_email: Dict[str, Dict[str, Any]] = {}
        for mailbox in mailboxes:
            email = self._normalize_email(mailbox.get("email"))
            if not email:
                continue
            current = mailbox_by_email.get(email)
            if current is None or (
                not self._amplemarket_mailbox_active(current)
                and self._amplemarket_mailbox_active(mailbox)
            ):
                mailbox_by_email[email] = mailbox
        return mailbox_by_email

    @staticmethod
    def _amplemarket_mailbox_active(mailbox: Optional[Dict[str, Any]]) -> bool:
        if not mailbox:
            return False
        status = str(mailbox.get("status") or "").strip().lower()
        return status in {"", "active", "connected"}

    def _upload_amplemarket_via_browser(
        self,
        *,
        username: str,
        password: str,
        inboxes: List[Dict[str, Any]],
        provider: str,
        onepassword: Any,
        headless: bool,
    ) -> Dict[str, str]:
        failures: Dict[str, str] = {}
        if not inboxes:
            return failures

        browser = None
        playwright = None
        context = None
        page = None
        try:
            from playwright.sync_api import sync_playwright

            playwright = sync_playwright().start()
            browser = self._launch_playwright_browser(playwright, headless=headless)
            context, page = self._create_oauth_context_page(browser)
            self._amplemarket_login(page, username, password)

            for inbox in inboxes:
                email = self._normalize_email(inbox.get("email"))
                if not email:
                    continue
                try:
                    self._connect_amplemarket_mailbox(
                        page=page,
                        context=context,
                        inbox=inbox,
                        provider=provider,
                        onepassword=onepassword,
                    )
                except Exception as exc:
                    failures[email] = str(exc)
                    logger.warning("[SendingToolUploader:Amplemarket] Browser upload failed for %s: %s", email, exc)
                    try:
                        self._close_non_primary_pages(context, page)
                    except Exception:
                        pass
        except Exception as exc:
            message = str(exc)
            logger.warning("[SendingToolUploader:Amplemarket] Browser upload session failed: %s", message)
            for inbox in inboxes:
                email = self._normalize_email(inbox.get("email"))
                if email and email not in failures:
                    failures[email] = message
        finally:
            self._close_oauth_context_page(context=context, page=page)
            if browser is not None:
                try:
                    browser.close()
                except Exception:
                    pass
            if playwright is not None:
                try:
                    playwright.stop()
                except Exception:
                    pass
        return failures

    def _amplemarket_login(self, page: Any, username: str, password: str) -> None:
        login_url = str(os.getenv("AMPLEMARKET_LOGIN_URL") or "https://app.amplemarket.com/login").strip()
        page.goto(login_url, wait_until="domcontentloaded")
        self._fill_any(
            page,
            [
                "input[name='email']",
                "input[type='email']",
                "input[autocomplete='email']",
                "input[placeholder*='email' i]",
            ],
            username,
            timeout_ms=30_000,
        )
        self._fill_any(
            page,
            [
                "input[name='password']",
                "input[type='password']",
                "input[autocomplete='current-password']",
                "input[placeholder*='password' i]",
            ],
            password,
            timeout_ms=30_000,
        )
        self._click_any(
            page,
            [
                "button[type='submit']",
                "button:has-text('Sign in')",
                "button:has-text('Log in')",
                "button:has-text('Login')",
                "//button[contains(.,'Sign in') or contains(.,'Log in') or contains(.,'Login')]",
            ],
            timeout_ms=20_000,
        )

        end_at = time.time() + 35
        while time.time() < end_at:
            url = str(getattr(page, "url", "") or "").lower()
            if "/login" not in url and "/signin" not in url:
                return
            if self._exists(page, ["text=Invalid", "text=incorrect", "text=Wrong"], timeout_ms=500):
                raise RuntimeError("Amplemarket rejected the supplied username/password")
            time.sleep(0.75)
        raise RuntimeError("Amplemarket login did not complete")

    def _connect_amplemarket_mailbox(
        self,
        *,
        page: Any,
        context: Any,
        inbox: Dict[str, Any],
        provider: str,
        onepassword: Any,
    ) -> None:
        email = self._normalize_email(inbox.get("email"))
        if not email:
            raise RuntimeError("Missing inbox email")

        self._open_amplemarket_mailboxes(page)
        if provider == "google":
            oauth_page = self._start_amplemarket_google_oauth(page=page, context=context, inbox=inbox)
            if oauth_page is not None:
                self._complete_google_signin_and_consent(
                    page=oauth_page,
                    context=context,
                    email=email,
                    password=str(inbox.get("password") or "").strip(),
                    onepassword=onepassword,
                )
                time.sleep(2.0)
                self._close_non_primary_pages(context, page)
                self._click_any(
                    page,
                    [
                        "button:has-text('Done')",
                        "button:has-text('Finish')",
                        "button:has-text('Confirm')",
                        "button:has-text('Save')",
                        "//button[contains(.,'Done') or contains(.,'Finish') or contains(.,'Confirm') or contains(.,'Save')]",
                    ],
                    timeout_ms=8_000,
                    optional=True,
                )
                return

        self._connect_amplemarket_imap_smtp(page=page, inbox=inbox)

    def _open_amplemarket_mailboxes(self, page: Any) -> None:
        mailboxes_url = str(
            os.getenv("AMPLEMARKET_MAILBOXES_URL")
            or "https://app.amplemarket.com/dashboard/settings/mailboxes"
        ).strip()
        page.goto(mailboxes_url, wait_until="domcontentloaded")
        if self._exists(page, ["text=Mailboxes", "text=Add Mailbox", "text=Add mailbox"], timeout_ms=8_000):
            return
        self._click_any(
            page,
            [
                "text='Account Settings'",
                "text='Settings'",
                "//a[contains(.,'Account Settings') or contains(.,'Settings')]",
                "//button[contains(.,'Account Settings') or contains(.,'Settings')]",
            ],
            timeout_ms=8_000,
            optional=True,
        )
        self._click_any(
            page,
            [
                "text='Mailboxes'",
                "//a[contains(.,'Mailboxes')]",
                "//button[contains(.,'Mailboxes')]",
            ],
            timeout_ms=12_000,
        )

    def _start_amplemarket_google_oauth(self, *, page: Any, context: Any, inbox: Dict[str, Any]) -> Optional[Any]:
        before_pages = list(context.pages)
        self._click_any(
            page,
            [
                "button:has-text('Add Mailbox')",
                "button:has-text('Add mailbox')",
                "button:has-text('+ Add Mailbox')",
                "button:has-text('Connect Mailbox')",
                "//button[contains(.,'Add Mailbox') or contains(.,'Add mailbox') or contains(.,'Connect Mailbox')]",
            ],
            timeout_ms=15_000,
        )
        clicked = self._click_any(
            page,
            [
                "text='Google'",
                "text='Gmail'",
                "text='Google Workspace'",
                "button:has-text('Google')",
                "button:has-text('Gmail')",
                "//button[contains(.,'Google') or contains(.,'Gmail')]",
            ],
            timeout_ms=8_000,
            optional=True,
        )
        if not clicked:
            return None
        try:
            self._fill_any(
                page,
                ["input[type='email']", "input[name*='email' i]", "input[placeholder*='email' i]"],
                self._normalize_email(inbox.get("email")),
                timeout_ms=5_000,
            )
        except Exception:
            pass
        self._click_any(
            page,
            [
                "button:has-text('Connect')",
                "button:has-text('Continue')",
                "button:has-text('Next')",
                "//button[contains(.,'Connect') or contains(.,'Continue') or contains(.,'Next')]",
            ],
            timeout_ms=8_000,
            optional=True,
        )

        oauth_page = self._detect_oauth_page(context=context, fallback_page=page, previous_pages=before_pages)
        url = str(getattr(oauth_page, "url", "") or "").lower()
        return oauth_page if "accounts.google.com" in url or oauth_page is not page else None

    def _connect_amplemarket_imap_smtp(self, *, page: Any, inbox: Dict[str, Any]) -> None:
        email = self._normalize_email(inbox.get("email"))
        password = str(inbox.get("password") or "").strip()
        if not email or not password:
            raise RuntimeError("Missing inbox email/password for Amplemarket IMAP/SMTP setup")

        imap_host = str(inbox.get("imap_host") or os.getenv("SMTP_PLUS_IMAP_HOST") or os.getenv("IMAP_PROXY_HOST") or "imap.simpleinboxes.com").strip()
        imap_port = str(inbox.get("imap_port") or os.getenv("SMTP_PLUS_IMAP_PORT") or os.getenv("IMAP_PROXY_PORT") or "993").strip()
        smtp_host = str(inbox.get("smtp_host") or os.getenv("SMTP_PLUS_SMTP_HOST") or "smtp.office365.com").strip()
        smtp_port = str(inbox.get("smtp_port") or os.getenv("SMTP_PLUS_SMTP_PORT") or "587").strip()

        self._click_any(
            page,
            [
                "button:has-text('Add Mailbox')",
                "button:has-text('Add mailbox')",
                "button:has-text('+ Add Mailbox')",
                "button:has-text('Connect Mailbox')",
                "//button[contains(.,'Add Mailbox') or contains(.,'Add mailbox') or contains(.,'Connect Mailbox')]",
            ],
            timeout_ms=15_000,
            optional=True,
        )
        self._click_any(
            page,
            [
                "text='IMAP/SMTP Setup'",
                "text='IMAP/SMTP'",
                "text='SMTP'",
                "button:has-text('IMAP/SMTP')",
                "button:has-text('SMTP')",
                "//button[contains(.,'IMAP') or contains(.,'SMTP')]",
            ],
            timeout_ms=12_000,
            optional=True,
        )
        self._fill_any(
            page,
            [
                "input[type='email']",
                "input[name*='email' i]",
                "input[placeholder*='email' i]",
                "//label[contains(translate(normalize-space(.),'EMAIL','email'),'email')]/following::input[1]",
            ],
            email,
            timeout_ms=12_000,
        )
        for selectors, value in (
            (["input[name*='username' i]", "input[placeholder*='username' i]", "//label[contains(translate(normalize-space(.),'USERNAME','username'),'username')]/following::input[1]"], email),
            (["input[type='password']", "input[name*='password' i]", "input[placeholder*='password' i]"], password),
            (["input[name='imap_host']", "input[name='imapHost']", "input[placeholder*='IMAP host' i]", "//label[contains(translate(normalize-space(.),'IMAPHOST','imaphost'),'imap') and contains(translate(normalize-space(.),'IMAPHOST','imaphost'),'host')]/following::input[1]"], imap_host),
            (["input[name='imap_port']", "input[name='imapPort']", "input[placeholder*='IMAP port' i]", "//label[contains(translate(normalize-space(.),'IMAPPORT','imapport'),'imap') and contains(translate(normalize-space(.),'IMAPPORT','imapport'),'port')]/following::input[1]"], imap_port),
            (["input[name='smtp_host']", "input[name='smtpHost']", "input[placeholder*='SMTP host' i]", "//label[contains(translate(normalize-space(.),'SMTPHOST','smtphost'),'smtp') and contains(translate(normalize-space(.),'SMTPHOST','smtphost'),'host')]/following::input[1]"], smtp_host),
            (["input[name='smtp_port']", "input[name='smtpPort']", "input[placeholder*='SMTP port' i]", "//label[contains(translate(normalize-space(.),'SMTPPORT','smtpport'),'smtp') and contains(translate(normalize-space(.),'SMTPPORT','smtpport'),'port')]/following::input[1]"], smtp_port),
        ):
            try:
                self._fill_any(page, selectors, value, timeout_ms=8_000)
            except Exception:
                pass

        self._click_any(
            page,
            [
                "button:has-text('Confirm')",
                "button:has-text('Connect')",
                "button:has-text('Save')",
                "button[type='submit']",
                "//button[contains(.,'Confirm') or contains(.,'Connect') or contains(.,'Save')]",
            ],
            timeout_ms=20_000,
        )
        time.sleep(2.0)

    def _fetch_amplemarket_mailboxes(self, *, api_key: str) -> List[Dict[str, Any]]:
        mailboxes: List[Dict[str, Any]] = []
        url = "https://api.amplemarket.com/mailboxes"
        params: Optional[Dict[str, Any]] = {"page[size]": 20}
        for _ in range(100):
            response = requests.get(
                url,
                headers=self._amplemarket_headers(api_key),
                params=params,
                timeout=self.timeout_seconds,
            )
            if response.status_code >= 400:
                raise RuntimeError(self._response_error(response))
            payload = self._json_or_empty(response)
            rows = payload.get("mailboxes") if isinstance(payload, dict) else []
            if isinstance(rows, list):
                for row in rows:
                    mailbox = self._normalize_amplemarket_mailbox(row)
                    if mailbox:
                        mailboxes.append(mailbox)

            links = payload.get("_links") if isinstance(payload, dict) else {}
            next_href = str(((links or {}).get("next") or {}).get("href") or "").strip()
            if not next_href:
                break
            url = next_href if next_href.startswith("http") else f"https://api.amplemarket.com{'' if next_href.startswith('/') else '/'}{next_href}"
            params = None
        return mailboxes

    def _apply_amplemarket_mailbox_settings(
        self,
        *,
        api_key: str,
        mailbox: Dict[str, Any],
        settings: Dict[str, Any],
    ) -> None:
        try:
            daily_limit = int(round(float(settings.get("dailyLimit"))))
        except Exception:
            return
        daily_limit = max(0, daily_limit)
        if mailbox.get("daily_email_limit") == daily_limit:
            return

        mailbox_id = str(mailbox.get("id") or "").strip()
        if not mailbox_id:
            raise RuntimeError("Mailbox id is missing")
        response = requests.patch(
            f"https://api.amplemarket.com/mailboxes/{requests.utils.quote(mailbox_id)}",
            headers=self._amplemarket_headers(api_key),
            json={"daily_email_limit": daily_limit},
            timeout=self.timeout_seconds,
        )
        if response.status_code >= 400:
            raise RuntimeError(self._response_error(response))

    @staticmethod
    def _normalize_amplemarket_mailbox(row: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(row, dict):
            return None
        mailbox_id = str(row.get("id") or "").strip()
        email = SendingToolUploader._normalize_email(row.get("email"))
        if not mailbox_id or not email:
            return None
        result = dict(row)
        result["id"] = mailbox_id
        result["email"] = email
        raw_daily_limit = result.get("daily_email_limit")
        try:
            result["daily_email_limit"] = int(round(float(raw_daily_limit)))
        except Exception:
            result["daily_email_limit"] = None
        return result

    @staticmethod
    def _amplemarket_headers(api_key: str) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def _normalize_settings(self, settings: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        source = settings if isinstance(settings, dict) else {}
        if isinstance(settings, dict) and "postUploadEnabled" in settings:
            apply_requested = settings.get("postUploadEnabled") is True
        elif isinstance(settings, dict) and "applyRequested" in settings:
            apply_requested = settings.get("applyRequested") is True
        else:
            apply_requested = isinstance(settings, dict) and len(settings) > 0

        def as_number(value: Any, default: Optional[int] = None) -> Optional[int]:
            if value in (None, ""):
                return default
            try:
                return int(float(value))
            except Exception:
                return default

        def as_dict(value: Any) -> Dict[str, Any]:
            return dict(value) if isinstance(value, dict) else {}

        def as_signature(value: Any) -> str:
            text = "" if value is None else str(value)
            return (
                text.replace("\r\n", "\n")
                .replace("\\r\\n", "\n")
                .replace("\\n", "\n")
                .replace("\\r", "\n")
            )

        def as_tags(raw_tags: Any, raw_tag: Any) -> List[str]:
            values: List[Any] = []
            if isinstance(raw_tags, list):
                values = raw_tags
            elif isinstance(raw_tags, str):
                values = raw_tags.replace("\n", ",").split(",")
            elif isinstance(raw_tag, str):
                values = raw_tag.replace("\n", ",").split(",")

            seen: Set[str] = set()
            parsed: List[str] = []
            for value in values:
                tag = str(value or "").strip()
                normalized = tag.lower()
                if not tag or normalized in seen:
                    continue
                seen.add(normalized)
                parsed.append(tag)
            return parsed

        def normalize_instantly_reply_rate(value: Any) -> Any:
            if value in (None, ""):
                return value
            try:
                numeric = float(value)
            except Exception:
                return value
            return numeric * 100 if 0 < numeric <= 1 else numeric

        def instantly_warmup(value: Any) -> Dict[str, Any]:
            payload = as_dict(value)
            if "reply_rate" not in payload and "warmup_reply_rate" in payload:
                payload["reply_rate"] = payload.pop("warmup_reply_rate")
            if "reply_rate" in payload:
                payload["reply_rate"] = normalize_instantly_reply_rate(payload.get("reply_rate"))
            return payload

        tags = as_tags(source.get("tags"), source.get("tag"))

        return {
            "applyRequested": apply_requested,
            "enableWarmup": (source.get("enableWarmup", True) is not False) if apply_requested else None,
            "dailyLimit": as_number(source.get("dailyLimit")),
            "trackingDomainName": str(source.get("trackingDomainName") or "").strip(),
            "trackingDomainStatus": str(source.get("trackingDomainStatus") or "").strip(),
            "enableSlowRamp": source.get("enableSlowRamp", True) is not False,
            "sendingGap": as_number(source.get("sendingGap")),
            "signature": as_signature(source.get("signature")),
            "tag": tags[0] if tags else str(source.get("tag") or "").strip(),
            "tags": tags,
            "instantlyWarmup": instantly_warmup(source.get("instantlyWarmup")),
            "smartleadWarmup": as_dict(source.get("smartleadWarmup")),
            "smartleadAccount": as_dict(source.get("smartleadAccount")),
            "bisonWarmupDailyLimit": as_number(
                source.get("bisonWarmupDailyLimit")
                or as_dict(source.get("bisonWarmup")).get("dailyLimit")
                or as_dict(source.get("bisonWarmup")).get("daily_limit")
            ),
        }

    @staticmethod
    def _unique_tags(settings: Dict[str, Any]) -> List[str]:
        values: List[Any] = []
        raw_tags = settings.get("tags")
        raw_tag = settings.get("tag")
        if isinstance(raw_tags, list):
            values.extend(raw_tags)
        elif isinstance(raw_tags, str):
            values.extend(raw_tags.replace("\n", ",").split(","))
        if isinstance(raw_tag, str):
            values.extend(raw_tag.replace("\n", ",").split(","))

        seen: Set[str] = set()
        tags: List[str] = []
        for value in values:
            tag = str(value or "").strip()
            normalized = tag.lower()
            if not tag or normalized in seen:
                continue
            seen.add(normalized)
            tags.append(tag)
        return tags

    @staticmethod
    def _normalize_smartlead_warmup_payload(settings: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(settings.get("smartleadWarmup") or {})
        if "enabled" in payload and "warmup_enabled" not in payload:
            payload["warmup_enabled"] = bool(payload.get("enabled"))
        payload.pop("enabled", None)
        if settings.get("enableWarmup") is not None:
            payload["warmup_enabled"] = bool(settings.get("enableWarmup"))

        if "total_warmup_per_day" not in payload:
            if "warmup_daily_limit" in payload:
                payload["total_warmup_per_day"] = payload.pop("warmup_daily_limit")
            elif "limit" in payload:
                payload["total_warmup_per_day"] = payload.pop("limit")

        if "daily_rampup" not in payload:
            if "warmup_rampup_increment" in payload:
                payload["daily_rampup"] = payload.pop("warmup_rampup_increment")
            elif "rampup_increment" in payload:
                payload["daily_rampup"] = payload.pop("rampup_increment")
        if "daily_rampup" in payload:
            try:
                payload["daily_rampup"] = max(5, int(float(payload["daily_rampup"])))
            except Exception:
                payload.pop("daily_rampup", None)

        return payload

    def _apply_instantly_settings(
        self,
        *,
        api_key: str,
        result: Dict[str, Any],
        settings: Dict[str, Any],
    ) -> Dict[str, Any]:
        uploaded = [
            self._normalize_email(email)
            for email in (result.get("uploaded_emails") or [])
            if self._normalize_email(email)
        ]
        if not uploaded:
            return result
        if not settings.get("applyRequested"):
            return result

        should_apply = any(
            [
                settings.get("dailyLimit") is not None,
                bool(settings.get("trackingDomainName")),
                bool(settings.get("trackingDomainStatus")),
                settings.get("enableSlowRamp") is not None,
                settings.get("sendingGap") is not None,
                bool(settings.get("signature")),
                bool(settings.get("tag")),
                bool(settings.get("tags")),
                isinstance(settings.get("instantlyWarmup"), dict) and bool(settings.get("instantlyWarmup")),
                settings.get("enableWarmup") is not None,
            ]
        )
        if not should_apply:
            return result

        failed = list(result.get("failed_uploads") or [])
        for email in uploaded:
            try:
                self._instantly_patch_account(api_key=api_key, email=email, settings=settings)
                if settings.get("enableWarmup", True):
                    self._instantly_enable_warmup(api_key=api_key, email=email)
                tags = self._unique_tags(settings)
                for tag in tags:
                    tag_id = self._instantly_find_or_create_tag_id(api_key=api_key, tag_name=tag)
                    self._instantly_assign_tag(api_key=api_key, email=email, tag_id=tag_id)
            except Exception as exc:
                failed.append({"email": email, "error": f"Instantly settings apply failed: {exc}"})

        result["failed_uploads"] = failed
        result["uploaded_emails"] = [
            email for email in (result.get("uploaded_emails") or [])
            if self._normalize_email(email) not in {self._normalize_email(row.get("email")) for row in failed}
        ]
        return result

    def _instantly_patch_account(self, *, api_key: str, email: str, settings: Dict[str, Any]) -> None:
        payload: Dict[str, Any] = {}
        if settings.get("dailyLimit") is not None:
            payload["daily_limit"] = settings.get("dailyLimit")
        if settings.get("trackingDomainName"):
            payload["tracking_domain_name"] = settings.get("trackingDomainName")
        if settings.get("trackingDomainStatus"):
            payload["tracking_domain_status"] = settings.get("trackingDomainStatus")
        if settings.get("enableSlowRamp") is not None:
            payload["enable_slow_ramp"] = bool(settings.get("enableSlowRamp"))
        if settings.get("sendingGap") is not None:
            payload["sending_gap"] = settings.get("sendingGap")
        if settings.get("signature") is not None:
            payload["signature"] = settings.get("signature")

        warmup_payload = dict(settings.get("instantlyWarmup") or {})
        if "limit" not in warmup_payload and "warmup_daily_limit" in warmup_payload:
            warmup_payload["limit"] = warmup_payload.pop("warmup_daily_limit")
        if "increment" not in warmup_payload and "warmup_rampup_increment" in warmup_payload:
            warmup_payload["increment"] = warmup_payload.pop("warmup_rampup_increment")
        if "reply_rate" not in warmup_payload and "warmup_reply_rate" in warmup_payload:
            warmup_payload["reply_rate"] = warmup_payload.pop("warmup_reply_rate")
        if "reply_rate" in warmup_payload:
            try:
                reply_rate = float(warmup_payload["reply_rate"])
                warmup_payload["reply_rate"] = reply_rate * 100 if 0 < reply_rate <= 1 else reply_rate
            except Exception:
                pass
        if settings.get("enableWarmup") is not None:
            warmup_payload["enabled"] = bool(settings.get("enableWarmup"))
        if "advanced" in warmup_payload and not isinstance(warmup_payload.get("advanced"), dict):
            warmup_payload.pop("advanced", None)
        if warmup_payload:
            payload["warmup"] = warmup_payload

        if not payload:
            return

        response = requests.patch(
            f"https://api.instantly.ai/api/v2/accounts/{requests.utils.quote(email)}",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            raise RuntimeError(self._response_error(response))

    def _instantly_enable_warmup(self, *, api_key: str, email: str) -> None:
        payloads = [
            {"emails": [email]},
            {"account_emails": [email]},
            {"accounts": [email]},
            {"emails": [email], "enabled": True},
        ]
        last_error = ""
        for payload in payloads:
            response = requests.post(
                "https://api.instantly.ai/api/v2/accounts/warmup/enable",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=self.timeout_seconds,
            )
            if response.ok:
                return
            last_error = self._response_error(response)
        raise RuntimeError(last_error or "Instantly warmup enable failed")

    def _instantly_find_or_create_tag_id(self, *, api_key: str, tag_name: str) -> str:
        normalized_tag = str(tag_name or "").strip().lower()
        if not normalized_tag:
            raise RuntimeError("Empty Instantly tag name")

        tags = self._instantly_fetch_tags(api_key=api_key)
        for row in tags:
            if str(row.get("name") or "").strip().lower() == normalized_tag and row.get("id"):
                return str(row["id"])

        payloads = [
            {"label": tag_name, "color": "#64748B"},
            {"label": tag_name},
            {"name": tag_name},
            {"title": tag_name},
        ]
        last_error = ""
        for payload in payloads:
            response = requests.post(
                "https://api.instantly.ai/api/v2/custom-tags",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=self.timeout_seconds,
            )
            if response.ok:
                data = self._json_or_empty(response)
                tag_id = str((data or {}).get("id") or (data or {}).get("tag_id") or "").strip()
                if tag_id:
                    return tag_id
            else:
                last_error = self._response_error(response)

        tags = self._instantly_fetch_tags(api_key=api_key)
        for row in tags:
            if str(row.get("name") or "").strip().lower() == normalized_tag and row.get("id"):
                return str(row["id"])

        raise RuntimeError(last_error or f"Unable to create Instantly tag '{tag_name}'")

    def _instantly_fetch_tags(self, *, api_key: str) -> List[Dict[str, Any]]:
        response = requests.get(
            "https://api.instantly.ai/api/v2/custom-tags",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            params={"limit": 100},
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            raise RuntimeError(self._response_error(response))
        data = self._json_or_empty(response)
        rows = []
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            if isinstance(data.get("items"), list):
                rows = data.get("items") or []
            elif isinstance(data.get("data"), list):
                rows = data.get("data") or []

        parsed: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            parsed.append(
                {
                    "id": str(row.get("id") or row.get("tag_id") or "").strip(),
                    "name": str(row.get("name") or row.get("label") or row.get("title") or "").strip(),
                }
            )
        return parsed

    def _instantly_fetch_account(self, *, api_key: str, email: str) -> Dict[str, Any]:
        response = requests.get(
            f"https://api.instantly.ai/api/v2/accounts/{requests.utils.quote(email)}",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            raise RuntimeError(self._response_error(response))
        data = self._json_or_empty(response)
        if isinstance(data, dict) and isinstance(data.get("account"), dict):
            return data.get("account") or {}
        return data if isinstance(data, dict) else {}

    def _instantly_assign_tag(self, *, api_key: str, email: str, tag_id: str) -> None:
        payloads = [
            {"resource_type": 1, "resource_ids": [email], "tag_ids": [tag_id], "assign": True},
            {"resource_type": "account", "resource_ids": [email], "tag_ids": [tag_id], "assign": True},
            {"resource_type": "account", "resource_id": email, "tag_id": tag_id},
        ]
        last_error = ""
        for payload in payloads:
            response = requests.post(
                "https://api.instantly.ai/api/v2/custom-tags/toggle-resource",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=self.timeout_seconds,
            )
            if response.ok:
                if self._instantly_has_tag_mapping(api_key=api_key, email=email, tag_id=tag_id):
                    return
            last_error = self._response_error(response)
        raise RuntimeError(last_error or "Instantly tag assignment failed")

    def _instantly_has_tag_mapping(self, *, api_key: str, email: str, tag_id: str) -> bool:
        normalized = self._normalize_email(email)
        variants = [
            {"limit": 100, "tag_ids": str(tag_id), "resource_ids": normalized},
            {"limit": 100, "tag_id": str(tag_id), "resource_id": normalized},
        ]
        for attempt in range(3):
            for base_params in variants:
                params = dict(base_params)
                for _ in range(20):
                    response = requests.get(
                        "https://api.instantly.ai/api/v2/custom-tag-mappings",
                        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                        params=params,
                        timeout=self.timeout_seconds,
                    )
                    if not response.ok:
                        break
                    data = self._json_or_empty(response)
                    rows = []
                    if isinstance(data, dict) and isinstance(data.get("items"), list):
                        rows = data.get("items") or []
                    elif isinstance(data, dict) and isinstance(data.get("data"), list):
                        rows = data.get("data") or []
                    elif isinstance(data, list):
                        rows = data
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        mapped_email = self._normalize_email(row.get("resource_id") or row.get("email") or row.get("account_email"))
                        mapped_tag_id = str(row.get("tag_id") or row.get("custom_tag_id") or "")
                        if mapped_email == normalized and (not mapped_tag_id or mapped_tag_id == str(tag_id)):
                            return True
                    next_after = str(data.get("next_starting_after") or "").strip() if isinstance(data, dict) else ""
                    if not next_after:
                        break
                    params["starting_after"] = next_after
            if attempt < 2:
                time.sleep(3)
        return False

    def _apply_smartlead_settings(
        self,
        *,
        api_key: str,
        result: Dict[str, Any],
        settings: Dict[str, Any],
        credential: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        uploaded = [
            self._normalize_email(email)
            for email in (result.get("uploaded_emails") or [])
            if self._normalize_email(email)
        ]
        if not uploaded:
            return result
        if not settings.get("applyRequested"):
            return result

        should_apply = any(
            [
                settings.get("dailyLimit") is not None,
                bool(settings.get("signature")),
                bool(settings.get("tag")),
                bool(settings.get("tags")),
                isinstance(settings.get("smartleadWarmup"), dict) and bool(settings.get("smartleadWarmup")),
                isinstance(settings.get("smartleadAccount"), dict) and bool(settings.get("smartleadAccount")),
                settings.get("enableWarmup") is not None,
            ]
        )
        if not should_apply:
            return result

        tag_contexts: List[Dict[str, Any]] = []
        tags = self._unique_tags(settings)
        if tags:
            try:
                for tag_name in tags:
                    tag_contexts.append(
                        self._resolve_smartlead_tag_context(
                            fallback_api_key=api_key,
                            credential=credential or {},
                            tag_name=tag_name,
                        )
                    )
            except Exception as exc:
                failed = list(result.get("failed_uploads") or [])
                for email in uploaded:
                    failed.append({"email": email, "error": f"Smartlead tag setup failed: {exc}"})
                result["failed_uploads"] = failed
                result["uploaded_emails"] = [
                    email for email in (result.get("uploaded_emails") or [])
                    if self._normalize_email(email) not in {self._normalize_email(row.get("email")) for row in failed}
                ]
                return result

        failed = list(result.get("failed_uploads") or [])
        for email in uploaded:
            try:
                account = self._smartlead_fetch_account(api_key=api_key, email=email)
                if not account:
                    raise RuntimeError("Smartlead account not found")
                account_id = str(account.get("id") or account.get("email_account_id") or "").strip()
                if not account_id:
                    raise RuntimeError("Smartlead account ID missing")

                account_payload = dict(settings.get("smartleadAccount") or {})
                if settings.get("dailyLimit") is not None:
                    account_payload["max_email_per_day"] = settings.get("dailyLimit")
                if settings.get("signature") is not None:
                    account_payload["signature"] = settings.get("signature")
                if account_payload:
                    response = requests.post(
                        f"https://server.smartlead.ai/api/v1/email-accounts/{requests.utils.quote(account_id)}",
                        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                        params={"api_key": api_key},
                        json=account_payload,
                        timeout=self.timeout_seconds,
                    )
                    if not response.ok:
                        raise RuntimeError(self._response_error(response))

                for tag_context in tag_contexts:
                    self._smartlead_assign_tag_mapping(
                        api_key=tag_context["api_key"],
                        account_id=int(account_id),
                        tag_id=int(tag_context["tag_id"]),
                    )
                    mapped = self._smartlead_verify_tag_mapping(
                        jwt_token=str(tag_context["jwt_token"]),
                        account_id=int(account_id),
                        expected_tag_name=str(tag_context["tag_name"]),
                    )
                    if not mapped:
                        raise RuntimeError(f"Smartlead tag mapping not visible after assignment ({tag_context['tag_name']})")

                warmup_payload = self._normalize_smartlead_warmup_payload(settings)
                if warmup_payload:
                    response = requests.post(
                        f"https://server.smartlead.ai/api/v1/email-accounts/{requests.utils.quote(account_id)}/warmup",
                        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                        params={"api_key": api_key},
                        json=warmup_payload,
                        timeout=self.timeout_seconds,
                    )
                    if not response.ok:
                        raise RuntimeError(self._response_error(response))
            except Exception as exc:
                failed.append({"email": email, "error": f"Smartlead settings apply failed: {exc}"})

        result["failed_uploads"] = failed
        result["uploaded_emails"] = [
            email for email in (result.get("uploaded_emails") or [])
            if self._normalize_email(email) not in {self._normalize_email(row.get("email")) for row in failed}
        ]
        return result

    def _smartlead_get_session(
        self,
        *,
        fallback_api_key: str,
        credential: Dict[str, Any],
    ) -> Dict[str, str]:
        username = str((credential or {}).get("username") or "").strip()
        password = str((credential or {}).get("password") or "").strip()
        if not username or not password:
            raise RuntimeError("Smartlead username/password are required for tag management")

        response = requests.post(
            "https://server.smartlead.ai/api/auth/login",
            json={"email": username, "password": password},
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            raise RuntimeError(f"Smartlead login failed: {self._response_error(response)}")
        payload = self._json_or_empty(response)
        jwt_token = str((payload or {}).get("token") or "").strip()
        api_key = str(((payload or {}).get("user") or {}).get("api_key") or "").strip() or str(fallback_api_key or "").strip()
        if not jwt_token:
            raise RuntimeError("Smartlead login did not return JWT token")
        if not api_key:
            raise RuntimeError("Smartlead login did not return API key")
        return {"jwt_token": jwt_token, "api_key": api_key}

    def _smartlead_graphql(
        self,
        *,
        jwt_token: str,
        operation_name: str,
        query: str,
        variables: Dict[str, Any],
    ) -> Dict[str, Any]:
        response = requests.post(
            "https://fe-gql.smartlead.ai/v1/graphql",
            headers={"Authorization": f"Bearer {jwt_token}", "Content-Type": "application/json"},
            json={
                "operationName": operation_name,
                "query": query,
                "variables": variables,
            },
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            raise RuntimeError(f"Smartlead GraphQL request failed: {self._response_error(response)}")
        payload = self._json_or_empty(response)
        errors = payload.get("errors") if isinstance(payload, dict) else None
        if isinstance(errors, list) and errors:
            first = errors[0] if isinstance(errors[0], dict) else {}
            raise RuntimeError(str(first.get("message") or "Smartlead GraphQL returned errors"))
        return payload if isinstance(payload, dict) else {}

    def _resolve_smartlead_tag_context(
        self,
        *,
        fallback_api_key: str,
        credential: Dict[str, Any],
        tag_name: str,
    ) -> Dict[str, Any]:
        normalized_tag = str(tag_name or "").strip().lower()
        if not normalized_tag:
            raise RuntimeError("Smartlead tag name is empty")

        session = self._smartlead_get_session(fallback_api_key=fallback_api_key, credential=credential)
        jwt_token = str(session["jwt_token"])

        list_payload = self._smartlead_graphql(
            jwt_token=jwt_token,
            operation_name="getAllTags",
            query="""
                query getAllTags {
                  tags {
                    id
                    name
                    color
                    __typename
                  }
                }
            """,
            variables={},
        )
        tags = ((list_payload.get("data") or {}).get("tags") or []) if isinstance(list_payload, dict) else []
        for tag in tags:
            if not isinstance(tag, dict):
                continue
            if str(tag.get("name") or "").strip().lower() == normalized_tag:
                return {
                    "api_key": session["api_key"],
                    "jwt_token": jwt_token,
                    "tag_id": int(tag.get("id")),
                    "tag_name": str(tag.get("name") or tag_name),
                }

        create_payload = self._smartlead_graphql(
            jwt_token=jwt_token,
            operation_name="createTag",
            query="""
                mutation createTag($object: tags_insert_input!) {
                  insert_tags_one(object: $object) {
                    id
                    name
                    color
                    __typename
                  }
                }
            """,
            variables={"object": {"name": tag_name, "color": "#EFB1FC"}},
        )
        created = ((create_payload.get("data") or {}).get("insert_tags_one") or {}) if isinstance(create_payload, dict) else {}
        created_id = int(created.get("id") or 0)
        if not created_id:
            raise RuntimeError("Smartlead createTag did not return tag id")
        return {
            "api_key": session["api_key"],
            "jwt_token": jwt_token,
            "tag_id": created_id,
            "tag_name": str(created.get("name") or tag_name),
        }

    def _smartlead_assign_tag_mapping(self, *, api_key: str, account_id: int, tag_id: int) -> None:
        response = requests.post(
            "https://server.smartlead.ai/api/v1/email-accounts/tag-mapping",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            params={"api_key": api_key},
            json={"email_account_ids": [int(account_id)], "tag_ids": [int(tag_id)]},
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            raise RuntimeError(self._response_error(response))
        payload = self._json_or_empty(response)
        if isinstance(payload, dict) and payload.get("success") is False:
            raise RuntimeError(str(payload.get("message") or "Smartlead tag mapping failed"))

    def _smartlead_verify_tag_mapping(
        self,
        *,
        jwt_token: str,
        account_id: int,
        expected_tag_name: str,
    ) -> bool:
        payload = self._smartlead_graphql(
            jwt_token=jwt_token,
            operation_name="getEmailAccountTagsAndClientById",
            query="""
                query getEmailAccountTagsAndClientById($id: Int!) {
                  email_accounts_by_pk(id: $id) {
                    email_account_tag_mappings {
                      tag {
                        id
                        name
                        color
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                }
            """,
            variables={"id": int(account_id)},
        )
        mappings = (
            ((payload.get("data") or {}).get("email_accounts_by_pk") or {}).get("email_account_tag_mappings") or []
            if isinstance(payload, dict)
            else []
        )
        normalized_expected = str(expected_tag_name or "").strip().lower()
        for row in mappings:
            if not isinstance(row, dict):
                continue
            tag = row.get("tag") or {}
            if str((tag or {}).get("name") or "").strip().lower() == normalized_expected:
                return True
        return False

    def _smartlead_fetch_account(self, *, api_key: str, email: str) -> Optional[Dict[str, Any]]:
        normalized = self._normalize_email(email)
        if not normalized:
            return None

        for params in ({"api_key": api_key, "username": normalized}, {"api_key": api_key, "limit": 100}):
            try:
                response = requests.get(
                    "https://server.smartlead.ai/api/v1/email-accounts/",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    params=params,
                    timeout=self.timeout_seconds,
                )
                if not response.ok:
                    continue
                data = self._json_or_empty(response)
                rows: List[Dict[str, Any]] = []
                if isinstance(data, list):
                    rows = [row for row in data if isinstance(row, dict)]
                elif isinstance(data, dict) and isinstance(data.get("data"), list):
                    rows = [row for row in data.get("data") or [] if isinstance(row, dict)]
                for row in rows:
                    if self._normalize_email(row.get("username") or row.get("email")) == normalized:
                        return row
            except Exception:
                continue
        return None

    def _instantly_init_oauth_session(self, api_key: str) -> Tuple[str, str]:
        response = requests.post(
            "https://api.instantly.ai/api/v2/oauth/google/init",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={},
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            raise RuntimeError(f"Instantly OAuth init failed: {self._response_error(response)}")
        payload = self._json_or_empty(response)
        auth_url = str((payload or {}).get("auth_url") or "").strip()
        session_id = str((payload or {}).get("session_id") or "").strip()
        if not auth_url or not session_id:
            raise RuntimeError(f"Instantly OAuth init returned missing auth_url/session_id: {payload}")
        return auth_url, session_id

    def _poll_instantly_oauth_status(self, api_key: str, session_id: str) -> Tuple[bool, str]:
        attempts = max(1, int(os.getenv("INSTANTLY_OAUTH_STATUS_ATTEMPTS", "25")))
        interval_seconds = max(1.0, float(os.getenv("INSTANTLY_OAUTH_STATUS_INTERVAL_SECONDS", "3")))
        last_error = ""
        for _ in range(attempts):
            try:
                response = requests.get(
                    f"https://api.instantly.ai/api/v2/oauth/session/status/{session_id}",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    timeout=self.timeout_seconds,
                )
                payload = self._json_or_empty(response)
                if not response.ok:
                    last_error = self._response_error(response)
                else:
                    status = str((payload or {}).get("status") or "").strip().lower()
                    if status == "success":
                        return True, ""
                    if status in {"error", "expired"}:
                        detail = str((payload or {}).get("error_description") or (payload or {}).get("error") or "").strip()
                        return False, detail or status
                    last_error = str(payload or "")
            except Exception as exc:
                last_error = str(exc)
            time.sleep(interval_seconds)
        return False, last_error or "Instantly OAuth status timed out"

    def _smartlead_login(self, page: Any, username: str, password: str) -> None:
        attempts = max(1, int(os.getenv("SMARTLEAD_LOGIN_ATTEMPTS", "3")))
        last_error = "Unknown Smartlead login error"

        email_selectors = [
            "input[name='email']",
            "input[type='email']",
            "input[aria-label='Email Address']",
            "//input[@aria-label='Email Address']",
        ]
        password_selectors = [
            "input[name='password']",
            "input[type='password']",
            "input[aria-label='Enter Password']",
            "//input[@aria-label='Enter Password']",
        ]
        login_button_selectors = [
            "button:has-text('Login')",
            "//span[normalize-space()='Login']",
            "//button[normalize-space()='Login']",
            "//button[contains(.,'Login')]",
        ]

        for attempt in range(1, attempts + 1):
            try:
                # Use "commit" so the goto returns as soon as the server
                # responds — Smartlead's slow CDN often exceeds the
                # domcontentloaded timeout. The element wait below handles
                # the actual SPA render.
                page.goto("https://app.smartlead.ai/login", wait_until="commit")

                # Smartlead's Quasar SPA can redirect to /app if already
                # logged in, or it renders the login form. Both happen after
                # JS bundles load. Wait for the page to settle, then decide
                # which state we're in by waiting for a concrete element.
                self._wait_for_smartlead_login_or_app(page)

                current_url = str(getattr(page, "url", "") or "").lower()
                if "app.smartlead.ai/app" in current_url:
                    return

                # At this point the login form is confirmed visible.
                self._fill_any(
                    page,
                    email_selectors,
                    username,
                    timeout_ms=15_000,
                )
                self._fill_any(
                    page,
                    password_selectors,
                    password,
                    timeout_ms=15_000,
                )
                self._click_any(
                    page,
                    login_button_selectors,
                    timeout_ms=20_000,
                )

                # After clicking login, wait for the form to disappear — this
                # is more reliable than URL polling because the SPA redirect
                # can lag behind the form teardown.
                if self._wait_for_smartlead_login_form_gone(page, email_selectors, timeout_seconds=60):
                    return
                if self._exists(page, email_selectors, timeout_ms=2_500):
                    raise RuntimeError("Smartlead login failed: still on login form after submit")
                raise RuntimeError(f"Smartlead login did not reach app URL. url={getattr(page, 'url', '')}")
            except Exception as exc:
                last_error = str(exc)
                if attempt >= attempts:
                    break
                logger.warning(
                    "[SendingToolUploader:Smartlead] Login attempt %s/%s failed: %s",
                    attempt,
                    attempts,
                    last_error,
                )
                time.sleep(2.0 * attempt)

        excerpt = ""
        try:
            excerpt = (page.locator("body").inner_text(timeout=3_000) or "").strip()[:600]
        except Exception:
            excerpt = ""
        if excerpt:
            raise RuntimeError(f"{last_error} | smartlead_login_page_excerpt={excerpt}")
        raise RuntimeError(last_error)

    def _wait_for_smartlead_login_or_app(self, page: Any) -> None:
        """Block until either the login form renders or the page redirects to /app.

        Smartlead's Quasar SPA takes 15-30s to hydrate in headless Chrome.
        Instead of guessing with sleeps, we ask Playwright to wait for a
        concrete selector that proves the SPA has rendered.
        """
        timeout_ms = max(
            10_000,
            int(os.getenv("SMARTLEAD_SPA_RENDER_TIMEOUT_MS", "60000")),
        )
        # Use Playwright's built-in selector wait: first matching selector wins.
        # "input[name='email']" = login form rendered.
        # CSS selector for an element only present inside /app (sidebar nav).
        app_or_login = (
            "input[name='email'],"
            "input[type='email'],"
            "nav,"
            "[class*='sidebar'],"
            "[class*='complementary']"
        )
        try:
            page.locator(app_or_login).first.wait_for(
                state="visible", timeout=timeout_ms,
            )
        except Exception:
            # Timeout is not fatal — fall through to the URL check / fill_any
            # which will produce a clearer error if the page is truly stuck.
            pass

    def _wait_for_smartlead_login_form_gone(
        self,
        page: Any,
        email_selectors: Sequence[str],
        timeout_seconds: float,
    ) -> bool:
        """Wait for the login form to disappear and the app to load.

        Returns True once the page URL reaches /app or the login email input
        is gone (meaning the SPA tore down the form after a successful login).
        """
        end_at = time.time() + max(1.0, timeout_seconds)
        while time.time() < end_at:
            url = str(getattr(page, "url", "") or "").lower()
            if "app.smartlead.ai/app" in url:
                return True
            # If the email field is gone, the login form was replaced — the
            # SPA is transitioning to the app even if the URL hasn't updated.
            if not self._exists(page, email_selectors, timeout_ms=800):
                # Give the SPA a moment to finish the route transition.
                time.sleep(1.5)
                url = str(getattr(page, "url", "") or "").lower()
                if "app.smartlead.ai/app" in url:
                    return True
                # Form is gone but URL still not at /app — could be a loading
                # spinner. Wait a bit longer before declaring success.
                if not self._exists(page, email_selectors, timeout_ms=1_500):
                    return True
            time.sleep(0.5)
        return False

    def _start_smartlead_google_oauth_for_user(
        self,
        *,
        page: Any,
        context: Any,
        email: str,
    ) -> Tuple[Any, bool]:
        page.goto("https://app.smartlead.ai/app/email-accounts/emails", wait_until="commit")

        # Wait for the email-accounts page to render — look for either
        # the "Connect Mailbox" button or an existing email row.
        add_account_selectors = self._smartlead_add_account_selectors()
        connect_or_email_selectors = add_account_selectors + [
            "text='Connect Mailbox'",
            "text='Add Account(s)'",
            "button:has-text('Connect Mailbox')",
            "button:has-text('Add Account')",
            f"//div[@data-identifier='{email}']",
            f"//p[contains(text(), '{email}')]",
        ]
        spa_timeout = max(10_000, int(os.getenv("SMARTLEAD_SPA_RENDER_TIMEOUT_MS", "60000")))
        try:
            self._combined_locator(page, connect_or_email_selectors).first.wait_for(
                state="visible", timeout=spa_timeout
            )
        except Exception:
            pass

        self._close_smartlead_popups(page)

        if self._exists(
            page,
            [
                f"//div[@data-identifier='{email}']",
                f"//p[contains(text(), '{email}')]",
                f"//div[normalize-space()='{email}']",
            ],
            timeout_ms=2_000,
        ):
            return page, True

        try:
            self._click_smartlead_add_account_entry(page, timeout_ms=20_000)
        except RuntimeError:
            # Capture page state for debugging before re-raising
            page_url = str(getattr(page, "url", "") or "")
            try:
                page_text = page.inner_text("body")[:600]
            except Exception:
                page_text = "(could not capture page text)"
            logger.warning(
                "[SendingToolUploader:Smartlead] Connect Mailbox not found. url=%s page_text=%s",
                page_url,
                page_text,
            )
            raise
        time.sleep(1.0)

        self._click_any(
            page,
            [
                "text='Email Account'",
                "text='Email Accounts'",
                "text='Add Email Account'",
                "text='Connect Email Account'",
                "text='Connect Mailbox'",
                "button:has-text('Email Account')",
                "button:has-text('Add Email Account')",
                "button:has-text('Connect Email Account')",
                "button:has-text('Connect Mailbox')",
                "//span[normalize-space()='Email Account']",
                "//span[normalize-space()='Add Email Account']",
                "//span[normalize-space()='Connect Email Account']",
                "//span[normalize-space()='Connect Mailbox']",
            ],
            timeout_ms=5_000,
            optional=True,
        )

        self._click_any(
            page,
            [
                "text='Smartlead Infrastructure'",
                "//div[contains(@class,'infrastructure-card') and contains(@class,'smartlead-infrastructure')]",
            ],
            timeout_ms=20_000,
            optional=True,
        )
        try:
            self._click_any(
                page,
                [
                    "text='Google OAuth'",
                    "//p[normalize-space()='Google OAuth']",
                    "//div[normalize-space()='Google OAuth']",
                ],
                timeout_ms=20_000,
            )
        except RuntimeError:
            page_url = str(getattr(page, "url", "") or "")
            try:
                page_text = page.inner_text("body")[:600]
            except Exception:
                page_text = "(could not capture page text)"
            logger.warning(
                "[SendingToolUploader:Smartlead] Google OAuth not found. url=%s page_text=%s",
                page_url,
                page_text,
            )
            raise
        before_pages = list(context.pages)
        self._click_any(
            page,
            [
                "text='Connect Account'",
                "button:has-text('Connect Account')",
                "//span[text()='Connect Account']",
                "//button[contains(.,'Connect Account')]",
            ],
            timeout_ms=20_000,
        )
        oauth_page = self._detect_oauth_page(context=context, fallback_page=page, previous_pages=before_pages)
        return oauth_page, False

    @staticmethod
    def _smartlead_add_account_selectors() -> List[str]:
        return [
            "text='Connect Mailbox'",
            "text='Add Account(s)'",
            "button:has-text('Connect Mailbox')",
            "button:has-text('Add Account(s)')",
            "button:has-text('Add Account')",
            "button:has-text('Add')",
            "[role='button']:has-text('Connect Mailbox')",
            "[role='button']:has-text('Add Account(s)')",
            "[role='button']:has-text('Add Account')",
            "[role='button']:has-text('Add')",
            "//button[.//span[normalize-space()='Connect Mailbox'] or normalize-space()='Connect Mailbox']",
            "//button[.//span[normalize-space()='Add Account(s)'] or normalize-space()='Add Account(s)']",
            "//button[.//span[normalize-space()='Add Account'] or normalize-space()='Add Account']",
            "//button[.//span[normalize-space()='Add'] or normalize-space()='Add']",
            "//*[@role='button' and (.//span[normalize-space()='Connect Mailbox'] or normalize-space()='Connect Mailbox')]",
            "//*[@role='button' and (.//span[normalize-space()='Add Account(s)'] or normalize-space()='Add Account(s)')]",
            "//*[@role='button' and (.//span[normalize-space()='Add Account'] or normalize-space()='Add Account')]",
            "//*[@role='button' and (.//span[normalize-space()='Add'] or normalize-space()='Add')]",
        ]

    def _click_smartlead_add_account_entry(self, page: Any, *, timeout_ms: int = 20_000) -> None:
        role_pattern = re.compile(r"^(connect mailbox|add account\(s\)|add account|add)$", re.I)
        try:
            button = page.get_by_role("button", name=role_pattern).first
            button.wait_for(state="visible", timeout=timeout_ms)
            button.click(timeout=timeout_ms)
            return
        except Exception:
            pass

        selectors = self._smartlead_add_account_selectors()
        if self._click_any(page, selectors, timeout_ms=timeout_ms, optional=True):
            return

        candidates = self._visible_control_texts(page)
        raise RuntimeError(f"Could not click Smartlead add-account entry. Visible controls: {candidates}")

    def _visible_control_texts(self, page: Any, *, limit: int = 30) -> List[str]:
        try:
            values = page.locator("button, [role='button'], .q-btn").evaluate_all(
                """els => els
                    .filter(el => {
                        const style = window.getComputedStyle(el);
                        const rect = el.getBoundingClientRect();
                        return style && style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
                    })
                    .map(el => (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim())
                    .filter(Boolean)
                    .slice(0, 30)
                """
            )
            return [str(value)[:120] for value in values[:limit]]
        except Exception:
            return []

    def _finalize_smartlead_connection(self, *, page: Any, context: Any, oauth_page: Any) -> None:
        try:
            if oauth_page is not None and oauth_page != page:
                if not oauth_page.is_closed():
                    oauth_page.close()
        except Exception:
            pass
        try:
            page.bring_to_front()
        except Exception:
            pass
        time.sleep(1.0)
        self._click_any(
            page,
            [
                "text='OK'",
                "button:has-text('OK')",
                "//span[text()='OK']",
                "//button[contains(.,'OK')]",
            ],
            timeout_ms=5_000,
            optional=True,
        )
        self._close_smartlead_popups(page)
        self._click_any(
            page,
            [
                "text='Save'",
                "button:has-text('Save')",
                "//span[text()='Save']",
                "//button[contains(.,'Save')]",
            ],
            timeout_ms=20_000,
            optional=True,
        )
        time.sleep(1.0)
        self._close_non_primary_pages(context, page)

    def _complete_google_signin_and_consent(
        self,
        *,
        page: Any,
        context: Any,
        email: str,
        password: str,
        onepassword: Any,
        otp_secret: str = "",
    ) -> None:
        clean_email = self._normalize_email(email)
        max_password_submissions = max(
            1,
            int(os.getenv("GOOGLE_OAUTH_MAX_PASSWORD_SUBMISSIONS", "2")),
        )
        password_submissions = 0
        self._wait_for_google_oauth_state(page)
        for _ in range(24):
            try:
                page.wait_for_load_state("domcontentloaded", timeout=2_000)
            except Exception:
                pass

            if self._is_google_consent_page(page, expected_email=clean_email):
                break
            if not self._is_google_signin_prompt(page):
                self._wait_for_google_oauth_state(page, timeout_seconds=2)
                if self._is_google_signin_prompt(page) or self._is_google_consent_page(page, expected_email=clean_email):
                    continue
                break

            # TOTP field FIRST — Google reuses account-identifier containers on
            # the TOTP page, so chooser selectors must never run before this.
            totp_selectors = [
                "input[name='totpPin']",
                "input[type='tel']",
                "input[inputmode='numeric']",
                "input[autocomplete='one-time-code']",
                "input[aria-label*='code']",
                "input[aria-label*='Code']",
            ]
            if self._exists(page, totp_selectors, timeout_ms=2_500):
                code = self._get_totp_code(onepassword, clean_email) or self._totp_from_secret(otp_secret)
                if not code:
                    raise RuntimeError(f"Google requested TOTP for {clean_email} but no 2FA code was available")
                self._fill_any(page, totp_selectors, code, timeout_ms=15_000)
                self._click_any(
                    page,
                    ["#totpNext", "//span[text()='Next']", "//span[text()='Verify']", "//button//span[text()='Next']"],
                    timeout_ms=10_000,
                    optional=True,
                )
                time.sleep(1.0)
                continue

            # Check for password field FIRST — the password page also contains
            # a div[@data-identifier] with the email, so the account-selector
            # click below would match and loop forever without entering the password.
            if self._exists(page, ["input[name='Passwd']", "input[type='password']"], timeout_ms=2_500):
                self._fill_any(
                    page,
                    [
                        "input[name='Passwd']",
                        "input[type='password']",
                        "input[aria-label*='password' i]",
                    ],
                    password,
                    timeout_ms=20_000,
                )
                self._click_any(
                    page,
                    ["#passwordNext", "//*[@id='passwordNext']", "//span[text()='Next']"],
                    timeout_ms=15_000,
                )
                password_submissions += 1
                time.sleep(2.2)

                # If Google keeps the same password challenge screen, fail fast to avoid lockouts.
                if self._exists(page, ["input[name='Passwd']", "input[type='password']"], timeout_ms=1_000):
                    challenge_error = self._read_google_password_challenge_error(page)
                    if challenge_error:
                        raise RuntimeError(
                            f"Google password challenge rejected for {clean_email}: {challenge_error}"
                        )
                    if password_submissions >= max_password_submissions:
                        raise RuntimeError(
                            f"Google password challenge did not advance for {clean_email} after "
                            f"{password_submissions} submission(s); stopping to avoid account lockout"
                        )
                continue

            # Email field (identifier page)
            if self._exists(page, ["input#identifierId", "input[type='email']"], timeout_ms=2_500):
                self._fill_any(page, ["input#identifierId", "input[type='email']"], clean_email, timeout_ms=20_000)
                self._click_any(
                    page,
                    ["#identifierNext", "//*[@id='identifierNext']", "//span[text()='Next']"],
                    timeout_ms=15_000,
                )
                time.sleep(1.0)
                continue

            # Account selector (choose-account page) — AFTER all input-field
            # checks, because password/TOTP pages also contain data-identifier.
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
                time.sleep(1.0)
                continue

            # "Use another account" link
            self._click_any(
                page,
                [
                    "//div[text()='Use another account']",
                    "//button//span[text()='Use another account']",
                ],
                timeout_ms=2_500,
                optional=True,
            )

            # Fallback: "Try another way" → "Enter your password"
            self._click_any(
                page,
                [
                    "//span[text()='Try another way']",
                    "//button//span[text()='Try another way']",
                    "//span[contains(text(),'Try another')]",
                ],
                timeout_ms=2_500,
                optional=True,
            )
            self._click_any(
                page,
                [
                    "//span[contains(text(),'Enter your password')]",
                    "//span[contains(text(),'Use your password')]",
                    "//div[contains(text(),'Enter your password')]",
                ],
                timeout_ms=3_000,
                optional=True,
            )

            time.sleep(0.8)

        # Wait for the consent page to fully load after the signin loop exits.
        try:
            page.wait_for_load_state("domcontentloaded", timeout=8_000)
        except Exception:
            pass
        time.sleep(1.5)

        self._complete_google_consent(page, expected_email=clean_email)

        # Give the redirect back to the sending tool time to complete.
        try:
            page.wait_for_load_state("domcontentloaded", timeout=8_000)
        except Exception:
            pass

        if self._is_google_signin_prompt(page):
            raise RuntimeError(
                f"Google OAuth sign-in did not complete for {clean_email}. Current URL: {str(getattr(page, 'url', '') or '')}"
            )
        self._close_non_primary_pages(context, page)

    def _wait_for_google_oauth_state(self, page: Any, *, timeout_seconds: float = 25) -> None:
        end_at = time.time() + max(1.0, timeout_seconds)
        while time.time() < end_at:
            url = str(getattr(page, "url", "") or "").lower()
            if self._is_sending_tool_oauth_callback_url(url):
                return
            if "accounts.google.com" in url and (
                self._exists(
                    page,
                    [
                        "input[name='totpPin']",
                        "input[type='tel']",
                        "input[autocomplete='one-time-code']",
                        "input[name='Passwd']",
                        "input[type='password']",
                        "input#identifierId",
                        "input[type='email']",
                        "button:has-text('Continue')",
                        "button:has-text('Allow')",
                        "//*[contains(normalize-space(),'Choose an account')]",
                    ],
                    timeout_ms=800,
                )
                or self._is_google_consent_page(page)
            ):
                return
            time.sleep(0.4)

    def _read_google_password_challenge_error(self, page: Any) -> str:
        checks = [
            "Too many failed attempts",
            "Wrong password",
            "Incorrect password",
            "Couldn’t verify",
            "Could not verify",
            "Try again",
        ]
        for label in checks:
            if self._exists(
                page,
                [
                    f"//*[contains(normalize-space(),\"{label}\")]",
                    f"//div[contains(normalize-space(),\"{label}\")]",
                    f"//span[contains(normalize-space(),\"{label}\")]",
                ],
                timeout_ms=800,
            ):
                return label
        return ""

    def _complete_google_consent(self, page: Any, *, expected_email: str = "") -> None:
        for attempt in range(8):
            clicked = False

            self._assert_google_oauth_account(page, expected_email=expected_email)

            # Use Playwright role/text selectors first (most reliable), then XPath fallbacks.
            for label in ("Select all", "Continue", "Allow", "I understand"):
                try:
                    btn = page.get_by_role("button", name=label, exact=True)
                    if btn.count() > 0:
                        btn.first.click(timeout=5_000)
                        clicked = True
                        time.sleep(1.0)
                        continue
                except Exception:
                    pass
                # XPath fallback
                try:
                    loc = page.locator(f"xpath=//span[text()='{label}']").first
                    loc.wait_for(state="visible", timeout=2_000)
                    loc.click(timeout=3_000)
                    clicked = True
                    time.sleep(1.0)
                except Exception:
                    pass

            if not clicked:
                break
            time.sleep(0.8)

    def _is_google_consent_page(self, page: Any, *, expected_email: str = "") -> bool:
        url = str(getattr(page, "url", "") or "").lower()
        if "accounts.google.com" not in url:
            return False
        path = self._url_path(url)
        if "/consent" in path or "/signin/oauth" in path:
            if self._exists(page, ["input#identifierId", "input[name='Passwd']"], timeout_ms=500):
                return False
            if self._exists(
                page,
                [
                    "button:has-text('Continue')",
                    "button:has-text('Allow')",
                    "//*[contains(normalize-space(),'Select all')]",
                    "//*[contains(normalize-space(),'Google will allow')]",
                    "//*[contains(normalize-space(),'wants access')]",
                    "//*[contains(normalize-space(),'Sign in to')]",
                ],
                timeout_ms=1_000,
            ):
                if expected_email:
                    self._assert_google_oauth_account(page, expected_email=expected_email)
                return True
        return False

    def _assert_google_oauth_account(self, page: Any, *, expected_email: str = "") -> None:
        clean_email = self._normalize_email(expected_email)
        if not clean_email:
            return
        body = self._safe_body_excerpt(page, limit=20_000).lower()
        if not body:
            return
        if clean_email in body:
            return
        if "accounts.google.com" not in str(getattr(page, "url", "") or "").lower():
            return
        consent_markers = (
            "google will allow",
            "wants access",
            "sign in to",
            "choose an account",
            "continue to",
        )
        if not any(marker in body for marker in consent_markers):
            return
        visible_emails = sorted(set(re.findall(r"[\w.+%-]+@[\w.-]+\.[a-z]{2,}", body, flags=re.I)))
        if visible_emails:
            raise RuntimeError(
                f"Google OAuth is showing a different account. Expected {clean_email}; visible accounts: {', '.join(visible_emails[:5])}"
            )

    @staticmethod
    def _totp_from_secret(secret: str) -> str:
        clean_secret = str(secret or "").strip().replace(" ", "")
        if not clean_secret:
            return ""
        try:
            import pyotp

            return str(pyotp.TOTP(clean_secret).now() or "").strip()
        except Exception as exc:
            logger.warning("[SendingToolUploader] Failed to generate TOTP from stored secret: %s", exc)
            return ""

    def _get_totp_code(self, onepassword: Any, email: str) -> str:
        if onepassword is None:
            return ""
        try:
            item = onepassword.find_google_login_item(email)
            if not item:
                return ""
            item_id = str(item.get("id") or "").strip()
            if not item_id:
                return ""
            return str(onepassword.get_totp(item_id) or "").strip()
        except Exception as exc:
            logger.warning("[SendingToolUploader] Failed to get 1Password TOTP for %s: %s", email, exc)
            return ""

    def _is_google_signin_prompt(self, page: Any) -> bool:
        url = str(getattr(page, "url", "") or "").lower()
        if self._is_sending_tool_oauth_callback_url(url):
            return False
        if self._is_google_consent_page(page):
            return False
        if "accounts.google.com" in url:
            # The consent page is handled by _complete_google_consent, not the signin loop.
            # Check only the path (before '?') — the query string often contains a
            # "continue=…/consent…" parameter that would cause a false match.
            path = self._url_path(url)
            if "/consent" in path or "/signin/oauth" in path:
                return False
            return True
        return self._exists(
            page,
            [
                "input#identifierId",
                "input[type='email']",
                "input[name='Passwd']",
                "//h1[contains(normalize-space(),'Sign in')]",
                "//*[contains(normalize-space(),'Use your Google Account')]",
                "//*[contains(normalize-space(),'Choose an account')]",
            ],
            timeout_ms=1_200,
        )

    @staticmethod
    def _is_sending_tool_oauth_callback_url(url: str) -> bool:
        lowered = str(url or "").lower()
        actual_page = SendingToolUploader._url_path(lowered)
        callback_markers = [
            "iapi.instantly.ai/oauth/google/redirect",
            "app.instantly.ai/oauth",
            "api.instantly.ai/oauth",
            "smartlead.ai/oauth",
            "sender-email-connect/google-callback",
            "sender-emails",
        ]
        return any(marker in actual_page for marker in callback_markers)

    @staticmethod
    def _url_path(url: str) -> str:
        return str(url or "").split("?", 1)[0]

    def _detect_oauth_page(self, *, context: Any, fallback_page: Any, previous_pages: Sequence[Any]) -> Any:
        previous_ids = {id(p) for p in previous_pages}
        end_at = time.time() + 8
        while time.time() < end_at:
            for p in context.pages:
                if id(p) not in previous_ids:
                    try:
                        p.wait_for_load_state("domcontentloaded", timeout=5_000)
                    except Exception:
                        pass
                    return p
            url = str(getattr(fallback_page, "url", "") or "").lower()
            if "accounts.google.com" in url:
                return fallback_page
            time.sleep(0.3)
        return fallback_page

    def _close_non_primary_pages(self, context: Any, primary_page: Any) -> None:
        for p in list(context.pages):
            if p == primary_page:
                continue
            try:
                p.close()
            except Exception:
                pass
        try:
            primary_page.bring_to_front()
        except Exception:
            pass

    def _close_smartlead_popups(self, page: Any) -> None:
        self._click_any(
            page,
            [
                "//span[text()='Close']",
                "//i[text()='close']",
                "//div[contains(@class,'gleap-notification-close')]",
            ],
            timeout_ms=2_500,
            optional=True,
        )

    def _create_oauth_context_page(
        self,
        browser: Any,
        *,
        storage_state: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Any, Any]:
        kwargs: Dict[str, Any] = {
            "viewport": {"width": 1720, "height": 980},
            "locale": "en-US",
        }
        if storage_state:
            kwargs["storage_state"] = storage_state
        context = browser.new_context(**kwargs)
        page = context.new_page()
        page.set_default_timeout(30_000)
        page.set_default_navigation_timeout(60_000)
        return context, page

    def _close_oauth_context_page(self, *, context: Any, page: Any) -> None:
        try:
            if page is not None:
                page.close()
        except Exception:
            pass
        try:
            if context is not None:
                context.close()
        except Exception:
            pass

    def _extract_smartlead_storage_state(self, context: Any) -> Dict[str, Any]:
        try:
            raw = context.storage_state() or {}
        except Exception:
            return {"cookies": [], "origins": []}

        cookies = []
        for row in raw.get("cookies") or []:
            if not isinstance(row, dict):
                continue
            domain = str(row.get("domain") or "").strip().lower()
            if "smartlead.ai" not in domain:
                continue
            cookies.append(row)

        origins = []
        for row in raw.get("origins") or []:
            if not isinstance(row, dict):
                continue
            origin = str(row.get("origin") or "").strip().lower()
            if "smartlead.ai" not in origin:
                continue
            origins.append(row)

        return {"cookies": cookies, "origins": origins}

    def _clear_google_session_state(self, context: Any) -> None:
        # Remove Google cookies while preserving Smartlead session cookies.
        try:
            cookies = context.cookies()
            for cookie in cookies:
                if not isinstance(cookie, dict):
                    continue
                domain = str(cookie.get("domain") or "").strip().lower()
                if (
                    "google." not in domain
                    and "googleusercontent.com" not in domain
                    and "gstatic.com" not in domain
                ):
                    continue
                try:
                    context.clear_cookies(
                        name=str(cookie.get("name") or ""),
                        domain=str(cookie.get("domain") or ""),
                        path=str(cookie.get("path") or "/"),
                    )
                except Exception:
                    continue
        except Exception:
            pass

        # Best-effort storage clear on Google origins.
        cleanup_page = None
        try:
            cleanup_page = context.new_page()
            for url in ("https://accounts.google.com", "https://www.google.com"):
                try:
                    cleanup_page.goto(url, wait_until="domcontentloaded", timeout=12_000)
                    cleanup_page.evaluate(
                        "() => { try { localStorage.clear(); } catch (_) {} try { sessionStorage.clear(); } catch (_) {} }"
                    )
                except Exception:
                    continue
        except Exception:
            pass
        finally:
            try:
                if cleanup_page is not None:
                    cleanup_page.close()
            except Exception:
                pass

    def _wait_for_url_contains(self, page: Any, needles: Sequence[str], timeout_seconds: float) -> bool:
        lowered = [str(v or "").strip().lower() for v in needles if str(v or "").strip()]
        end_at = time.time() + max(1.0, timeout_seconds)
        while time.time() < end_at:
            url = str(getattr(page, "url", "") or "").lower()
            if any(n in url for n in lowered):
                return True
            time.sleep(0.4)
        return False

    def _selector(self, raw: str) -> str:
        selector = str(raw or "").strip()
        return f"xpath={selector}" if selector.startswith("//") else selector

    def _combined_locator(self, page: Any, selectors: Sequence[str]) -> Any:
        """Build a single Playwright locator that matches ANY of *selectors*.

        Uses Playwright's `locator.or_()` combinator so that a single
        `wait_for()` call watches all selectors in parallel — the full
        timeout applies to the combined set, not split across each one.
        `.first` is NOT applied here — callers should call `.first` on
        the returned locator when they need a single-element handle.
        """
        combined = page.locator(self._selector(selectors[0]))
        for sel in selectors[1:]:
            combined = combined.or_(page.locator(self._selector(sel)))
        return combined

    def _exists(self, page: Any, selectors: Sequence[str], timeout_ms: int = 1_500) -> bool:
        if not selectors:
            return False
        try:
            self._combined_locator(page, selectors).first.wait_for(
                state="visible", timeout=timeout_ms
            )
            return True
        except Exception:
            return False

    def _click_any(
        self,
        page: Any,
        selectors: Sequence[str],
        *,
        timeout_ms: int = 20_000,
        optional: bool = False,
    ) -> bool:
        if not selectors:
            return False
        try:
            loc = self._combined_locator(page, selectors).first
            loc.wait_for(state="visible", timeout=timeout_ms)
            loc.click(timeout=timeout_ms)
            return True
        except Exception:
            if optional:
                return False
            raise RuntimeError(f"Could not click any selector: {list(selectors)}")

    def _fill_any(
        self,
        page: Any,
        selectors: Sequence[str],
        value: str,
        *,
        timeout_ms: int = 20_000,
    ) -> None:
        try:
            loc = self._combined_locator(page, selectors).first
            loc.wait_for(state="visible", timeout=timeout_ms)
            loc.fill(value, timeout=timeout_ms)
        except Exception:
            raise RuntimeError(f"Could not fill any selector: {list(selectors)}")

    @staticmethod
    def _safe_body_excerpt(page: Any, *, limit: int = 1000) -> str:
        try:
            text = page.locator("body").inner_text(timeout=2_000)
            return str(text or "")[: max(0, limit)]
        except Exception:
            return ""

    @staticmethod
    def _contains_normalized(haystack: str, needle: str) -> bool:
        def norm(value: str) -> str:
            return re.sub(r"\s+", " ", str(value or "").strip().lower())

        return norm(needle) in norm(haystack)

    @staticmethod
    def _xpath_literal(value: str) -> str:
        text = str(value or "")
        if "'" not in text:
            return f"'{text}'"
        if '"' not in text:
            return f'"{text}"'
        parts = text.split("'")
        return "concat(" + ", \"'\", ".join(f"'{part}'" for part in parts) + ")"

    def _build_result(
        self,
        *,
        tool: str,
        inboxes: List[Dict[str, Any]],
        present: Set[str],
        provisional_errors: Dict[str, str],
        missing_error_prefix: str,
        total_candidates: int,
    ) -> Dict[str, Any]:
        uploaded_emails: List[str] = []
        failed_uploads: List[Dict[str, str]] = []
        seen_failed = set()
        for row in inboxes:
            email = self._normalize_email(row.get("email"))
            if not email:
                continue
            if email in present:
                uploaded_emails.append(email)
                continue
            if email in seen_failed:
                continue
            seen_failed.add(email)
            base = missing_error_prefix
            if provisional_errors.get(email):
                base = f"{base} ({provisional_errors[email]})"
            failed_uploads.append({"email": email, "error": base})
        return {
            "tool": tool,
            "total_candidates": total_candidates,
            "uploaded_emails": sorted(set(uploaded_emails)),
            "failed_uploads": failed_uploads,
            "skipped_already_uploaded": 0,
        }

    def _check_instantly_account(self, *, api_key: str, email: str) -> bool:
        try:
            response = requests.get(
                f"https://api.instantly.ai/api/v2/accounts/{email}",
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=self.timeout_seconds,
            )
            if response.status_code == 404:
                return False
            if not response.ok:
                logger.warning(
                    "[SendingToolUploader:Instantly] validation failed for %s (status=%s): %s",
                    email,
                    response.status_code,
                    self._response_error(response),
                )
                return False
            return True
        except Exception as exc:
            logger.warning("[SendingToolUploader:Instantly] validation request failed for %s: %s", email, exc)
            return False

    def _check_smartlead_account(self, *, api_key: str, email: str) -> bool:
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        try:
            response = requests.get(
                "https://server.smartlead.ai/api/v1/email-accounts/",
                params={"api_key": api_key, "username": email},
                headers=headers,
                timeout=self.timeout_seconds,
            )
            if response.status_code == 404:
                return False
            if response.ok:
                payload = self._json_or_empty(response)
                if isinstance(payload, list):
                    return any(self._normalize_email(row.get("username") or row.get("email")) == email for row in payload)
                if isinstance(payload, dict):
                    rows = payload.get("data")
                    if isinstance(rows, list):
                        return any(self._normalize_email(row.get("username") or row.get("email")) == email for row in rows)
            else:
                logger.warning(
                    "[SendingToolUploader:Smartlead] direct validation failed for %s (status=%s): %s",
                    email,
                    response.status_code,
                    self._response_error(response),
                )
        except Exception as exc:
            logger.warning("[SendingToolUploader:Smartlead] direct validation request failed for %s: %s", email, exc)

        try:
            response = requests.get(
                "https://server.smartlead.ai/api/v1/email-accounts/",
                params={"api_key": api_key, "limit": 100},
                headers=headers,
                timeout=self.timeout_seconds,
            )
            if response.status_code == 404:
                return False
            if not response.ok:
                logger.warning(
                    "[SendingToolUploader:Smartlead] list validation failed for %s (status=%s): %s",
                    email,
                    response.status_code,
                    self._response_error(response),
                )
                return False
            payload = self._json_or_empty(response)
            rows: List[Dict[str, Any]] = []
            if isinstance(payload, list):
                rows = [row for row in payload if isinstance(row, dict)]
            elif isinstance(payload, dict) and isinstance(payload.get("data"), list):
                rows = [row for row in payload["data"] if isinstance(row, dict)]
            return any(self._normalize_email(row.get("username") or row.get("email")) == email for row in rows)
        except Exception as exc:
            logger.warning("[SendingToolUploader:Smartlead] list validation request failed for %s: %s", email, exc)
            return False

    def _wait_for_smartlead_accounts(
        self,
        *,
        targets: Set[str],
        api_key: str,
        smartlead_username: str,
        smartlead_password: str,
        attempts: int,
        interval_seconds: float,
        concurrency: int,
    ) -> Set[str]:
        clean_targets = {self._normalize_email(email) for email in targets if self._normalize_email(email)}
        if not clean_targets:
            return set()

        present_via_api = self._wait_for_account_presence(
            targets=clean_targets,
            attempts=attempts,
            interval_seconds=interval_seconds,
            checker=lambda email: self._check_smartlead_account(api_key=api_key, email=email),
            concurrency=concurrency,
            tool_label="Smartlead",
        )
        unresolved = {email for email in clean_targets if email not in present_via_api}
        if not unresolved:
            return present_via_api

        enable_private_fallback = self._as_bool(
            os.getenv("SMARTLEAD_ENABLE_PRIVATE_VALIDATION_FALLBACK", "false"),
            default=False,
        )
        if not enable_private_fallback:
            return present_via_api

        if not smartlead_username or not smartlead_password:
            logger.warning(
                "[SendingToolUploader:Smartlead] API validation unresolved for %s/%s inbox(es), "
                "but private fallback is enabled without Smartlead UI credentials.",
                len(unresolved),
                len(clean_targets),
            )
            return present_via_api

        logger.warning(
            "[SendingToolUploader:Smartlead] API validation unresolved for %s/%s inbox(es); "
            "running private fallback because SMARTLEAD_ENABLE_PRIVATE_VALIDATION_FALLBACK=true.",
            len(unresolved),
            len(clean_targets),
        )
        try:
            private_present = self._wait_for_smartlead_private_accounts(
                targets=unresolved,
                username=smartlead_username,
                password=smartlead_password,
                attempts=attempts,
                interval_seconds=interval_seconds,
            )
            return set(present_via_api).union(private_present)
        except Exception as exc:
            logger.warning(
                "[SendingToolUploader:Smartlead] Private fallback failed after API validation: %s",
                exc,
            )
            return present_via_api

    def _wait_for_smartlead_private_accounts(
        self,
        *,
        targets: Set[str],
        username: str,
        password: str,
        attempts: int,
        interval_seconds: float,
    ) -> Set[str]:
        clean_targets = {self._normalize_email(email) for email in targets if self._normalize_email(email)}
        if not clean_targets:
            return set()

        present: Set[str] = set()
        unresolved = set(clean_targets)
        for attempt in range(1, attempts + 1):
            try:
                token = self._smartlead_private_login_token(username=username, password=password)
                for email in list(unresolved):
                    if self._check_smartlead_private_account(token=token, email=email):
                        present.add(email)
                        unresolved.discard(email)
            except Exception as exc:
                logger.warning(
                    "[SendingToolUploader:Smartlead] Private account list attempt %s failed: %s",
                    attempt,
                    exc,
                )

            if not unresolved:
                return present
            if attempt < attempts:
                time.sleep(interval_seconds)

        if unresolved:
            logger.warning(
                "[SendingToolUploader:Smartlead] Private validation exhausted with %s/%s found.",
                len(present),
                len(clean_targets),
            )
        return present

    def _smartlead_private_login_token(self, *, username: str, password: str) -> str:
        clean_user = str(username or "").strip()
        clean_pass = str(password or "").strip()
        if not clean_user or not clean_pass:
            raise RuntimeError("Smartlead private validation requires username/password")

        login_resp = requests.post(
            "https://server.smartlead.ai/api/auth/login",
            json={"email": clean_user, "password": clean_pass},
            timeout=self.timeout_seconds,
        )
        if not login_resp.ok:
            raise RuntimeError(
                f"Smartlead private login failed ({login_resp.status_code}): {self._response_error(login_resp)}"
            )
        login_payload = self._json_or_empty(login_resp)
        token = str((login_payload or {}).get("token") or "").strip()
        if not token:
            raise RuntimeError("Smartlead private login response missing token")
        return token

    def _check_smartlead_private_account(self, *, token: str, email: str) -> bool:
        clean_email = self._normalize_email(email)
        if not clean_email:
            return False

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        lookup = requests.get(
            "https://server.smartlead.ai/api/email-account/get-total-email-accounts",
            params={"offset": 0, "limit": 10, "searchString": clean_email},
            headers=headers,
            timeout=self.timeout_seconds,
        )
        if not lookup.ok:
            logger.warning(
                "[SendingToolUploader:Smartlead] Private lookup failed for %s (status=%s): %s",
                clean_email,
                lookup.status_code,
                self._response_error(lookup),
            )
            return False

        payload = self._json_or_empty(lookup)
        rows = []
        if isinstance(payload, dict):
            rows = (payload.get("data") or {}).get("email_accounts") or []
        if not isinstance(rows, list):
            rows = []

        account_id: Optional[str] = None
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_email = self._normalize_email(
                row.get("from_email") or row.get("email") or row.get("username")
            )
            if row_email != clean_email:
                continue
            candidate_id = str(row.get("id") or "").strip()
            if candidate_id:
                account_id = candidate_id
                break

        if not account_id:
            return False

        details = requests.get(
            f"https://server.smartlead.ai/api/email-account/fetch-warmup-details-by-email-account-id/{account_id}",
            headers=headers,
            timeout=self.timeout_seconds,
        )
        if not details.ok:
            logger.warning(
                "[SendingToolUploader:Smartlead] Private detail fetch failed for %s (id=%s, status=%s): %s",
                clean_email,
                account_id,
                details.status_code,
                self._response_error(details),
            )
            return False
        return True

    def _wait_for_account_presence(
        self,
        *,
        targets: Set[str],
        attempts: int,
        interval_seconds: float,
        checker,
        concurrency: int,
        tool_label: str,
    ) -> Set[str]:
        if not targets:
            return set()

        unresolved = list(targets)
        present: Set[str] = set()
        for attempt in range(1, attempts + 1):
            found_now = self._check_accounts_batch(unresolved, checker, concurrency)
            if found_now:
                present.update(found_now)
                unresolved = [email for email in unresolved if email not in found_now]
            if not unresolved:
                return present
            if attempt < attempts:
                time.sleep(interval_seconds)

        if unresolved:
            logger.warning(
                "[SendingToolUploader:%s] Validation exhausted with %s/%s found.",
                tool_label,
                len(present),
                len(targets),
            )
        return present

    def _check_accounts_batch(self, emails: List[str], checker, concurrency: int) -> Set[str]:
        if not emails:
            return set()
        pending = list(emails)
        found: Set[str] = set()
        worker_count = max(1, min(concurrency, len(pending)))
        import threading

        lock = threading.Lock()

        def worker() -> None:
            while True:
                with lock:
                    if not pending:
                        return
                    email = pending.pop()
                if checker(email):
                    with lock:
                        found.add(email)

        threads: List[threading.Thread] = []
        for _ in range(worker_count):
            thread = threading.Thread(target=worker)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        return found

    @staticmethod
    def _normalize_tool(value: str) -> str:
        text = str(value or "").strip().lower()
        if "instantly" in text:
            return "instantly.ai"
        if "smartlead" in text:
            return "smartlead.ai"
        if "amplemarket" in text or "ample market" in text:
            return "amplemarket"
        if "bison" in text:
            return "email-bison"
        return text

    @staticmethod
    def _normalize_email(value: Any) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def _json_or_empty(response: requests.Response) -> Any:
        try:
            return response.json()
        except Exception:
            return {}

    @staticmethod
    def _response_error(response: requests.Response) -> str:
        payload = SendingToolUploader._json_or_empty(response)
        if isinstance(payload, dict):
            message = payload.get("message") or payload.get("error")
            if message:
                return str(message)
        if isinstance(payload, list) and payload:
            return str(payload[0])
        return response.text or f"HTTP {response.status_code}"

    @staticmethod
    def _as_bool(value: Any, default: bool = False) -> bool:
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}
