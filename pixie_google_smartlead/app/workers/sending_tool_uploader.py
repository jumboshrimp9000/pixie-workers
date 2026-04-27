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
            and normalized_tool in {"instantly.ai", "smartlead.ai"}
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

    def _normalize_settings(self, settings: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        source = settings if isinstance(settings, dict) else {}
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
            "instantlyWarmup": as_dict(source.get("instantlyWarmup")),
            "smartleadWarmup": as_dict(source.get("smartleadWarmup")),
            "smartleadAccount": as_dict(source.get("smartleadAccount")),
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
        response = requests.get(
            "https://api.instantly.ai/api/v2/custom-tag-mappings",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            params={"limit": 100},
            timeout=self.timeout_seconds,
        )
        if not response.ok:
            return False
        data = self._json_or_empty(response)
        rows = []
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            rows = data.get("items") or []
        elif isinstance(data, list):
            rows = data
        normalized = self._normalize_email(email)
        for row in rows:
            if not isinstance(row, dict):
                continue
            if self._normalize_email(row.get("resource_id")) == normalized and str(row.get("tag_id") or "") == str(tag_id):
                return True
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
    ) -> None:
        clean_email = self._normalize_email(email)
        max_password_submissions = max(
            1,
            int(os.getenv("GOOGLE_OAUTH_MAX_PASSWORD_SUBMISSIONS", "2")),
        )
        password_submissions = 0
        for _ in range(14):
            if not self._is_google_signin_prompt(page):
                break

            # TOTP field FIRST — Google reuses account-identifier containers on
            # the TOTP page, so chooser selectors must never run before this.
            totp_selectors = [
                "input[name='totpPin']",
                "input[type='tel']",
                "input[inputmode='numeric']",
                "input[autocomplete='one-time-code']",
                "input[aria-label*='code']",
            ]
            if self._exists(page, totp_selectors, timeout_ms=2_500):
                code = self._get_totp_code(onepassword, clean_email)
                if not code:
                    raise RuntimeError(f"Google requested TOTP for {clean_email} but no 1Password code was available")
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
                    ["input[name='Passwd']", "input[type='password']"],
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

        self._complete_google_consent(page)

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

    def _complete_google_consent(self, page: Any) -> None:
        for attempt in range(6):
            clicked = False

            # Use Playwright role/text selectors first (most reliable), then XPath fallbacks.
            for label in ("Continue", "Allow", "I understand"):
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
        if "accounts.google.com" in url:
            # The consent page is handled by _complete_google_consent, not the signin loop.
            # Check only the path (before '?') — the query string often contains a
            # "continue=…/consent…" parameter that would cause a false match.
            path = self._url_path(url)
            if "/consent" in path:
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
