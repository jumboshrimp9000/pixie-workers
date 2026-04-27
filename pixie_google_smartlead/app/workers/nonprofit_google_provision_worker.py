import asyncio
import json
import logging
import os
import random
import string
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from playwright.async_api import async_playwright

from app import get_order_logger
from app.workers.google_admin_playwright import GoogleAdminPlaywrightClient, GoogleAdminUser
from app.workers.google_fulfillment_clients import CloudflareClient
from app.workers.nonprofit_google_admin_playwright import (
    enable_dkim_for_domain,
    setup_user_2fa,
    verify_domain_in_admin,
)
from app.workers.nonprofit_google_panel_client import NonprofitGooglePanelClient
from app.workers.onepassword_client import OnePasswordCliClient
from app.workers.sending_tool_uploader import SendingToolUploader
from app.workers.supabase_client import SupabaseRestClient


logger = logging.getLogger(__name__)


INTERIM_STATUSES = {
    "PANEL_ASSIGNED": "Free Google - Panel Assigned",
    "TXT_FETCHED": "Free Google - TXT Token Fetched",
    "TXT_WRITTEN": "Free Google - TXT Written",
    "DNS_PROPAGATING": "Free Google - Waiting for DNS propagation",
    "TXT_VISIBLE": "Free Google - DNS TXT Visible",
    "DOMAIN_VERIFIED": "Free Google - Domain Verified",
    "USERS_CREATED": "Free Google - Users Created",
    "DKIM_ENABLED": "Free Google - DKIM Enabled",
    "MFA_ENROLLMENT": "Free Google - 2FA Enrolling",
    "SENDING_TOOL_UPLOAD": "Free Google - Sending Tool Upload",
    "COMPLETE": "Free Google - Provisioning Complete",
    "FAILED": "Free Google - Failed",
}


class DeferredNonprofitAction(RuntimeError):
    def __init__(self, message: str, *, delay_seconds: float, consume_attempt: bool = False) -> None:
        super().__init__(message)
        self.delay_seconds = delay_seconds
        self.consume_attempt = consume_attempt


class NonprofitGoogleProvisionWorker:
    def __init__(self) -> None:
        self.client = SupabaseRestClient.from_env()
        self.poll_interval_seconds = max(1.0, float(os.getenv("NONPROFIT_GOOGLE_POLL_SECONDS", "10")))
        self.batch_size = max(1, int(os.getenv("NONPROFIT_GOOGLE_BATCH_SIZE", "3")))
        self.max_retries = max(1, int(os.getenv("NONPROFIT_GOOGLE_MAX_RETRIES", "8")))
        self.action_types = ["free_google_provision"]
        self.playwright_headless = self._as_bool(os.getenv("NONPROFIT_GOOGLE_PLAYWRIGHT_HEADLESS", "true"), default=True)
        self.require_mfa_enrollment = self._as_bool(
            os.getenv("NONPROFIT_GOOGLE_REQUIRE_MFA_ENROLLMENT", "true"),
            default=True,
        )
        self.dns_propagation_retry_seconds = max(
            60.0,
            float(os.getenv("NONPROFIT_GOOGLE_DNS_PROPAGATION_RETRY_SECONDS", "300")),
        )
        self.admin_vault = str(os.getenv("NONPROFIT_GOOGLE_ADMIN_OP_VAULT") or "").strip()
        self.user_vault = str(os.getenv("NONPROFIT_GOOGLE_USER_OP_VAULT") or "icje7jpscrdm6xtlcr252zxinq").strip()
        self._cloudflare: Optional[CloudflareClient] = None
        self._sending_tool_uploader = SendingToolUploader()

    def run_forever(self) -> None:
        logger.info(
            "Nonprofit Google provision worker started (types=%s, poll=%.1fs)",
            self.action_types,
            self.poll_interval_seconds,
        )
        while True:
            try:
                processed = self._poll_once()
                if processed == 0:
                    time.sleep(self.poll_interval_seconds)
            except Exception:
                logger.exception("Nonprofit provision poll failed")
                time.sleep(self.poll_interval_seconds)

    def _poll_once(self) -> int:
        actions = self.client.get_actions(self.action_types, limit=self.batch_size)
        if not actions:
            return 0
        processed = 0
        for action in actions:
            claimed = self.client.claim_action(action)
            if not claimed:
                continue
            processed += 1
            with self.client.action_lease_heartbeat(claimed):
                self._process_action(claimed)
        return processed

    def _process_action(self, action: Dict[str, Any]) -> None:
        action_id = str(action.get("id") or "")
        action_logger = get_order_logger(action_id)
        prior_result = action.get("result") or {}
        prior_steps_raw = prior_result.get("steps") if isinstance(prior_result, dict) else []
        if not isinstance(prior_steps_raw, list):
            prior_steps_raw = []
        steps: List[Dict[str, Any]] = [row for row in prior_steps_raw if isinstance(row, dict)]
        last_step_status: Dict[str, str] = {}
        last_step_details: Dict[str, Dict[str, Any]] = {}
        for row in steps:
            step_name = str(row.get("step") or "")
            if not step_name:
                continue
            status = str(row.get("status") or "").lower()
            details = row.get("details")
            if status == "completed":
                last_step_status[step_name] = "completed"
                if isinstance(details, dict):
                    last_step_details[step_name] = details
            elif step_name not in last_step_status:
                last_step_status[step_name] = status
                if isinstance(details, dict):
                    last_step_details[step_name] = details

        domain_id = str(action.get("domain_id") or "").strip()
        payload = action.get("payload") or {}

        def log_event(event_type: str, severity: str, message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
            if severity == "error":
                action_logger.error(message)
            elif severity == "warn":
                action_logger.warning(message)
            else:
                action_logger.info(message)
            try:
                self.client.insert_action_log(action, event_type, severity, message, metadata or {})
            except Exception:
                pass

        def start_step(step_name: str) -> Dict[str, Any]:
            row = {"step": step_name, "status": "in_progress", "startedAt": self._iso_now()}
            steps.append(row)
            return row

        def complete_step(step: Dict[str, Any], details: Optional[Dict[str, Any]] = None) -> None:
            step["status"] = "completed"
            step["completedAt"] = self._iso_now()
            if details:
                step["details"] = details

        def fail_step(step: Dict[str, Any], error: str) -> None:
            step["status"] = "failed"
            step["completedAt"] = self._iso_now()
            step["error"] = error

        def skip_step(step_name: str, reason: str) -> None:
            steps.append({"step": step_name, "status": "skipped", "details": {"reason": reason}})

        def checkpoint(step_name: str) -> Optional[Dict[str, Any]]:
            if last_step_status.get(step_name) == "completed":
                details = last_step_details.get(step_name) or {}
                log_event("step_resumed", "info", f"[{step_name}] Reusing checkpoint", {"details": details})
                return details
            return None

        def persist_progress(interim_status: Optional[str] = None, domain_status: Optional[str] = None) -> None:
            try:
                self.client.update_action(
                    action,
                    {"result": {"steps": steps, "lastUpdated": self._iso_now()}},
                )
                if domain_id and (interim_status or domain_status):
                    fields: Dict[str, Any] = {}
                    if interim_status:
                        fields["interim_status"] = interim_status
                    if domain_status:
                        fields["status"] = domain_status
                    # Conditional update: never overwrite a domain that has been
                    # moved into a cancellation/terminal state. Protects the
                    # cancel/provision race window so a customer-initiated cancel
                    # is not clobbered by a subsequent progress write.
                    if domain_status:
                        self.client.update_domain_if_active(domain_id, fields)
                    else:
                        self.client.update_domain(domain_id, fields)
            except Exception:
                pass

        try:
            if not domain_id:
                raise RuntimeError("Action missing domain_id")

            domain = self.client.get_domain(domain_id)
            if not domain:
                raise RuntimeError(f"Domain {domain_id} not found")
            domain_status = str(domain.get("status") or "").strip().lower()
            if domain_status in ("queued_for_cancellation", "suspended", "cancelled"):
                self.client.complete_action(action, {"skipped": True, "reason": f"Domain is {domain_status}"})
                return

            domain_name = str(domain.get("domain") or "").strip().lower()
            if not domain_name:
                raise RuntimeError("Domain value is missing")

            customer_id = str(action.get("customer_id") or domain.get("customer_id") or payload.get("customer_id") or "").strip()
            inboxes = self.client.get_domain_inboxes_all(domain_id)
            inboxes = [inbox for inbox in inboxes if str(inbox.get("status") or "").strip().lower() != "deleted"]
            if not inboxes:
                self.client.complete_action(action, {"skipped": True, "reason": "No inboxes found"})
                return

            # Conditional: only flip to in_progress if the domain is still in an
            # active/preparing state (race guard for mid-flight cancellation).
            self.client.update_domain_if_active(domain_id, {"status": "in_progress"})
            for inbox in inboxes:
                if str(inbox.get("status") or "") in {"pending", "provisioning"}:
                    self.client.update_inbox(str(inbox.get("id")), {"status": "provisioning"})

            log_event(
                "action_started",
                "info",
                f"Starting nonprofit Google provisioning for {domain_name}",
                {"domain": domain_name, "customer_id": customer_id, "inbox_count": len(inboxes)},
            )

            panel_details = checkpoint("assign_panel")
            if not panel_details:
                step = start_step("assign_panel")
                panel_details = self._assign_or_reuse_panel(
                    domain_id=domain_id,
                    customer_id=customer_id,
                    user_count=len(inboxes),
                )
                complete_step(step, self._panel_step_details(panel_details))
                persist_progress(INTERIM_STATUSES["PANEL_ASSIGNED"], "in_progress")

            panel_id = str(panel_details.get("panel_id") or panel_details.get("id") or "").strip()
            if not panel_id:
                raise RuntimeError("Panel assignment RPC did not return panel_id")

            panel_record = self._get_panel_record(panel_id)
            if not panel_record:
                raise RuntimeError(f"Assigned nonprofit panel {panel_id} not found")

            admin_creds = self._resolve_panel_credentials(panel_record)
            admin_op_client = self._maybe_build_op_client(self.admin_vault)
            if admin_op_client is not None:
                admin_creds["_op_client"] = admin_op_client
            panel_client = NonprofitGooglePanelClient(str(admin_creds.get("apps_script_url") or ""))

            zone_checkpoint = checkpoint("ensure_cloudflare_zone")
            if zone_checkpoint:
                zone_id = str(zone_checkpoint.get("zone_id") or "").strip()
            else:
                step = start_step("ensure_cloudflare_zone")
                zone_id, created = self._ensure_cloudflare_zone(domain)
                complete_step(step, {"zone_id": zone_id, "created": created})
                persist_progress(INTERIM_STATUSES["PANEL_ASSIGNED"], "in_progress")

            txt_checkpoint = checkpoint("get_domain_txt")
            if txt_checkpoint:
                txt_record = str(txt_checkpoint.get("txtRecord") or txt_checkpoint.get("txt_record") or "").strip()
            else:
                step = start_step("get_domain_txt")
                txt_result = panel_client.get_domain_txt(domain_name)
                txt_record = str(txt_result.get("txtRecord") or "").strip()
                if not txt_result.get("success") or not txt_record:
                    raise RuntimeError(f"Apps Script failed to return TXT token: {txt_result}")
                complete_step(step, {"txtRecord": txt_record, "response": txt_result})
                persist_progress(INTERIM_STATUSES["TXT_FETCHED"], "in_progress")

            txt_write_checkpoint = checkpoint("write_txt_record")
            if not txt_write_checkpoint:
                step = start_step("write_txt_record")
                dns_summary = self._get_cloudflare().upsert_dns_records(
                    zone_id,
                    [{"type": "TXT", "name": "@", "content": txt_record, "ttl": 3600}],
                )
                if int(dns_summary.get("failed") or 0) > 0:
                    fail_message = f"Cloudflare TXT write failed for {domain_name}: {dns_summary}"
                    fail_step(step, fail_message)
                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                    raise RuntimeError(fail_message)
                complete_step(step, dns_summary)
                persist_progress(INTERIM_STATUSES["TXT_WRITTEN"], "in_progress")

            txt_visible_checkpoint = checkpoint("wait_public_txt")
            if not txt_visible_checkpoint:
                step = start_step("wait_public_txt")
                public_dns = self._check_public_txt_record(domain_name, txt_record)
                if not bool(public_dns.get("visible")):
                    message = (
                        f"Waiting for DNS propagation: Google verification TXT for {domain_name} "
                        "is not visible in public DNS yet."
                    )
                    step["status"] = "waiting"
                    step["completedAt"] = self._iso_now()
                    step["error"] = message
                    step["details"] = {
                        **public_dns,
                        "classification": "dns_propagation",
                        "playwrightAttempted": False,
                        "nextRetrySeconds": self.dns_propagation_retry_seconds,
                    }
                    persist_progress(INTERIM_STATUSES["DNS_PROPAGATING"], "ns_pending")
                    raise DeferredNonprofitAction(
                        message,
                        delay_seconds=self.dns_propagation_retry_seconds,
                        consume_attempt=False,
                    )
                complete_step(step, public_dns)
                persist_progress(INTERIM_STATUSES["TXT_VISIBLE"], "in_progress")

            verify_checkpoint = checkpoint("verify_domain")
            if not verify_checkpoint:
                step = start_step("verify_domain")
                verify_result = panel_client.verify_domain_via_api(domain_name)
                if not self._domain_verification_succeeded(verify_result):
                    verify_result = asyncio.run(self._verify_domain_ui(domain_name, admin_creds))
                if not self._domain_verification_succeeded(verify_result):
                    if verify_result.get("timedOut") or "verification failed" in str(verify_result.get("error") or "").lower():
                        message = (
                            f"Waiting for Google to recognize DNS for {domain_name}. "
                            f"Public TXT is visible, but Google verification is not complete yet: {verify_result}"
                        )
                        step["status"] = "waiting"
                        step["completedAt"] = self._iso_now()
                        step["error"] = message
                        step["details"] = {
                            "verifyResult": verify_result,
                            "classification": "google_dns_recognition_pending",
                            "playwrightAttempted": True,
                            "nextRetrySeconds": self.dns_propagation_retry_seconds,
                        }
                        persist_progress(INTERIM_STATUSES["DNS_PROPAGATING"], "ns_pending")
                        raise DeferredNonprofitAction(
                            message,
                            delay_seconds=self.dns_propagation_retry_seconds,
                            consume_attempt=False,
                        )
                    fail_step(step, f"Domain verification failed: {verify_result}")
                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                    raise RuntimeError(f"Domain verification failed for {domain_name}: {verify_result}")
                complete_step(step, verify_result)
                persist_progress(INTERIM_STATUSES["DOMAIN_VERIFIED"], "in_progress")

            create_checkpoint = checkpoint("batch_create_users")
            user_payloads = self._build_panel_user_payloads(domain_name, inboxes)
            if not create_checkpoint:
                step = start_step("batch_create_users")
                create_result = panel_client.batch_create_users(user_payloads, skip_photos=True)
                if list(create_result.get("errors") or []):
                    non_duplicate = [
                        row for row in (create_result.get("errors") or [])
                        if "already exists" not in str(row.get("error") or "").lower()
                    ]
                    if non_duplicate:
                        if self._is_directory_authorization_failure(non_duplicate):
                            log_event(
                                "step_warning",
                                "warn",
                                "[batch_create_users] Apps Script Directory API is not authorized; falling back to Google Admin Playwright user creation.",
                                {"error_count": len(non_duplicate)},
                            )
                            fallback_result = self._create_users_via_google_admin_ui(
                                domain_name=domain_name,
                                user_payloads=user_payloads,
                                admin_creds=admin_creds,
                            )
                            if int(fallback_result.get("failed") or 0) > 0:
                                details = {
                                    "primary": "apps_script",
                                    "fallback": "google_admin_playwright",
                                    "appsScriptErrors": non_duplicate,
                                    "playwright": fallback_result,
                                }
                                fail_step(step, json.dumps(details)[:2000])
                                persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                                raise RuntimeError(f"Playwright user creation fallback failed: {details}")
                            create_result = {
                                "primary": "apps_script",
                                "fallback": "google_admin_playwright",
                                "appsScriptErrors": non_duplicate,
                                "playwright": fallback_result,
                            }
                        else:
                            fail_step(step, json.dumps(non_duplicate)[:2000])
                            persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                            raise RuntimeError(f"Apps Script user creation failed: {non_duplicate}")
                complete_step(step, create_result)
                persist_progress(INTERIM_STATUSES["USERS_CREATED"], "in_progress")

            dkim_checkpoint = checkpoint("enable_dkim")
            if not dkim_checkpoint:
                step = start_step("enable_dkim")
                dkim_result = asyncio.run(self._enable_dkim(domain_name, admin_creds))
                complete_step(step, dkim_result)
                persist_progress(INTERIM_STATUSES["DKIM_ENABLED"], "in_progress")

            mfa_checkpoint = checkpoint("setup_2fa")
            mfa_details = dict(mfa_checkpoint or {})
            if not mfa_checkpoint:
                step = start_step("setup_2fa")
                user_op_client = self._build_op_client(self.user_vault)
                results = asyncio.run(self._setup_all_user_2fa(inboxes, domain_name, admin_creds, user_op_client))
                failures = [row for row in results if not row.get("success")]
                step_details = {
                    "completed": len(results) - len(failures),
                    "failed": len(failures),
                    "results": results,
                }
                if failures and self.require_mfa_enrollment:
                    fail_step(step, json.dumps(failures)[:2000])
                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                    raise RuntimeError(f"2FA setup failed for {len(failures)} user(s)")
                complete_step(step, step_details)
                persist_progress(INTERIM_STATUSES["MFA_ENROLLMENT"], "in_progress")
                mfa_details = step_details

            upload_checkpoint = checkpoint("upload_sending_tool")
            if not upload_checkpoint:
                step = start_step("upload_sending_tool")
                upload_result = self._upload_to_sending_tool(domain_id, domain_name, inboxes)
                if upload_result.get("skipped"):
                    fail_message = f"Sending-tool upload skipped: {upload_result.get('skipped')}"
                    fail_step(step, fail_message)
                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                    raise RuntimeError(fail_message)
                if upload_result.get("failed_uploads"):
                    fail_message = (
                        f"{upload_result.get('tool') or 'sending tool'} upload validation failed for "
                        f"{len(upload_result.get('failed_uploads') or [])}/{upload_result.get('total_candidates') or 0} inbox(es)"
                    )
                    fail_step(step, fail_message)
                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                    raise RuntimeError(fail_message)
                complete_step(step, upload_result)
                persist_progress(INTERIM_STATUSES["SENDING_TOOL_UPLOAD"], "in_progress")

            finalize_checkpoint = checkpoint("finalize")
            if not finalize_checkpoint:
                # Re-check domain status before marking active — a cancel may have
                # landed while this worker was in the middle of provisioning.
                fresh_domain = self.client.get_domain(domain_id) or {}
                fresh_status = str(fresh_domain.get("status") or "").strip().lower()
                if fresh_status in ("queued_for_cancellation", "cancelled", "suspended"):
                    log_event(
                        "action_skipped",
                        "warn",
                        f"Domain {domain_name} became {fresh_status} during provisioning; aborting finalize",
                        {"domain_status": fresh_status},
                    )
                    self.client.complete_action(
                        action,
                        {
                            "steps": steps,
                            "skipped": True,
                            "reason": f"Domain is {fresh_status}",
                            "panel_id": panel_id,
                        },
                    )
                    return

                step = start_step("finalize")
                item_ids = self._item_ids_from_mfa_results(mfa_details)
                now_iso = self._iso_now()
                for inbox in inboxes:
                    inbox_id = str(inbox.get("id") or "").strip()
                    email = self._resolve_inbox_email(inbox, domain_name)
                    fields: Dict[str, Any] = {"status": "active", "email": email, "activated_at": now_iso}
                    if email in item_ids:
                        fields["onepassword_item_id"] = item_ids[email]
                    self.client.update_inbox(inbox_id, fields)
                self.client.update_domain(
                    domain_id,
                    {
                        "status": "active",
                        "interim_status": INTERIM_STATUSES["COMPLETE"],
                        "nonprofit_panel_id": panel_id,
                        "activated_at": now_iso,
                    },
                )
                complete_step(step, {"domain_status": "active", "panel_id": panel_id})

            result = {"steps": steps, "panel_id": panel_id, "domain": domain_name}
            self.client.complete_action(action, result)
            log_event("action_completed", "info", f"Nonprofit Google provisioning complete for {domain_name}", result)
        except DeferredNonprofitAction as exc:
            message = str(exc)
            log_event(
                "action_deferred",
                "warn",
                message,
                {"delay_seconds": exc.delay_seconds, "consume_attempt": exc.consume_attempt},
            )
            try:
                self.client.update_action(action, {"result": {"steps": steps, "lastUpdated": self._iso_now()}})
            except Exception:
                pass
            self.client.defer_action(
                action,
                message,
                delay_seconds=exc.delay_seconds,
                consume_attempt=exc.consume_attempt,
            )
        except Exception as exc:
            message = str(exc)
            log_event("action_failed", "error", message)
            try:
                self.client.update_action(action, {"result": {"steps": steps, "lastUpdated": self._iso_now()}})
            except Exception:
                pass
            self.client.fail_action(action, message, max_retries=self.max_retries)

    async def _verify_domain_ui(self, domain_name: str, admin_creds: Dict[str, Any]) -> Dict[str, Any]:
        admin_op = self._maybe_build_op_client(self.admin_vault)
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=self.playwright_headless)
            context = await browser.new_context(viewport={"width": 1920, "height": 1080}, locale="en-US")
            page = await context.new_page()
            try:
                return await verify_domain_in_admin(page, domain_name, admin_creds, admin_op, logger.info)
            finally:
                await context.close()
                await browser.close()

    async def _enable_dkim(self, domain_name: str, admin_creds: Dict[str, Any]) -> Dict[str, Any]:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=self.playwright_headless)
            context = await browser.new_context(viewport={"width": 1920, "height": 1080}, locale="en-US")
            page = await context.new_page()
            try:
                return await enable_dkim_for_domain(page, domain_name, admin_creds, logger.info)
            finally:
                await context.close()
                await browser.close()

    async def _setup_all_user_2fa(
        self,
        inboxes: List[Dict[str, Any]],
        domain_name: str,
        admin_creds: Dict[str, Any],
        user_op_client: OnePasswordCliClient,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=self.playwright_headless)
            for inbox in inboxes:
                context = await browser.new_context(viewport={"width": 1440, "height": 1080}, locale="en-US")
                page = await context.new_page()
                email = self._resolve_inbox_email(inbox, domain_name)
                password = str(inbox.get("password") or "").strip()
                try:
                    result = await setup_user_2fa(page, email, password, admin_creds, user_op_client, logger.info)
                    item_id = str(result.get("item_id") or "").strip()
                    if item_id:
                        self.client.update_inbox(str(inbox.get("id")), {"onepassword_item_id": item_id})
                    results.append({"email": email, **result})
                except Exception as exc:
                    results.append({"email": email, "success": False, "error": str(exc)})
                finally:
                    await context.close()
            await browser.close()
        return results

    def _upload_to_sending_tool(self, domain_id: str, domain_name: str, inboxes: List[Dict[str, Any]]) -> Dict[str, Any]:
        bundle = self.client.get_domain_tool_credentials(domain_id)
        if not bundle:
            return {"skipped": "No sending tool credentials assigned"}
        tool_slug = self._normalize_tool_slug(str(bundle.get("slug") or ""))
        if not tool_slug:
            return {"skipped": "Unsupported sending tool slug"}

        credential = bundle.get("credential") or {}
        api_key = str(credential.get("api_key") or credential.get("apiKey") or "").strip()
        if not api_key:
            return {"skipped": "Sending tool api_key missing"}

        user_op_client = self._maybe_build_op_client(self.user_vault)
        payloads = []
        for inbox in inboxes:
            payloads.append(
                {
                    "email": self._resolve_inbox_email(inbox, domain_name),
                    "password": str(inbox.get("password") or "").strip(),
                    "first_name": str(inbox.get("first_name") or inbox.get("firstName") or "").strip(),
                    "last_name": str(inbox.get("last_name") or inbox.get("lastName") or "").strip(),
                }
            )

        return self._sending_tool_uploader.upload_and_validate(
            tool=tool_slug,
            api_key=api_key,
            inboxes=payloads,
            provider="google",
            credential=credential,
            settings=credential.get("settings") if isinstance(credential.get("settings"), dict) else {},
            onepassword=user_op_client,
            headless=self.playwright_headless,
            use_playwright_oauth=True,
        )

    def _assign_or_reuse_panel(self, *, domain_id: str, customer_id: str, user_count: int) -> Dict[str, Any]:
        existing = self._get_existing_panel_assignment(domain_id)
        if existing:
            return existing
        return self._rpc(
            "assign_nonprofit_panel",
            {
                "p_domain_id": domain_id,
                "p_customer_id": customer_id or None,
                "p_user_count": int(user_count),
            },
        )

    def _get_existing_panel_assignment(self, domain_id: str) -> Optional[Dict[str, Any]]:
        rows = self.client._request(  # type: ignore[attr-defined]
            "GET",
            "domain_panel_assignments",
            params={"select": "*", "domain_id": f"eq.{domain_id}", "status": "eq.assigned", "limit": "1"},
        )
        if not rows:
            return None
        row = rows[0]
        return {
            "assignment_id": row.get("id"),
            "panel_id": row.get("panel_id"),
            "domain_id": row.get("domain_id"),
        }

    def _get_panel_record(self, panel_id: str) -> Optional[Dict[str, Any]]:
        rows = self.client._request(  # type: ignore[attr-defined]
            "GET",
            "nonprofit_panels",
            params={"select": "*", "id": f"eq.{panel_id}", "limit": "1"},
        )
        return rows[0] if rows else None

    @staticmethod
    def _panel_step_details(panel_details: Dict[str, Any]) -> Dict[str, Any]:
        allowed_keys = {
            "assignment_id",
            "panel_id",
            "id",
            "name",
            "status",
            "max_users",
            "max_domains",
            "current_user_count",
            "current_domain_count",
        }
        return {
            key: value
            for key, value in panel_details.items()
            if key in allowed_keys and value is not None
        }

    def _resolve_panel_credentials(self, panel_record: Dict[str, Any]) -> Dict[str, Any]:
        apps_script_url = str(panel_record.get("apps_script_url") or "").strip()
        if not apps_script_url:
            raise RuntimeError("Assigned nonprofit panel is missing apps_script_url")

        result: Dict[str, Any] = {
            "panel_id": str(panel_record.get("id") or "").strip(),
            "apps_script_url": apps_script_url,
            "admin_email": str(panel_record.get("admin_email") or "").strip().lower(),
            "admin_password": str(panel_record.get("admin_password") or "").strip(),
            "op_item_id": str(panel_record.get("op_totp_item") or "").strip(),
            "op_item_title": str(panel_record.get("op_totp_item") or "").strip(),
            "op_totp_item": str(panel_record.get("op_totp_item") or "").strip(),
            "totp_secret": str(panel_record.get("totp_secret") or "").strip(),
        }
        if not result["admin_email"] or not result["admin_password"]:
            raise RuntimeError(f"Could not resolve admin credentials for nonprofit panel {result['panel_id']}")
        return result

    def _ensure_cloudflare_zone(self, domain: Dict[str, Any]) -> tuple[str, bool]:
        zone_id = str(domain.get("cloudflare_zone_id") or "").strip()
        if zone_id:
            return zone_id, False
        zone_id, created = self._get_cloudflare().get_or_create_zone(str(domain.get("domain") or "").strip().lower())
        self.client.update_domain(str(domain.get("id")), {"cloudflare_zone_id": zone_id})
        return zone_id, created

    def _check_public_txt_record(self, domain_name: str, expected_txt: str) -> Dict[str, Any]:
        expected = self._normalize_txt_value(expected_txt)
        resolvers = [
            ("google", "https://dns.google/resolve", {}),
            ("cloudflare", "https://cloudflare-dns.com/dns-query", {"accept": "application/dns-json"}),
        ]
        checks: List[Dict[str, Any]] = []

        for resolver_name, url, headers in resolvers:
            try:
                response = requests.get(
                    url,
                    params={"name": domain_name, "type": "TXT"},
                    headers=headers,
                    timeout=15,
                )
                payload = response.json() if response.text else {}
                answers = payload.get("Answer") if isinstance(payload, dict) else []
                values = [
                    self._normalize_txt_value(answer.get("data"))
                    for answer in answers or []
                    if isinstance(answer, dict) and int(answer.get("type") or 0) == 16
                ]
                visible = expected in values
                checks.append(
                    {
                        "resolver": resolver_name,
                        "visible": visible,
                        "answerCount": len(values),
                        "status": payload.get("Status") if isinstance(payload, dict) else None,
                    }
                )
            except Exception as exc:
                checks.append({"resolver": resolver_name, "visible": False, "error": str(exc)})

        google_check = next((row for row in checks if row.get("resolver") == "google"), {})
        any_visible = any(bool(row.get("visible")) for row in checks)
        google_visible = bool(google_check.get("visible"))
        google_unavailable = bool(google_check.get("error"))

        return {
            "visible": google_visible or (google_unavailable and any_visible),
            "requiredRecord": expected,
            "checks": checks,
        }

    @staticmethod
    def _domain_verification_succeeded(result: Dict[str, Any]) -> bool:
        if bool(result.get("verified")):
            return True
        verification = result.get("verification")
        if isinstance(verification, dict):
            if bool(verification.get("verified")):
                return True
            if bool(verification.get("success")) and "verified successfully" in str(verification.get("message") or "").lower():
                return True
        if bool(result.get("success")) and "verified successfully" in str(result.get("message") or "").lower():
            return True
        return False

    @staticmethod
    def _normalize_txt_value(value: Any) -> str:
        text = str(value or "").strip()
        if text.startswith('"') and text.endswith('"'):
            text = text[1:-1]
        return text.replace('" "', "").strip()

    def _build_panel_user_payloads(self, domain_name: str, inboxes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for inbox in inboxes:
            email = self._resolve_inbox_email(inbox, domain_name)
            local_part = email.split("@")[0]
            password = str(inbox.get("password") or "").strip()
            if not password:
                password = self._generate_password()
                inbox_id = str(inbox.get("id") or "").strip()
                if inbox_id:
                    self.client.update_inbox(inbox_id, {"password": password})
            rows.append(
                {
                    "firstName": str(inbox.get("first_name") or inbox.get("firstName") or local_part).strip() or local_part,
                    "lastName": str(inbox.get("last_name") or inbox.get("lastName") or "Inbox").strip() or "Inbox",
                    "email": email,
                    "password": password,
                    "orgUnitPath": f"/{domain_name}",
                    "recoveryEmail": str(inbox.get("recovery_email") or "").strip() or None,
                }
            )
        return rows

    @staticmethod
    def _is_directory_authorization_failure(errors: List[Dict[str, Any]]) -> bool:
        if not errors:
            return False
        return all(
            "not authorized to access this resource/api" in str(row.get("error") or "").lower()
            for row in errors
            if isinstance(row, dict)
        )

    def _create_users_via_google_admin_ui(
        self,
        *,
        domain_name: str,
        user_payloads: List[Dict[str, Any]],
        admin_creds: Dict[str, Any],
    ) -> Dict[str, Any]:
        admin_email = str(admin_creds.get("admin_email") or "").strip().lower()
        admin_password = str(admin_creds.get("admin_password") or "").strip()
        if not admin_email or not admin_password:
            raise RuntimeError("Panel admin credentials are required for Google Admin Playwright user creation fallback")

        users = [
            GoogleAdminUser(
                email=str(row.get("email") or "").strip().lower(),
                first_name=str(row.get("firstName") or "").strip(),
                last_name=str(row.get("lastName") or "").strip(),
                password=str(row.get("password") or "").strip(),
            )
            for row in user_payloads
            if str(row.get("email") or "").strip() and str(row.get("password") or "").strip()
        ]
        if len(users) != len(user_payloads):
            raise RuntimeError("Every requested Google user must have an email and password before Playwright fallback")

        op_client = admin_creds.get("_op_client")
        if not isinstance(op_client, OnePasswordCliClient):
            op_client = None

        with GoogleAdminPlaywrightClient(headless=self.playwright_headless, timeout_seconds=60) as browser:
            browser.login(
                admin_email,
                admin_password,
                onepassword=op_client,
                totp_secret=str(admin_creds.get("totp_secret") or "").strip(),
            )
            result = browser.add_users(users, domain_name)

        return {
            "added": int(result.get("added") or 0),
            "failed": int(result.get("failed") or 0),
            "requested": len(users),
            "method": "google_admin_playwright",
        }

    def _resolve_inbox_email(self, inbox: Dict[str, Any], domain_name: str) -> str:
        explicit = str(inbox.get("email") or inbox.get("google_email") or inbox.get("workspace_email") or "").strip().lower()
        if "@" in explicit:
            return explicit
        username = str(
            inbox.get("username")
            or inbox.get("email_username")
            or inbox.get("local_part")
            or explicit
            or f"user{random.randint(1000, 9999)}"
        ).strip().lower()
        target_domain = domain_name or str(inbox.get("domain") or "").strip().lower()
        return f"{username}@{target_domain}"

    def _item_ids_from_mfa_results(self, details: Dict[str, Any]) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for row in details.get("results") or []:
            if not isinstance(row, dict):
                continue
            email = str(row.get("email") or "").strip().lower()
            item_id = str(row.get("item_id") or "").strip()
            if email and item_id:
                mapping[email] = item_id
        return mapping

    def _normalize_tool_slug(self, raw: str) -> str:
        text = str(raw or "").strip().lower()
        if "instantly" in text:
            return "instantly.ai"
        if "smartlead" in text:
            return "smartlead.ai"
        return text

    def _rpc(self, fn: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = self.client.session.post(
            f"{self.client.base_url.rsplit('/rest/v1', 1)[0]}/rest/v1/rpc/{fn}",
            headers=self.client.base_headers,
            json=payload,
            timeout=self.client.timeout_seconds,
        )
        if response.status_code >= 300:
            raise RuntimeError(f"Supabase RPC {fn} failed ({response.status_code}): {response.text}")
        data = response.json() if response.text else {}
        if isinstance(data, list):
            return data[0] if data else {}
        if isinstance(data, dict):
            return data
        return {"value": data}

    def _build_op_client(self, vault: str) -> OnePasswordCliClient:
        token = str(os.getenv("OP_SERVICE_ACCOUNT_TOKEN") or "").strip()
        return OnePasswordCliClient(service_account_token=token, vault=vault)

    def _maybe_build_op_client(self, vault: str) -> Optional[OnePasswordCliClient]:
        try:
            return self._build_op_client(vault)
        except Exception:
            return None

    def _get_cloudflare(self) -> CloudflareClient:
        if self._cloudflare is None:
            api_token = os.getenv("CLOUDFLARE_API_TOKEN", "").strip()
            global_key = (
                os.getenv("CLOUDFLARE_GLOBAL_KEY", "").strip()
                or os.getenv("CLOUDFLARE_GLOBAL_API_KEY", "").strip()
            )
            global_email = os.getenv("CLOUDFLARE_EMAIL", "").strip()
            self._cloudflare = CloudflareClient(
                api_token=api_token,
                account_id=os.getenv("CLOUDFLARE_ACCOUNT_ID", ""),
                global_api_key=global_key,
                global_email=global_email,
            )
            logger.info(
                "Cloudflare auth configured for nonprofit worker (mode=%s, global_fallback=%s, account_id_set=%s)",
                "token" if api_token else "global",
                bool(api_token and global_key and global_email),
                bool(os.getenv("CLOUDFLARE_ACCOUNT_ID", "").strip()),
            )
        return self._cloudflare

    @staticmethod
    def _generate_password(length: int = 18) -> str:
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        return "".join(random.choice(alphabet) for _ in range(length))

    @staticmethod
    def _as_bool(value: Any, default: bool = False) -> bool:
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _iso_now() -> str:
        return datetime.now(timezone.utc).isoformat()
