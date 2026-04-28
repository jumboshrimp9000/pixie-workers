import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app import get_order_logger
from app.workers.google_admin_playwright import GoogleAdminPlaywrightClient
from app.workers.google_fulfillment_clients import PartnerHubClient
from app.workers.onepassword_client import OnePasswordCliClient
from app.workers.supabase_client import SupabaseRestClient


logger = logging.getLogger(__name__)

LIVE_OR_CANCELABLE_STATUSES = {"pending", "provisioning", "active", "suspended", "in_progress"}


class GoogleCancelWorker:
    """
    Cancels paid Google domains.

    The ordering is deliberate:
      1. remove Google-side users
      2. remove the domain from Google Admin
      3. cancel the PartnerHub/PartnerStage subscription

    Supplier cancellation is never attempted until Google domain removal has
    succeeded, because cancelling the subscription first can leave the domain
    attached to the wrong Google tenant/admin.
    """

    def __init__(self) -> None:
        self.client = SupabaseRestClient.from_env()
        self.poll_interval_seconds = max(1.0, float(os.getenv("GOOGLE_CANCEL_POLL_SECONDS", "10")))
        self.batch_size = max(1, int(os.getenv("GOOGLE_CANCEL_BATCH_SIZE", "3")))
        self.max_retries = max(1, int(os.getenv("GOOGLE_CANCEL_MAX_RETRIES", "8")))
        action_types = os.getenv("GOOGLE_CANCEL_ACTION_TYPES", "google_cancel_domain")
        self.action_types = [token.strip() for token in action_types.split(",") if token.strip()]

        self.playwright_headless = self._as_bool(
            os.getenv("GOOGLE_CANCEL_PLAYWRIGHT_HEADLESS", os.getenv("GOOGLE_PLAYWRIGHT_HEADLESS", "true")),
            default=True,
        )
        self.partnerhub_base_url = os.getenv("PARTNERHUB_API_BASE_URL", "https://partnerhubapi.netstager.com/api")
        self.partnerhub_api_key = os.getenv("PARTNERHUB_API_KEY") or os.getenv("GOOGLE_NETSTAGER_API_KEY") or ""
        self.partnerhub_plan_id = os.getenv("PARTNERHUB_PLAN_ID", "94c835bb-a675-4249-8fba-e95cdb2ca4ed")
        self.partnerhub_delete_type = (
            os.getenv("GOOGLE_CANCEL_DELETE_TYPE")
            or os.getenv("CANCEL_FINAL_SUPPLIER_DELETE_TYPE")
            or "IMMEDIATE"
        ).strip().upper()

        self._partnerhub: Optional[PartnerHubClient] = None

    def run_forever(self) -> None:
        logger.info(
            "Google cancel worker started (types=%s, poll=%.1fs)",
            self.action_types,
            self.poll_interval_seconds,
        )
        while True:
            try:
                processed = self._poll_once()
                if processed == 0:
                    time.sleep(self.poll_interval_seconds)
            except Exception:
                logger.exception("Google cancel poll failed")
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
        domain_id = str(action.get("domain_id") or "").strip()
        payload = action.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}

        prior_result = action.get("result") or {}
        prior_steps_raw = prior_result.get("steps") if isinstance(prior_result, dict) else []
        if not isinstance(prior_steps_raw, list):
            prior_steps_raw = []
        steps: List[Dict[str, Any]] = [row for row in prior_steps_raw if isinstance(row, dict)]
        last_step_status = {str(row.get("step") or ""): str(row.get("status") or "").lower() for row in steps}
        last_step_details = {
            str(row.get("step") or ""): row.get("details")
            for row in steps
            if isinstance(row.get("details"), dict) and str(row.get("step") or "")
        }

        def start_step(name: str) -> Dict[str, Any]:
            row = {"step": name, "status": "in_progress", "startedAt": self._iso_now()}
            steps.append(row)
            return row

        def complete_step(step: Dict[str, Any], details: Optional[Dict[str, Any]] = None) -> None:
            step["status"] = "completed"
            step["completedAt"] = self._iso_now()
            if details:
                step["details"] = details

        def checkpoint(step_name: str) -> Optional[Dict[str, Any]]:
            if last_step_status.get(step_name) == "completed":
                steps.append({"step": step_name, "status": "skipped", "details": {"reason": "Resumed from checkpoint"}})
                return last_step_details.get(step_name) or {}
            return None

        def persist() -> None:
            try:
                self.client.update_action(action, {"result": {"steps": steps, "lastUpdated": self._iso_now()}})
            except Exception:
                pass

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

        try:
            if not domain_id:
                raise RuntimeError("Action missing domain_id")

            domain = self.client.get_domain(domain_id)
            if not domain:
                raise RuntimeError(f"Domain {domain_id} not found")

            domain_name = str(domain.get("domain") or "").strip().lower()
            if not domain_name:
                raise RuntimeError("Domain row is missing domain name")

            provider = str(domain.get("provider") or "").strip().lower()
            if provider != "google":
                result = {"skipped": True, "reason": "Domain provider is not google", "domain": domain_name}
                self.client.complete_action(action, result)
                log_event("action_completed", "info", "Skipped non-google cancellation action", result)
                return

            all_inboxes = self.client.get_domain_inboxes_all(domain_id)
            inboxes = [
                row
                for row in all_inboxes
                if str(row.get("status") or "").strip().lower() != "deleted"
            ]

            if self._has_free_promo_inboxes(inboxes):
                raise RuntimeError(
                    "google_cancel_domain is for paid Google domains only. "
                    "Use free_google_cancel_domain for free promo/nonprofit Google domains."
                )

            google_teardown_completed = last_step_status.get("remove_google_users_and_domain") == "completed"
            admin_email = str((last_step_details.get("resolve_admin_login") or {}).get("admin_email") or "").strip().lower()
            admin_password = ""
            target_emails: List[str] = []
            if not google_teardown_completed:
                admin_row = self._resolve_admin_login_row(inboxes, domain_name)
                admin_email = str(admin_row.get("email") or "").strip().lower()
                admin_password = str(admin_row.get("password") or "").strip()
                target_emails = self._ordered_target_emails(inboxes, domain_name, admin_email)
            payment_status_on_cancel = payload.get("payment_status_on_cancel")

            if google_teardown_completed:
                checkpoint("resolve_admin_login")
            elif not checkpoint("resolve_admin_login"):
                step = start_step("resolve_admin_login")
                complete_step(step, {"admin_email": admin_email, "targets": len(target_emails)})
                persist()

            if not checkpoint("remove_google_users_and_domain"):
                step = start_step("remove_google_users_and_domain")
                op_client: Optional[OnePasswordCliClient] = None
                try:
                    op_client = OnePasswordCliClient.from_env()
                except Exception as exc:
                    log_event(
                        "onepassword_unavailable",
                        "warn",
                        "1Password unavailable for Google cancel login; continuing without OTP helper.",
                        {"error": str(exc)},
                    )

                with GoogleAdminPlaywrightClient(headless=self.playwright_headless) as browser:
                    browser.login(admin_email, admin_password, onepassword=op_client)
                    non_admin_targets = [email for email in target_emails if email != admin_email]
                    admin_targets = [admin_email] if admin_email in target_emails else []
                    delete_result = browser.delete_users(non_admin_targets, permanent=True)
                    if int(delete_result.get("failed") or 0) > 0:
                        raise RuntimeError(f"Google user deletion failed before domain removal: {delete_result}")
                    admin_delete_result: Optional[Dict[str, Any]] = None
                    try:
                        remove_result = browser.remove_domain(domain_name)
                    except Exception as first_remove_error:
                        if not admin_targets:
                            raise
                        admin_delete_result = browser.delete_users(admin_targets, permanent=True)
                        if int(admin_delete_result.get("failed") or 0) > 0:
                            raise RuntimeError(
                                "Google domain removal failed after non-admin user deletion, "
                                f"and admin deletion also failed: remove_error={first_remove_error}; "
                                f"admin_delete={admin_delete_result}"
                            ) from first_remove_error
                        remove_result = browser.remove_domain(domain_name)

                complete_step(
                    step,
                    {
                        "deleted_users": delete_result,
                        "deleted_admin_user": admin_delete_result,
                        "removed_domain": remove_result,
                    },
                )
                persist()

            if not checkpoint("cancel_partnerhub_subscription"):
                step = start_step("cancel_partnerhub_subscription")
                stored_order_id = str(
                    domain.get("partnerhub_order_id")
                    or domain.get("partnerstage_order_id")
                    or payload.get("partnerhub_order_id")
                    or payload.get("partnerstage_order_id")
                    or ""
                ).strip()
                if stored_order_id:
                    cancel_result = self._get_partnerhub().delete_order_by_order_id(
                        stored_order_id,
                        delete_type=self.partnerhub_delete_type,
                    )
                else:
                    cancel_result = self._get_partnerhub().delete_order_by_domain(
                        domain_name,
                        delete_type=self.partnerhub_delete_type,
                    )
                complete_step(step, cancel_result)
                persist()

            if not checkpoint("mark_deleted"):
                step = start_step("mark_deleted")
                now_iso = self._iso_now()
                for inbox in inboxes:
                    inbox_id = str(inbox.get("id") or "").strip()
                    if inbox_id:
                        self.client.update_inbox(inbox_id, {"status": "deleted", "deleted_at": now_iso})
                domain_update: Dict[str, Any] = {
                    "status": "cancelled",
                    "interim_status": None,
                    "cancel_at": None,
                    "cancelled_at": now_iso,
                }
                if payment_status_on_cancel is not None:
                    domain_update["payment_status"] = payment_status_on_cancel
                self.client.update_domain(domain_id, domain_update)
                complete_step(step, {"domain_status": "cancelled", "inboxes": len(inboxes)})
                persist()

            result = {
                "steps": steps,
                "domain": domain_name,
                "cancelled": True,
                "supplier_delete_type": self.partnerhub_delete_type,
            }
            self.client.complete_action(action, result)
            log_event("action_completed", "info", f"Cancelled paid Google domain {domain_name}", result)
        except Exception as exc:
            persist()
            log_event("action_failed", "error", str(exc))
            self.client.fail_action(action, str(exc), max_retries=self.max_retries)

    def _resolve_admin_login_row(self, inboxes: List[Dict[str, Any]], domain_name: str) -> Dict[str, Any]:
        candidates = [
            row
            for row in inboxes
            if bool(row.get("is_admin"))
            and str(row.get("email") or "").strip()
            and str(row.get("password") or "").strip()
            and str(row.get("status") or "").strip().lower() in LIVE_OR_CANCELABLE_STATUSES
        ]
        if not candidates:
            candidates = [
                row
                for row in inboxes
                if str(row.get("email") or "").strip()
                and str(row.get("password") or "").strip()
                and str(row.get("status") or "").strip().lower() in LIVE_OR_CANCELABLE_STATUSES
            ]
        if not candidates:
            raise RuntimeError(
                f"No admin-capable Google inbox found for {domain_name}. "
                "Cancellation cannot remove the Google domain before supplier cancellation without a live admin login."
            )
        return sorted(
            candidates,
            key=lambda row: (
                not bool(row.get("is_admin")),
                str(row.get("created_at") or ""),
                str(row.get("id") or ""),
            ),
        )[0]

    def _ordered_target_emails(
        self,
        inboxes: List[Dict[str, Any]],
        domain_name: str,
        admin_email: str,
    ) -> List[str]:
        emails: List[str] = []
        for inbox in inboxes:
            status = str(inbox.get("status") or "").strip().lower()
            if status not in LIVE_OR_CANCELABLE_STATUSES:
                continue
            email = self._resolve_inbox_email(inbox, domain_name)
            if email and email not in emails:
                emails.append(email)

        non_admin = [email for email in emails if email != admin_email]
        return non_admin + ([admin_email] if admin_email in emails else [])

    def _resolve_inbox_email(self, inbox: Dict[str, Any], domain_name: str) -> str:
        value = str(inbox.get("email") or "").strip().lower()
        if "@" in value:
            return value
        username = str(inbox.get("username") or value or "").strip().lower()
        if not username:
            return ""
        return f"{username}@{domain_name}"

    def _get_partnerhub(self) -> PartnerHubClient:
        if self._partnerhub is None:
            self._partnerhub = PartnerHubClient(
                api_key=self.partnerhub_api_key,
                base_url=self.partnerhub_base_url,
                default_plan_id=self.partnerhub_plan_id,
            )
        return self._partnerhub

    @staticmethod
    def _has_free_promo_inboxes(inboxes: List[Dict[str, Any]]) -> bool:
        for inbox in inboxes:
            billing_type = str(inbox.get("billing_type") or "").strip().lower()
            if billing_type == "free_inboxes_promo":
                return True
        return False

    @staticmethod
    def _as_bool(value: Optional[str], *, default: bool = False) -> bool:
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _iso_now() -> str:
        return datetime.now(timezone.utc).isoformat()
