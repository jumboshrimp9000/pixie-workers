import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app import get_order_logger
from app.workers.nonprofit_google_panel_client import NonprofitGooglePanelClient
from app.workers.supabase_client import SupabaseRestClient


logger = logging.getLogger(__name__)


class NonprofitGoogleCancelWorker:
    def __init__(self) -> None:
        self.client = SupabaseRestClient.from_env()
        self.poll_interval_seconds = max(1.0, float(os.getenv("NONPROFIT_GOOGLE_POLL_SECONDS", "10")))
        self.batch_size = max(1, int(os.getenv("NONPROFIT_GOOGLE_BATCH_SIZE", "3")))
        self.max_retries = max(1, int(os.getenv("NONPROFIT_GOOGLE_MAX_RETRIES", "8")))
        self.action_types = ["free_google_cancel_domain"]

    def run_forever(self) -> None:
        logger.info("Nonprofit Google cancel worker started (poll=%.1fs)", self.poll_interval_seconds)
        while True:
            try:
                processed = self._poll_once()
                if processed == 0:
                    time.sleep(self.poll_interval_seconds)
            except Exception:
                logger.exception("Nonprofit cancel poll failed")
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
            self._process_action(claimed)
        return processed

    def _process_action(self, action: Dict[str, Any]) -> None:
        action_id = str(action.get("id") or "")
        action_logger = get_order_logger(action_id)
        prior_result = action.get("result") or {}
        prior_steps_raw = prior_result.get("steps") if isinstance(prior_result, dict) else []
        steps: List[Dict[str, Any]] = [row for row in prior_steps_raw if isinstance(row, dict)]
        last_step_status = {str(row.get("step") or ""): str(row.get("status") or "").lower() for row in steps}
        last_step_details = {
            str(row.get("step") or ""): row.get("details")
            for row in steps
            if isinstance(row.get("details"), dict) and str(row.get("step") or "")
        }
        domain_id = str(action.get("domain_id") or "").strip()
        payload = action.get("payload") or {}

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
                self.client.update_action(action_id, {"result": {"steps": steps, "lastUpdated": self._iso_now()}})
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
            inboxes = self.client.get_domain_inboxes_all(domain_id)
            inboxes = [inbox for inbox in inboxes if str(inbox.get("status") or "").strip().lower() != "deleted"]
            payment_status_on_cancel = payload.get("payment_status_on_cancel")

            panel_assignment = self._get_existing_panel_assignment(domain_id)
            panel_client: Optional[NonprofitGooglePanelClient] = None
            if not panel_assignment:
                message = f"No nonprofit panel assignment found for domain {domain_name}; skipping panel-side delete"
                log_event("panel_assignment_missing", "warn", message, {"domain": domain_name, "domain_id": domain_id})
            else:
                panel = self._get_panel_record(str(panel_assignment.get("panel_id") or ""))
                if not panel:
                    raise RuntimeError("Assigned nonprofit panel not found")
                creds = self._resolve_panel_credentials(panel)
                panel_client = NonprofitGooglePanelClient(str(creds.get("apps_script_url") or ""))

            if not checkpoint("delete_users"):
                if panel_client is None:
                    steps.append(
                        {
                            "step": "delete_users",
                            "status": "skipped",
                            "details": {"reason": "No nonprofit panel assignment found"},
                        }
                    )
                else:
                    step = start_step("delete_users")
                    deleted = []
                    errors = []
                    for inbox in inboxes:
                        email = self._resolve_inbox_email(inbox, domain_name)
                        try:
                            result = panel_client.delete_user(email, permanent=True)
                            if isinstance(result, dict) and result.get("success") is False:
                                error_text = str(
                                    result.get("error")
                                    or result.get("message")
                                    or result.get("details")
                                    or result
                                )
                                if self._is_idempotent_delete_error(error_text):
                                    deleted.append({"email": email, "result": result, "idempotent": True})
                                    continue
                                errors.append({"email": email, "error": error_text, "result": result})
                                continue
                            deleted.append({"email": email, "result": result})
                        except Exception as exc:
                            error_text = str(exc)
                            if self._is_idempotent_delete_error(error_text):
                                deleted.append({"email": email, "result": {"success": False, "error": error_text}, "idempotent": True})
                                continue
                            errors.append({"email": email, "error": error_text})
                    if errors:
                        raise RuntimeError(f"User deletion failed: {errors}")
                    complete_step(step, {"deleted": len(deleted), "results": deleted})
                    persist()

            if not checkpoint("mark_deleted"):
                step = start_step("mark_deleted")
                now_iso = self._iso_now()
                for inbox in inboxes:
                    self.client.update_inbox(str(inbox.get("id")), {"status": "deleted", "deleted_at": now_iso})
                domain_update: Dict[str, Any] = {"status": "cancelled", "cancelled_at": now_iso}
                if payment_status_on_cancel is not None:
                    domain_update["payment_status"] = payment_status_on_cancel
                self.client.update_domain(domain_id, domain_update)
                complete_step(step, {"inboxes": len(inboxes), "domain_status": "cancelled"})
                persist()

            if not checkpoint("release_panel_assignment"):
                if panel_assignment is None:
                    steps.append(
                        {
                            "step": "release_panel_assignment",
                            "status": "skipped",
                            "details": {"reason": "No nonprofit panel assignment found"},
                        }
                    )
                else:
                    step = start_step("release_panel_assignment")
                    release = self._rpc("release_nonprofit_panel_assignment", {"p_domain_id": domain_id})
                    complete_step(step, release)
                    persist()

            # Release the customer's promo reservation counter so the freed
            # slots become available for a future order. Count promo inboxes
            # that were on this domain (from the loaded inbox list before
            # deletion).
            if not checkpoint("release_promo_reservation"):
                promo_count = sum(
                    1 for inbox in inboxes
                    if str(inbox.get("billing_type") or "").strip().lower() == "free_inboxes_promo"
                )
                customer_id = str(domain.get("customer_id") or "").strip()
                if promo_count > 0 and customer_id:
                    step = start_step("release_promo_reservation")
                    try:
                        rpc_result = self._rpc(
                            "release_promo_reservation",
                            {"p_customer_id": customer_id, "p_release_count": promo_count},
                        )
                        complete_step(step, {"released": promo_count, "result": rpc_result})
                    except Exception as exc:
                        complete_step(step, {"released": 0, "error": str(exc)})
                    persist()
                else:
                    steps.append(
                        {
                            "step": "release_promo_reservation",
                            "status": "skipped",
                            "details": {"reason": "No promo inboxes on domain"},
                        }
                    )

            # Recovery chain: if the backend's enqueueRecoveryMove flagged this
            # cancellation as a source teardown for a recovery_pool row, enqueue
            # the microsoft_recovery_move follow-up action so the MS worker
            # picks the domain up (with source_teardown_done=true) on its next
            # poll. The MS worker will skip its own teardown step and continue
            # with the recovery tenant add + room mailbox + Instantly flow.
            chain_recovery_pool_id = str(payload.get("chain_to_recovery_pool_id") or "").strip()
            if chain_recovery_pool_id and not checkpoint("chain_to_microsoft_recovery"):
                step = start_step("chain_to_microsoft_recovery")
                try:
                    self.client._request(  # type: ignore[attr-defined]
                        "POST",
                        "actions",
                        payload={
                            "customer_id": str(domain.get("customer_id") or ""),
                            "domain_id": domain_id,
                            "order_batch_id": str(domain.get("order_batch_id") or None) or None,
                            "type": "microsoft_recovery_move",
                            "status": "pending",
                            "payload": {
                                "recovery_pool_id": chain_recovery_pool_id,
                                "source_teardown_done": True,
                            },
                            "attempts": 0,
                            "max_attempts": 5,
                        },
                    )
                    complete_step(step, {"recovery_pool_id": chain_recovery_pool_id})
                except Exception as exc:
                    complete_step(step, {"recovery_pool_id": chain_recovery_pool_id, "error": str(exc)})
                persist()

            result = {"steps": steps, "domain": domain_name, "cancelled": True}
            self.client.complete_action(action_id, result)
            log_event("action_completed", "info", f"Cancelled nonprofit Google domain {domain_name}", result)
        except Exception as exc:
            persist()
            log_event("action_failed", "error", str(exc))
            self.client.fail_action(action, str(exc), max_retries=self.max_retries)

    def _get_existing_panel_assignment(self, domain_id: str) -> Optional[Dict[str, Any]]:
        rows = self.client._request(  # type: ignore[attr-defined]
            "GET",
            "domain_panel_assignments",
            params={"select": "*", "domain_id": f"eq.{domain_id}", "status": "eq.assigned", "limit": "1"},
        )
        return rows[0] if rows else None

    def _get_panel_record(self, panel_id: str) -> Optional[Dict[str, Any]]:
        rows = self.client._request(  # type: ignore[attr-defined]
            "GET",
            "nonprofit_panels",
            params={"select": "*", "id": f"eq.{panel_id}", "limit": "1"},
        )
        return rows[0] if rows else None

    def _resolve_panel_credentials(self, panel_record: Dict[str, Any]) -> Dict[str, Any]:
        apps_script_url = str(panel_record.get("apps_script_url") or "").strip()
        admin_email = str(panel_record.get("admin_email") or "").strip().lower()
        admin_password = str(panel_record.get("admin_password") or "").strip()
        if not apps_script_url:
            raise RuntimeError("Assigned nonprofit panel is missing apps_script_url")
        if not admin_email or not admin_password:
            raise RuntimeError("Assigned nonprofit panel is missing admin_email or admin_password")
        return {
            "apps_script_url": apps_script_url,
            "admin_email": admin_email,
            "admin_password": admin_password,
            "op_item_id": str(panel_record.get("op_totp_item") or "").strip(),
            "op_item_title": str(panel_record.get("op_totp_item") or "").strip(),
            "op_totp_item": str(panel_record.get("op_totp_item") or "").strip(),
        }

    def _resolve_inbox_email(self, inbox: Dict[str, Any], domain_name: str) -> str:
        value = str(inbox.get("email") or "").strip().lower()
        if "@" in value:
            return value
        username = str(inbox.get("username") or value or "user").strip().lower()
        return f"{username}@{domain_name}"

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

    @staticmethod
    def _is_idempotent_delete_error(error_text: str) -> bool:
        normalized = str(error_text or "").strip().lower()
        return any(token in normalized for token in ("not found", "does not exist", "notfounderror"))

    @staticmethod
    def _iso_now() -> str:
        return datetime.now(timezone.utc).isoformat()
