import logging
import os
import random
import string
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app import get_order_logger
from app.workers.google_admin_playwright import GoogleAdminPlaywrightClient, GoogleMfaUser
from app.workers.google_fulfillment_clients import PartnerHubClient
from app.workers.onepassword_client import OnePasswordCliClient
from app.workers.supabase_client import SupabaseRestClient


logger = logging.getLogger(__name__)

LIVE_STATUSES = {"pending", "provisioning", "active"}
TERMINAL_STATUS = "deleted"


class GoogleInboxLifecycleWorker:
    """
    Processes Google inbox lifecycle actions from Supabase:
      - google_add_inboxes
      - google_remove_inboxes
      - google_update_inboxes
      - google_update_profile_photos

    Each action is resumable:
      - step progress checkpoints are written to actions.result.steps[]
      - retries continue from completed checkpoints
    """

    def __init__(self):
        self.client = SupabaseRestClient.from_env()
        self.poll_interval_seconds = max(1.0, float(os.getenv("GOOGLE_LIFECYCLE_POLL_SECONDS", "10")))
        self.batch_size = max(1, int(os.getenv("GOOGLE_LIFECYCLE_BATCH_SIZE", "3")))
        self.max_retries = max(1, int(os.getenv("GOOGLE_LIFECYCLE_MAX_RETRIES", "8")))
        action_types = os.getenv(
            "GOOGLE_LIFECYCLE_ACTION_TYPES",
            "google_add_inboxes,google_remove_inboxes,google_update_inboxes,google_update_profile_photos",
        )
        self.action_types = [x.strip() for x in action_types.split(",") if x.strip()]
        self.max_inboxes_per_domain = max(1, int(os.getenv("GOOGLE_MAX_INBOXES_PER_DOMAIN", "5")))

        self.partnerhub_base_url = os.getenv("PARTNERHUB_API_BASE_URL", "https://partnerhubapi.netstager.com/api")
        self.partnerhub_api_key = os.getenv("PARTNERHUB_API_KEY") or os.getenv("GOOGLE_NETSTAGER_API_KEY") or ""
        self.partnerhub_plan_id = os.getenv("PARTNERHUB_PLAN_ID", "94c835bb-a675-4249-8fba-e95cdb2ca4ed")
        self.profile_photo_optional = self._as_bool(os.getenv("GOOGLE_PROFILE_PHOTO_OPTIONAL"), default=False)
        self.require_mfa_enrollment = self._as_bool(
            os.getenv("GOOGLE_REQUIRE_MFA_ENROLLMENT"),
            default=True,
        )
        self.mfa_non_headless_fallback = self._as_bool(
            os.getenv("GOOGLE_MFA_NON_HEADLESS_FALLBACK"),
            default=True,
        )
        self.mfa_non_headless_max_attempts = max(1, int(os.getenv("GOOGLE_MFA_NON_HEADLESS_MAX_ATTEMPTS", "1")))
        self.playwright_headless = self._as_bool(
            os.getenv("GOOGLE_PLAYWRIGHT_HEADLESS", os.getenv("GOOGLE_SELENIUM_HEADLESS", "true")),
            default=True,
        )

        self._partnerhub: Optional[PartnerHubClient] = None

    def run_forever(self) -> None:
        logger.info(
            "Google inbox lifecycle worker started (types=%s, poll=%.1fs)",
            self.action_types,
            self.poll_interval_seconds,
        )
        while True:
            try:
                processed = self._poll_once()
                if processed == 0:
                    time.sleep(self.poll_interval_seconds)
            except Exception:
                logger.exception("Google lifecycle poll failed")
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
        action_id = str(action.get("id"))
        action_type = str(action.get("type") or "").strip().lower()
        action_logger = get_order_logger(action_id)
        payload = action.get("payload") or {}
        mutation_submission_id = str(payload.get("mutation_submission_id") or "").strip()
        mutation_request_id = str(payload.get("mutation_request_id") or "").strip()
        prior_result = action.get("result") or {}
        prior_steps_raw = prior_result.get("steps") if isinstance(prior_result, dict) else []
        if not isinstance(prior_steps_raw, list):
            prior_steps_raw = []

        domain_id: Optional[str] = action.get("domain_id")
        steps: List[Dict[str, Any]] = [s for s in prior_steps_raw if isinstance(s, dict)]
        last_step_status: Dict[str, str] = {}
        last_step_details: Dict[str, Dict[str, Any]] = {}
        for row in steps:
            step_name = str(row.get("step") or "").strip()
            if not step_name:
                continue
            last_step_status[step_name] = str(row.get("status") or "").strip().lower()
            details = row.get("details")
            if isinstance(details, dict):
                last_step_details[step_name] = details

        def start_step(step_name: str) -> Dict[str, Any]:
            step = {"step": step_name, "status": "in_progress", "startedAt": self._iso_now()}
            steps.append(step)
            if mutation_request_id:
                try:
                    self.client.update_mutation_request(
                        mutation_request_id,
                        {"current_step": step_name},
                    )
                except Exception:
                    pass
            return step

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
                skip_step(step_name, "Resumed from previous completed step")
                log_event(
                    "step_resumed",
                    "info",
                    f"[{step_name}] Reusing checkpoint from previous attempt.",
                    {"details": details},
                )
                return details
            return None

        def persist_progress() -> None:
            try:
                self.client.update_action(
                    action,
                    {
                        "result": {
                            "steps": steps,
                            "lastUpdated": self._iso_now(),
                        }
                    },
                )
            except Exception:
                pass

        def log_event(
            event_type: str,
            severity: str,
            message: str,
            metadata: Optional[Dict[str, Any]] = None,
        ) -> None:
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
            if mutation_request_id:
                try:
                    self.client.insert_mutation_event(
                        {
                            "submission_id": mutation_submission_id or None,
                            "request_id": mutation_request_id,
                            "domain_id": action.get("domain_id"),
                            "inbox_id": action.get("inbox_id"),
                            "event_type": event_type,
                            "severity": severity,
                            "message": message,
                            "metadata": metadata or {},
                        }
                    )
                except Exception:
                    pass

        try:
            if not domain_id:
                raise ValueError("Action missing domain_id")

            domain = self.client.get_domain(domain_id)
            if not domain:
                raise ValueError(f"Domain {domain_id} not found")

            domain_name = str(domain.get("domain") or "").strip().lower()
            if not domain_name:
                raise ValueError("Domain value is missing")

            provider = str(domain.get("provider") or "").strip().lower()
            if provider != "google":
                result = {"skipped": True, "reason": "Domain provider is not google"}
                self.client.complete_action(action, result)
                log_event("action_completed", "info", "Skipped non-google domain", result)
                return

            mutation_items = self.client.get_mutation_items(mutation_request_id) if mutation_request_id else []
            mutation_items_by_inbox_id = {
                str(row.get("inbox_id") or "").strip(): row for row in mutation_items if str(row.get("inbox_id") or "").strip()
            }
            if mutation_request_id:
                now = self._iso_now()
                try:
                    self.client.update_mutation_request(
                        mutation_request_id,
                        {
                            "status": "processing",
                            "started_at": now,
                            "current_step": action_type,
                            "last_error": None,
                            "failed_at": None,
                        },
                    )
                    for item in mutation_items:
                        item_id = str(item.get("id") or "").strip()
                        if not item_id:
                            continue
                        self.client.update_mutation_item(
                            item_id,
                            {
                                "status": "processing",
                                "started_at": now,
                                "last_error": None,
                                "failed_at": None,
                            },
                        )
                    if mutation_submission_id:
                        self.client.refresh_mutation_submission(mutation_submission_id)
                except Exception:
                    pass

            log_event(
                "action_started",
                "info",
                f"Processing {action_type} for {domain_name}",
                {"domain": domain_name, "type": action_type},
            )

            if action_type == "google_add_inboxes":
                summary = self._handle_add(action, payload, domain, checkpoint, start_step, complete_step, fail_step, persist_progress, log_event)
            elif action_type == "google_remove_inboxes":
                summary = self._handle_remove(action, payload, domain, checkpoint, start_step, complete_step, fail_step, persist_progress, log_event)
            elif action_type == "google_update_inboxes":
                summary = self._handle_update(
                    action,
                    payload,
                    domain,
                    checkpoint,
                    start_step,
                    complete_step,
                    fail_step,
                    persist_progress,
                    log_event,
                    mutation_request_id=mutation_request_id,
                    mutation_submission_id=mutation_submission_id,
                    mutation_items_by_inbox_id=mutation_items_by_inbox_id,
                )
            elif action_type == "google_update_profile_photos":
                summary = self._handle_profile_photos(
                    action,
                    payload,
                    domain,
                    checkpoint,
                    start_step,
                    complete_step,
                    fail_step,
                    persist_progress,
                    log_event,
                )
            else:
                summary = {"skipped": True, "reason": f"Unsupported action type {action_type}"}

            result = {
                "type": action_type,
                "domain": domain_name,
                "summary": summary,
                "steps": steps,
            }
            self.client.complete_action(action, result)
            if mutation_request_id:
                try:
                    self.client.update_mutation_request(
                        mutation_request_id,
                        {
                            "status": "completed",
                            "current_step": "completed",
                            "last_error": None,
                            "completed_at": self._iso_now(),
                            "failed_at": None,
                            "retry_count": max(0, int(action.get("attempts") or 1) - 1),
                        },
                    )
                    if mutation_submission_id:
                        self.client.refresh_mutation_submission(mutation_submission_id)
                except Exception:
                    pass
            log_event("action_completed", "info", f"{action_type} completed", summary)
        except Exception as exc:
            message = str(exc)
            action_logger.exception("Lifecycle action failed")
            self.client.fail_action(action, message, max_retries=self.max_retries)
            if mutation_request_id:
                try:
                    attempts = int(action.get("attempts") or 1)
                    is_final = attempts >= self.max_retries
                    request_status = "failed" if is_final else "needs_attention"
                    self.client.update_mutation_request(
                        mutation_request_id,
                        {
                            "status": request_status,
                            "current_step": str(steps[-1].get("step") or action_type) if steps else action_type,
                            "last_error": message,
                            "failed_at": self._iso_now(),
                            "retry_count": attempts,
                        },
                    )
                    for item in self.client.get_mutation_items(mutation_request_id):
                        item_id = str(item.get("id") or "").strip()
                        if not item_id:
                            continue
                        item_status = str(item.get("status") or "").strip().lower()
                        if item_status == "completed":
                            continue
                        self.client.update_mutation_item(
                            item_id,
                            {
                                "status": "failed" if is_final else "processing",
                                "last_error": message,
                                "failed_at": self._iso_now() if is_final else None,
                            },
                        )
                    if mutation_submission_id:
                        self.client.refresh_mutation_submission(mutation_submission_id)
                except Exception:
                    pass
            log_event(
                "action_failed",
                "error",
                f"{action_type} failed: {message}",
                {"error": message, "steps": steps},
            )

    def _handle_add(
        self,
        action: Dict[str, Any],
        payload: Dict[str, Any],
        domain: Dict[str, Any],
        checkpoint,
        start_step,
        complete_step,
        fail_step,
        persist_progress,
        log_event,
    ) -> Dict[str, Any]:
        domain_id = str(domain.get("id"))
        domain_name = str(domain.get("domain") or "").strip().lower()
        target_ids = [str(v).strip() for v in (payload.get("inbox_ids") or []) if str(v or "").strip()]
        if not target_ids:
            raise RuntimeError("google_add_inboxes payload must include inbox_ids")

        target_info = checkpoint("resolve_targets")
        if target_info:
            resolved_ids = [str(v) for v in (target_info.get("resolved_ids") or [])]
        else:
            step = start_step("resolve_targets")
            all_inboxes = self.client.get_domain_inboxes_all(domain_id)
            by_id = {str(row.get("id")): row for row in all_inboxes}
            resolved = [by_id[i] for i in target_ids if i in by_id]
            if not resolved:
                fail_step(step, "No matching inbox rows found for inbox_ids")
                persist_progress()
                raise RuntimeError("No matching inbox rows found for inbox_ids")

            current_live = [r for r in all_inboxes if str(r.get("status") or "").lower() in LIVE_STATUSES]
            pending_to_activate = [
                r for r in resolved if str(r.get("status") or "").lower() not in LIVE_STATUSES
            ]
            projected_live = len(current_live) + len(pending_to_activate)
            if projected_live > self.max_inboxes_per_domain:
                fail_step(
                    step,
                    f"Domain would have {projected_live} live inboxes (max={self.max_inboxes_per_domain})",
                )
                persist_progress()
                raise RuntimeError(
                    f"Domain {domain_name} exceeds max inboxes ({projected_live} > {self.max_inboxes_per_domain})"
                )

            shared_password = self._resolve_domain_shared_password(all_inboxes, resolved)
            for row in resolved:
                update_payload: Dict[str, Any] = {
                    "status": "provisioning",
                    "is_admin": bool(row.get("is_admin")),
                    "password": shared_password,
                }
                self.client.update_inbox(str(row["id"]), update_payload)

            resolved_ids = [str(row["id"]) for row in resolved]
            complete_step(step, {"resolved_ids": resolved_ids})
            persist_progress()

        resolved_rows = self.client.get_inboxes_by_ids(domain_id, resolved_ids)

        order_info = checkpoint("resolve_partnerhub_order")
        if order_info:
            order_id = str(order_info.get("order_id") or "")
        else:
            step = start_step("resolve_partnerhub_order")
            order_payload = self._get_partnerhub().get_order_details(domain_name)
            order_id = str(self._get_partnerhub().extract_order_id(order_payload) or "")
            if not order_id:
                fail_step(step, f"Could not resolve PartnerHub order id for {domain_name}")
                persist_progress()
                raise RuntimeError(f"Could not resolve PartnerHub order id for {domain_name}")
            complete_step(step, {"order_id": order_id})
            persist_progress()

        sync_info = checkpoint("sync_partnerhub_add")
        if sync_info:
            synced_count = int(sync_info.get("user_count") or 0)
        else:
            step = start_step("sync_partnerhub_add")
            users = self._build_partnerhub_users(
                domain_name,
                resolved_rows,
                default_user_type="normal",
            )
            if not users:
                fail_step(step, "No users to add for google_add_inboxes")
                persist_progress()
                raise RuntimeError("No users to add for google_add_inboxes")

            organization_name = (
                str(payload.get("organization_name") or "").strip()
                or str(payload.get("client_name") or "").strip()
                or domain_name
            )
            plan_id = str(payload.get("plan_id") or self.partnerhub_plan_id)

            add_response = self._get_partnerhub().add_license_users(
                order_id=order_id,
                license_users=users,
                domain=domain_name,
                organization_name=organization_name,
                plan_id=plan_id,
            )
            synced_count = len(users)
            complete_step(step, {"user_count": synced_count, "order_id": order_id})
            persist_progress()
            log_event(
                "step_completed",
                "info",
                f"[sync_partnerhub_add] added {synced_count} users to order {order_id}",
                add_response,
            )

        mfa_summary = checkpoint("mfa_enrollment_summary") or {}
        if not mfa_summary:
            mfa_users = self._build_mfa_users(domain_name, resolved_rows)
            if not mfa_users:
                mfa_summary = {"completed": 0, "failed": 0, "skipped": "No users eligible for MFA setup"}
            else:
                email_to_inbox_id: Dict[str, str] = {}
                for row in resolved_rows:
                    inbox_id = str(row.get("id") or "").strip()
                    username = str(row.get("username") or "").strip().lower()
                    email_key = str(row.get("email") or f"{username}@{domain_name}").strip().lower()
                    if inbox_id and email_key:
                        email_to_inbox_id[email_key] = inbox_id

                try:
                    op_client = OnePasswordCliClient.from_env()
                except Exception as op_exc:
                    if self.require_mfa_enrollment:
                        raise RuntimeError(f"1Password is not configured: {op_exc}") from op_exc
                    mfa_summary = {"completed": 0, "failed": 0, "skipped": str(op_exc)}
                if not mfa_summary:
                    mfa_summary = {"completed": 0, "failed": 0, "failed_emails": []}
                    with GoogleAdminPlaywrightClient(headless=self.playwright_headless) as browser:
                        for mfa_user in mfa_users:
                            step_name = self._mfa_step_name(mfa_user.email)
                            if checkpoint(step_name):
                                mfa_summary["completed"] += 1
                                continue
                            step = start_step(step_name)
                            result = browser.enroll_users_mfa_with_1password(
                                [mfa_user],
                                op_client,
                            )
                            details = result.get("results") or {}
                            row = details.get(mfa_user.email) or details.get(mfa_user.email.lower()) or {}
                            if int(result.get("failed") or 0) > 0:
                                if self.playwright_headless and self.mfa_non_headless_fallback:
                                    try:
                                        log_event(
                                            "step_warning",
                                            "warn",
                                            f"[{step_name}] Headless MFA failed; retrying non-headless fallback.",
                                            {"email": mfa_user.email},
                                        )
                                        with GoogleAdminPlaywrightClient(headless=False) as fallback_browser:
                                            fallback = fallback_browser.enroll_users_mfa_with_1password(
                                                [mfa_user],
                                                op_client,
                                                max_attempts=self.mfa_non_headless_max_attempts,
                                            )
                                        fallback_details = fallback.get("results") or {}
                                        fallback_row = (
                                            fallback_details.get(mfa_user.email)
                                            or fallback_details.get(mfa_user.email.lower())
                                            or {}
                                        )
                                        if int(fallback.get("failed") or 0) == 0:
                                            result = fallback
                                            row = fallback_row
                                        else:
                                            row = {
                                                "status": "failed",
                                                "error": str(
                                                    fallback_row.get("error")
                                                    or row.get("error")
                                                    or "Unknown MFA enrollment error"
                                                ),
                                            }
                                    except Exception as fallback_exc:
                                        row = {
                                            "status": "failed",
                                            "error": f"{row.get('error') or 'Headless MFA failed'} | fallback_error={fallback_exc}",
                                        }

                            if str(row.get("status") or "").lower() == "failed":
                                message = str(row.get("error") or "Unknown MFA enrollment error")
                                fail_step(step, message)
                                persist_progress()
                                if mfa_user.email not in mfa_summary["failed_emails"]:
                                    mfa_summary["failed_emails"].append(mfa_user.email)
                                mfa_summary["failed"] += 1
                            else:
                                complete_step(
                                    step,
                                    {
                                        "email": mfa_user.email,
                                        "status": row.get("status"),
                                        "item_id": row.get("item_id"),
                                    },
                                )
                                inbox_id = email_to_inbox_id.get(str(mfa_user.email or "").strip().lower())
                                if inbox_id:
                                    inbox_updates: Dict[str, Any] = {}
                                    item_id = str(row.get("item_id") or "").strip()
                                    if item_id:
                                        inbox_updates["onepassword_item_id"] = item_id
                                    otp_secret = str(row.get("otp_secret") or "").strip()
                                    if otp_secret:
                                        inbox_updates["otp_secret"] = otp_secret
                                    if inbox_updates:
                                        try:
                                            self.client.update_inbox(inbox_id, inbox_updates)
                                        except Exception as inbox_exc:
                                            log_event(
                                                "step_warning",
                                                "warn",
                                                f"[{step_name}] Failed to persist MFA fields for {mfa_user.email}",
                                                {"email": mfa_user.email, "error": str(inbox_exc)},
                                            )
                                persist_progress()
                                mfa_summary["completed"] += 1

                    if mfa_summary.get("failed", 0) > 0 and self.require_mfa_enrollment:
                        raise RuntimeError(
                            f"MFA enrollment failed for users: {sorted(mfa_summary.get('failed_emails') or [])}"
                        )

            step = start_step("mfa_enrollment_summary")
            complete_step(step, mfa_summary)
            persist_progress()

        checkpoint_data = checkpoint("finalize_add")
        if checkpoint_data:
            return checkpoint_data

        step = start_step("finalize_add")
        for inbox_id in resolved_ids:
            inbox = self.client.get_inboxes_by_ids(domain_id, [inbox_id])
            if not inbox:
                continue
            row = inbox[0]
            username = str(row.get("username") or "").strip().lower()
            updates: Dict[str, Any] = {
                "status": "active",
                "email": f"{username}@{domain_name}",
                "is_admin": bool(row.get("is_admin")),
            }
            if not row.get("password"):
                updates["password"] = self._generate_password()
            self.client.update_inbox(inbox_id, updates)

        live_rows = [
            row
            for row in self.client.get_domain_inboxes_all(domain_id)
            if str(row.get("status") or "").lower() in LIVE_STATUSES
        ]
        admin_rows = [row for row in live_rows if bool(row.get("is_admin"))]
        reassigned_admin_inbox_id: Optional[str] = None
        if len(admin_rows) > 1:
            ordered_admin = sorted(admin_rows, key=lambda r: (str(r.get("created_at") or ""), str(r.get("id") or "")))
            keeper_id = str(ordered_admin[0].get("id") or "").strip()
            for row in ordered_admin[1:]:
                rid = str(row.get("id") or "").strip()
                if rid:
                    self.client.update_inbox(rid, {"is_admin": False})
            reassigned_admin_inbox_id = keeper_id
        elif not admin_rows and live_rows:
            ordered_live = sorted(live_rows, key=lambda r: (str(r.get("created_at") or ""), str(r.get("id") or "")))
            candidate_id = str(ordered_live[0].get("id") or "").strip()
            if candidate_id:
                self.client.update_inbox(candidate_id, {"is_admin": True})
                reassigned_admin_inbox_id = candidate_id

        result = {
            "added": len(resolved_ids),
            "synced_users": synced_count,
            "mfa": mfa_summary,
            "reassigned_admin_inbox_id": reassigned_admin_inbox_id,
        }
        complete_step(step, result)
        persist_progress()
        return result

    def _handle_remove(
        self,
        action: Dict[str, Any],
        payload: Dict[str, Any],
        domain: Dict[str, Any],
        checkpoint,
        start_step,
        complete_step,
        fail_step,
        persist_progress,
        log_event,
    ) -> Dict[str, Any]:
        domain_id = str(domain.get("id"))
        domain_name = str(domain.get("domain") or "").strip().lower()
        target_ids = [str(v).strip() for v in (payload.get("inbox_ids") or []) if str(v or "").strip()]
        if not target_ids:
            raise RuntimeError("google_remove_inboxes payload must include inbox_ids")

        target_info = checkpoint("resolve_targets")
        if target_info:
            resolved_ids = [str(v) for v in (target_info.get("resolved_ids") or [])]
            removed_admin_ids = [str(v) for v in (target_info.get("removed_admin_ids") or [])]
        else:
            step = start_step("resolve_targets")
            all_inboxes = self.client.get_domain_inboxes_all(domain_id)
            live_rows = [row for row in all_inboxes if str(row.get("status") or "").lower() in LIVE_STATUSES]
            by_id = {str(row.get("id")): row for row in live_rows}
            resolved = [by_id[i] for i in target_ids if i in by_id]
            if not resolved:
                fail_step(step, "No matching live inboxes found for removal")
                persist_progress()
                raise RuntimeError("No matching live inboxes found for removal")

            if len(live_rows) - len(resolved) < 1:
                fail_step(step, "Cannot remove all inboxes from a domain")
                persist_progress()
                raise RuntimeError("Cannot remove all inboxes from a domain")

            removed_admin_ids = [
                str(row.get("id") or "").strip()
                for row in resolved
                if bool(row.get("is_admin"))
            ]
            for row in resolved:
                self.client.update_inbox(
                    str(row["id"]),
                    {"status": "provisioning", "is_admin": bool(row.get("is_admin"))},
                )

            resolved_ids = [str(row["id"]) for row in resolved]
            complete_step(
                step,
                {
                    "resolved_ids": resolved_ids,
                    "removed_admin_ids": removed_admin_ids,
                },
            )
            persist_progress()

        sync_info = checkpoint("suspend_google_users")
        if sync_info:
            synced_count = int(sync_info.get("suspended") or 0)
        else:
            step = start_step("suspend_google_users")
            rows = self.client.get_inboxes_by_ids(domain_id, resolved_ids)
            emails = [
                str(row.get("email") or f"{str(row.get('username') or '').strip().lower()}@{domain_name}")
                .strip()
                .lower()
                for row in rows
                if str(row.get("email") or row.get("username") or "").strip()
            ]
            if emails:
                with GoogleAdminPlaywrightClient(headless=self.playwright_headless) as browser:
                    suspend_result = browser.suspend_users(emails)
            else:
                suspend_result = {"suspended": 0, "failed": 0}

            if int(suspend_result.get("failed") or 0) > 0:
                fail_step(step, f"Selenium suspend failed for {suspend_result.get('failed')} user(s)")
                persist_progress()
                raise RuntimeError(f"Selenium suspend failed: {suspend_result}")

            synced_count = int(suspend_result.get("suspended") or 0)
            complete_step(step, {"suspended": synced_count})
            persist_progress()
            log_event(
                "step_completed",
                "info",
                f"[suspend_google_users] suspended {synced_count} users",
                suspend_result,
            )

        checkpoint_data = checkpoint("finalize_remove")
        if checkpoint_data:
            return checkpoint_data

        step = start_step("finalize_remove")
        for inbox_id in resolved_ids:
            self.client.update_inbox(inbox_id, {"status": TERMINAL_STATUS, "is_admin": False})

        live_rows = [
            row
            for row in self.client.get_domain_inboxes_all(domain_id)
            if str(row.get("status") or "").lower() in LIVE_STATUSES
        ]
        admin_rows = [row for row in live_rows if bool(row.get("is_admin"))]
        reassigned_admin_inbox_id: Optional[str] = None

        if len(admin_rows) > 1:
            ordered_admin = sorted(admin_rows, key=lambda r: (str(r.get("created_at") or ""), str(r.get("id") or "")))
            keeper_id = str(ordered_admin[0].get("id") or "").strip()
            for row in ordered_admin[1:]:
                rid = str(row.get("id") or "").strip()
                if rid:
                    self.client.update_inbox(rid, {"is_admin": False})
            admin_rows = [ordered_admin[0]]
            reassigned_admin_inbox_id = keeper_id

        if not admin_rows and live_rows:
            ordered_live = sorted(live_rows, key=lambda r: (str(r.get("created_at") or ""), str(r.get("id") or "")))
            candidate_id = str(ordered_live[0].get("id") or "").strip()
            if candidate_id:
                self.client.update_inbox(candidate_id, {"is_admin": True})
                reassigned_admin_inbox_id = candidate_id

        result = {
            "removed": len(resolved_ids),
            "synced_users": synced_count,
            "removed_admin_ids": removed_admin_ids,
            "reassigned_admin_inbox_id": reassigned_admin_inbox_id,
        }
        complete_step(step, result)
        persist_progress()
        return result

    def _handle_update(
        self,
        action: Dict[str, Any],
        payload: Dict[str, Any],
        domain: Dict[str, Any],
        checkpoint,
        start_step,
        complete_step,
        fail_step,
        persist_progress,
        log_event,
        mutation_request_id: str = "",
        mutation_submission_id: str = "",
        mutation_items_by_inbox_id: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        domain_id = str(domain.get("id"))
        domain_name = str(domain.get("domain") or "").strip().lower()
        raw_updates = payload.get("updates") or []
        raw_photo_updates = payload.get("photo_updates") or []
        if not isinstance(raw_updates, list) or not raw_updates:
            raise RuntimeError("google_update_inboxes payload must include updates[]")
        if not isinstance(raw_photo_updates, list):
            raw_photo_updates = []
        mutation_items_by_inbox_id = mutation_items_by_inbox_id or {}

        resolved_checkpoint = checkpoint("apply_local_updates")
        if resolved_checkpoint:
            affected_ids = [str(v) for v in (resolved_checkpoint.get("affected_ids") or [])]
            all_target_ids = [str(v) for v in (resolved_checkpoint.get("all_target_ids") or affected_ids)]
            change_rows = [row for row in (resolved_checkpoint.get("changes") or []) if isinstance(row, dict)]
            photo_updates = [row for row in (resolved_checkpoint.get("photo_updates") or []) if isinstance(row, dict)]
        else:
            step = start_step("apply_local_updates")
            all_inboxes = self.client.get_domain_inboxes_all(domain_id)
            live_rows = [row for row in all_inboxes if str(row.get("status") or "").lower() in LIVE_STATUSES]
            by_id = {str(row.get("id")): row for row in live_rows}
            by_username = {str(row.get("username") or "").strip().lower(): row for row in live_rows}

            affected_ids: List[str] = []
            change_rows: List[Dict[str, str]] = []
            photo_updates: List[Dict[str, str]] = []
            all_target_id_set = set()
            for row in raw_updates:
                if not isinstance(row, dict):
                    continue
                target = None
                if row.get("inbox_id"):
                    target = by_id.get(str(row.get("inbox_id")))
                if target is None and row.get("username"):
                    target = by_username.get(str(row.get("username") or "").strip().lower())
                if target is None:
                    raise RuntimeError(f"Update target not found for {row}")

                inbox_id = str(target.get("id"))
                username = str(target.get("username") or "").strip().lower()
                old_email = str(target.get("email") or f"{username}@{domain_name}").strip().lower()
                old_first_name = str(target.get("first_name") or "").strip()
                old_last_name = str(target.get("last_name") or "").strip()
                updates: Dict[str, Any] = {"status": "provisioning"}

                new_username = str(row.get("username") or "").strip().lower()
                if new_username and new_username != username:
                    updates["username"] = new_username
                    updates["email"] = f"{new_username}@{domain_name}"
                new_first_name = old_first_name
                if row.get("first_name") is not None:
                    new_first_name = str(row.get("first_name") or "").strip()
                    updates["first_name"] = new_first_name
                new_last_name = old_last_name
                if row.get("last_name") is not None:
                    new_last_name = str(row.get("last_name") or "").strip()
                    updates["last_name"] = new_last_name
                profile_pic_url = ""
                if row.get("profile_pic_url") is not None:
                    profile_pic_url = str(row.get("profile_pic_url") or "").strip()
                    updates["profile_pic_url"] = profile_pic_url

                self.client.update_inbox(inbox_id, updates)
                affected_ids.append(inbox_id)
                all_target_id_set.add(inbox_id)
                new_email = str(updates.get("email") or old_email).strip().lower()
                username_changed = bool(new_email and new_email != old_email)
                name_changed = bool(
                    new_first_name != old_first_name
                    or new_last_name != old_last_name
                )
                change_rows.append(
                    {
                        "inbox_id": inbox_id,
                        "old_email": old_email,
                        "new_email": new_email,
                        "old_first_name": old_first_name,
                        "old_last_name": old_last_name,
                        "first_name": new_first_name,
                        "last_name": new_last_name,
                        "profile_pic_url": profile_pic_url,
                        "username_changed": username_changed,
                        "name_changed": name_changed,
                    }
                )
                if profile_pic_url:
                    photo_updates.append(
                        {
                            "inbox_id": inbox_id,
                            "email": new_email,
                            "profile_pic_url": profile_pic_url,
                        }
                    )

            for row in raw_photo_updates:
                if not isinstance(row, dict):
                    continue
                inbox_id = str(row.get("inbox_id") or "").strip()
                email = str(row.get("email") or "").strip().lower()
                profile_pic_url = str(row.get("profile_pic_url") or "").strip()
                if not inbox_id or not email or not profile_pic_url:
                    continue
                if inbox_id in all_target_id_set:
                    continue
                all_target_id_set.add(inbox_id)
                photo_updates.append(
                    {
                        "inbox_id": inbox_id,
                        "email": email,
                        "profile_pic_url": profile_pic_url,
                    }
                )

            if not affected_ids:
                fail_step(step, "No valid updates were provided")
                persist_progress()
                raise RuntimeError("No valid updates were provided")

            all_target_ids = sorted(all_target_id_set)
            complete_step(
                step,
                {
                    "affected_ids": affected_ids,
                    "all_target_ids": all_target_ids,
                    "changes": change_rows,
                    "photo_updates": photo_updates,
                },
            )
            persist_progress()

        admin_checkpoint = checkpoint("resolve_admin_login")
        if admin_checkpoint:
            admin_inbox_id = str(admin_checkpoint.get("admin_inbox_id") or "").strip()
        else:
            step = start_step("resolve_admin_login")
            rows = self.client.get_domain_inboxes_all(domain_id)
            live_rows = [row for row in rows if str(row.get("status") or "").lower() in LIVE_STATUSES]
            admin_candidates = [
                row
                for row in live_rows
                if bool(row.get("is_admin"))
                and str(row.get("email") or "").strip()
                and str(row.get("password") or "").strip()
            ]
            if not admin_candidates:
                fallback_candidates = [
                    row
                    for row in live_rows
                    if str(row.get("email") or "").strip() and str(row.get("password") or "").strip()
                ]
                if fallback_candidates:
                    fallback_candidates = sorted(
                        fallback_candidates,
                        key=lambda r: (str(r.get("created_at") or ""), str(r.get("id") or "")),
                    )
                    admin_candidates = [fallback_candidates[0]]
                    self.client.update_inbox(str(fallback_candidates[0].get("id")), {"is_admin": True})
                    log_event(
                        "step_warning",
                        "warn",
                        f"[resolve_admin_login] Domain {domain_name} had no admin flagged; promoted fallback inbox.",
                        {
                            "domain": domain_name,
                            "fallback_admin_email": str(fallback_candidates[0].get("email") or "").strip().lower(),
                        },
                    )

            if not admin_candidates:
                fail_step(step, f"No admin-capable inbox found for {domain_name}")
                persist_progress()
                raise RuntimeError(
                    f"No admin-capable inbox found for {domain_name}. "
                    "Ensure at least one live inbox has is_admin=true, email, and password."
                )

            admin_row = sorted(
                admin_candidates,
                key=lambda r: (str(r.get("created_at") or ""), str(r.get("id") or "")),
            )[0]
            admin_inbox_id = str(admin_row.get("id") or "").strip()
            admin_email = str(admin_row.get("email") or "").strip().lower()
            complete_step(
                step,
                {
                    "admin_inbox_id": admin_inbox_id,
                    "admin_email": admin_email,
                },
            )
            persist_progress()

        admin_rows = self.client.get_inboxes_by_ids(domain_id, [admin_inbox_id]) if admin_inbox_id else []
        if not admin_rows:
            raise RuntimeError(f"Admin inbox {admin_inbox_id} not found for domain {domain_name}")
        admin_email = str(admin_rows[0].get("email") or "").strip().lower()
        admin_password = str(admin_rows[0].get("password") or "").strip()
        if not admin_email or not admin_password:
            raise RuntimeError(
                f"Admin inbox {admin_inbox_id} for {domain_name} is missing email/password in Supabase"
            )

        sync_info = checkpoint("update_google_users")
        if sync_info:
            synced_count = int(sync_info.get("updated") or 0)
            username_updates = int(sync_info.get("username_updates") or 0)
            name_only_updates = int(sync_info.get("name_only_updates") or 0)
            onepassword_updated = int(sync_info.get("onepassword_updated") or 0)
            onepassword_missing = int(sync_info.get("onepassword_missing") or 0)
            onepassword_updated_ids = {
                str(v).strip() for v in (sync_info.get("onepassword_updated_ids") or []) if str(v).strip()
            }
        else:
            step = start_step("update_google_users")
            rows_by_id = {
                str(row.get("id")): row for row in self.client.get_inboxes_by_ids(domain_id, all_target_ids)
            }
            updates_payload: List[Dict[str, str]] = []
            for change in change_rows:
                inbox_id = str(change.get("inbox_id") or "")
                row = rows_by_id.get(inbox_id) or {}
                old_email = str(change.get("old_email") or "").strip().lower()
                new_email = str(change.get("new_email") or "").strip().lower()
                if not old_email or not new_email:
                    continue
                updates_payload.append(
                    {
                        "inbox_id": inbox_id,
                        "old_email": old_email,
                        "new_email": new_email,
                        "old_first_name": str(change.get("old_first_name") or row.get("first_name") or "").strip(),
                        "old_last_name": str(change.get("old_last_name") or row.get("last_name") or "").strip(),
                        "first_name": str(change.get("first_name") or row.get("first_name") or "").strip(),
                        "last_name": str(change.get("last_name") or row.get("last_name") or "").strip(),
                        "username_changed": bool(change.get("username_changed")),
                    }
                )

            op_client: Optional[OnePasswordCliClient] = None
            try:
                op_client = OnePasswordCliClient.from_env()
            except Exception as op_exc:
                log_event(
                    "step_warning",
                    "warn",
                    "[update_google_users] 1Password not configured; will skip 1Password username retitle",
                    {"error": str(op_exc)},
                )

            if updates_payload:
                with GoogleAdminPlaywrightClient(headless=self.playwright_headless) as browser:
                    browser._login_google_account(admin_email, admin_password, onepassword=op_client)
                    sync_response = browser.update_users(updates_payload)
            else:
                sync_response = {"updated": 0, "failed": 0, "username_updated": 0, "name_only_updated": 0}

            if int(sync_response.get("failed") or 0) > 0:
                fail_step(step, f"Playwright update failed for {sync_response.get('failed')} user(s)")
                persist_progress()
                raise RuntimeError(f"Playwright update users failed: {sync_response}")

            synced_count = int(sync_response.get("updated") or 0)
            username_updates = int(sync_response.get("username_updated") or 0)
            name_only_updates = int(sync_response.get("name_only_updated") or 0)

            onepassword_updated = 0
            onepassword_missing = 0
            onepassword_updated_ids = set()
            if op_client is not None:
                for payload_item in updates_payload:
                    if not bool(payload_item.get("username_changed")):
                        continue
                    inbox_id = str(payload_item.get("inbox_id") or "").strip()
                    row = rows_by_id.get(inbox_id) or {}
                    new_email = str(payload_item.get("new_email") or "").strip().lower()
                    old_email = str(payload_item.get("old_email") or "").strip().lower()
                    previous_name = " ".join(
                        [
                            str(payload_item.get("old_first_name") or "").strip(),
                            str(payload_item.get("old_last_name") or "").strip(),
                        ]
                    ).strip()
                    current_name = " ".join(
                        [
                            str(payload_item.get("first_name") or "").strip(),
                            str(payload_item.get("last_name") or "").strip(),
                        ]
                    ).strip()
                    item_id = str(row.get("onepassword_item_id") or "").strip()
                    if not item_id and old_email:
                        existing = op_client.find_google_login_item(old_email)
                        item_id = str((existing or {}).get("id") or "").strip()
                        if item_id:
                            try:
                                self.client.update_inbox(inbox_id, {"onepassword_item_id": item_id})
                            except Exception:
                                pass

                    if not item_id:
                        onepassword_missing += 1
                        continue

                    op_client.update_google_login_identity_by_item_id(
                        item_id=item_id,
                        email=new_email,
                        username=new_email,
                        password=None,
                        previous_name=previous_name,
                        previous_username=old_email.split("@")[0],
                        previous_email=old_email,
                        current_name=current_name,
                        current_username=new_email.split("@")[0],
                        current_email=new_email,
                    )
                    onepassword_updated += 1
                    onepassword_updated_ids.add(inbox_id)

            complete_step(
                step,
                {
                    "updated": synced_count,
                    "username_updates": username_updates,
                    "name_only_updates": name_only_updates,
                    "onepassword_updated": onepassword_updated,
                    "onepassword_missing": onepassword_missing,
                    "onepassword_updated_ids": sorted(onepassword_updated_ids),
                },
            )
            persist_progress()
            log_event(
                "step_completed",
                "info",
                f"[update_google_users] updated {synced_count} users",
                sync_response,
            )

        photo_checkpoint = checkpoint("update_profile_photos")
        if photo_checkpoint:
            photo_uploaded = int(photo_checkpoint.get("uploaded") or 0)
            photo_failed = int(photo_checkpoint.get("failed") or 0)
        else:
            step = start_step("update_profile_photos")
            deduped_photo_updates: List[Tuple[str, str, str]] = []
            seen_photo_targets = set()
            for photo_update in photo_updates:
                inbox_id = str(photo_update.get("inbox_id") or "").strip()
                email = str(photo_update.get("email") or "").strip().lower()
                profile_pic_url = str(photo_update.get("profile_pic_url") or "").strip()
                if not inbox_id or not email or not profile_pic_url:
                    continue
                key = (inbox_id, email)
                if key in seen_photo_targets:
                    continue
                seen_photo_targets.add(key)
                deduped_photo_updates.append((inbox_id, email, profile_pic_url))

            if deduped_photo_updates:
                op_client = None
                try:
                    op_client = OnePasswordCliClient.from_env()
                except Exception:
                    op_client = None
                with GoogleAdminPlaywrightClient(headless=self.playwright_headless) as browser:
                    browser._login_google_account(admin_email, admin_password, onepassword=op_client)
                    photo_response = browser.upload_profile_photos(
                        [(email, profile_pic_url) for _, email, profile_pic_url in deduped_photo_updates]
                    )
            else:
                photo_response = {"uploaded": 0, "failed": 0}

            if int(photo_response.get("failed") or 0) > 0 and not self.profile_photo_optional:
                fail_step(step, f"Playwright profile photo upload failed for {photo_response.get('failed')} user(s)")
                persist_progress()
                raise RuntimeError(f"Playwright profile photo upload failed: {photo_response}")

            photo_uploaded = int(photo_response.get("uploaded") or 0)
            photo_failed = int(photo_response.get("failed") or 0)
            complete_step(
                step,
                {
                    "uploaded": photo_uploaded,
                    "failed": photo_failed,
                    "optional": self.profile_photo_optional,
                },
            )
            persist_progress()
            if deduped_photo_updates:
                severity = "warn" if photo_failed > 0 else "info"
                log_event(
                    "step_completed",
                    severity,
                    f"[update_profile_photos] uploaded {photo_uploaded} profile photos",
                    photo_response,
                )

        checkpoint_data = checkpoint("finalize_update")
        if checkpoint_data:
            return checkpoint_data

        step = start_step("finalize_update")
        now_iso = self._iso_now()
        for inbox_id in all_target_ids:
            self.client.update_inbox(inbox_id, {"status": "active"})
        if mutation_request_id:
            for inbox_id, mutation_item in mutation_items_by_inbox_id.items():
                item_id = str(mutation_item.get("id") or "").strip()
                if not item_id:
                    continue
                matching_change = next(
                    (row for row in change_rows if str(row.get("inbox_id") or "").strip() == inbox_id),
                    None,
                )
                username_changed = bool(matching_change and matching_change.get("username_changed"))
                update_fields: Dict[str, Any] = {
                    "status": "completed",
                    "completed_at": now_iso,
                    "failed_at": None,
                    "last_error": None,
                    "alias_status": "active" if username_changed else "not_needed",
                }
                if inbox_id in onepassword_updated_ids:
                    update_fields["onepassword_updated_at"] = now_iso
                self.client.update_mutation_item(item_id, update_fields)
                if matching_change and username_changed:
                    self.client.upsert_inbox_email_alias(
                        inbox_id=inbox_id,
                        email=str(matching_change.get("old_email") or "").strip().lower(),
                        status="active",
                        source="mutation",
                    )
            self.client.update_mutation_request(
                mutation_request_id,
                {
                    "current_step": "finalize_update",
                    "last_error": None,
                },
            )
            if mutation_submission_id:
                self.client.refresh_mutation_submission(mutation_submission_id)
        result = {
            "updated": len(affected_ids),
            "total_targets": len(all_target_ids),
            "synced_users": synced_count,
            "username_updates": username_updates,
            "name_only_updates": name_only_updates,
            "onepassword_updated": onepassword_updated,
            "onepassword_missing": onepassword_missing,
            "profile_photos_uploaded": photo_uploaded,
            "profile_photos_failed": photo_failed,
        }
        complete_step(step, result)
        persist_progress()
        return result

    def _handle_profile_photos(
        self,
        action: Dict[str, Any],
        payload: Dict[str, Any],
        domain: Dict[str, Any],
        checkpoint,
        start_step,
        complete_step,
        fail_step,
        persist_progress,
        log_event,
    ) -> Dict[str, Any]:
        domain_id = str(domain.get("id"))
        domain_name = str(domain.get("domain") or "").strip().lower()
        raw_updates = payload.get("updates") or []
        if not isinstance(raw_updates, list) or not raw_updates:
            raise RuntimeError("google_update_profile_photos payload must include updates[]")

        resolved_checkpoint = checkpoint("apply_photo_urls")
        if resolved_checkpoint:
            affected_ids = [str(v) for v in (resolved_checkpoint.get("affected_ids") or [])]
        else:
            step = start_step("apply_photo_urls")
            all_inboxes = self.client.get_domain_inboxes_all(domain_id)
            live_rows = [row for row in all_inboxes if str(row.get("status") or "").lower() in LIVE_STATUSES]
            by_id = {str(row.get("id")): row for row in live_rows}
            by_username = {str(row.get("username") or "").strip().lower(): row for row in live_rows}

            affected_ids: List[str] = []
            for row in raw_updates:
                if not isinstance(row, dict):
                    continue
                target = None
                if row.get("inbox_id"):
                    target = by_id.get(str(row.get("inbox_id")))
                if target is None and row.get("username"):
                    target = by_username.get(str(row.get("username") or "").strip().lower())
                if target is None:
                    raise RuntimeError(f"Profile photo target not found: {row}")

                pic_url = str(row.get("profile_pic_url") or "").strip()
                if not pic_url:
                    raise RuntimeError(f"profile_pic_url is required for target: {row}")

                inbox_id = str(target.get("id"))
                self.client.update_inbox(inbox_id, {"profile_pic_url": pic_url, "status": "provisioning"})
                affected_ids.append(inbox_id)

            if not affected_ids:
                fail_step(step, "No valid profile photo updates were provided")
                persist_progress()
                raise RuntimeError("No valid profile photo updates were provided")

            complete_step(step, {"affected_ids": affected_ids})
            persist_progress()

        admin_checkpoint = checkpoint("resolve_admin_login")
        if admin_checkpoint:
            admin_inbox_id = str(admin_checkpoint.get("admin_inbox_id") or "").strip()
        else:
            step = start_step("resolve_admin_login")
            rows = self.client.get_domain_inboxes_all(domain_id)
            live_rows = [row for row in rows if str(row.get("status") or "").lower() in LIVE_STATUSES]
            admin_candidates = [
                row
                for row in live_rows
                if bool(row.get("is_admin"))
                and str(row.get("email") or "").strip()
                and str(row.get("password") or "").strip()
            ]
            if not admin_candidates:
                fallback_candidates = [
                    row
                    for row in live_rows
                    if str(row.get("email") or "").strip() and str(row.get("password") or "").strip()
                ]
                if fallback_candidates:
                    fallback_candidates = sorted(
                        fallback_candidates,
                        key=lambda r: (str(r.get("created_at") or ""), str(r.get("id") or "")),
                    )
                    admin_candidates = [fallback_candidates[0]]
                    self.client.update_inbox(str(fallback_candidates[0].get("id")), {"is_admin": True})
                    log_event(
                        "step_warning",
                        "warn",
                        f"[resolve_admin_login] Domain {domain_name} had no admin flagged; promoted fallback inbox.",
                        {
                            "domain": domain_name,
                            "fallback_admin_email": str(fallback_candidates[0].get("email") or "").strip().lower(),
                        },
                    )

            if not admin_candidates:
                fail_step(step, f"No admin-capable inbox found for {domain_name}")
                persist_progress()
                raise RuntimeError(
                    f"No admin-capable inbox found for {domain_name}. "
                    "Ensure at least one live inbox has is_admin=true, email, and password."
                )

            admin_row = sorted(
                admin_candidates,
                key=lambda r: (str(r.get("created_at") or ""), str(r.get("id") or "")),
            )[0]
            admin_inbox_id = str(admin_row.get("id") or "").strip()
            admin_email = str(admin_row.get("email") or "").strip().lower()
            complete_step(
                step,
                {
                    "admin_inbox_id": admin_inbox_id,
                    "admin_email": admin_email,
                },
            )
            persist_progress()

        admin_rows = self.client.get_inboxes_by_ids(domain_id, [admin_inbox_id]) if admin_inbox_id else []
        if not admin_rows:
            raise RuntimeError(f"Admin inbox {admin_inbox_id} not found for domain {domain_name}")
        admin_email = str(admin_rows[0].get("email") or "").strip().lower()
        admin_password = str(admin_rows[0].get("password") or "").strip()
        if not admin_email or not admin_password:
            raise RuntimeError(
                f"Admin inbox {admin_inbox_id} for {domain_name} is missing email/password in Supabase"
            )

        sync_checkpoint = checkpoint("upload_profile_photos_google")
        if sync_checkpoint:
            synced_count = int(sync_checkpoint.get("uploaded") or 0)
        else:
            step = start_step("upload_profile_photos_google")
            rows = self.client.get_inboxes_by_ids(domain_id, affected_ids)
            upload_pairs = []
            for row in rows:
                username = str(row.get("username") or "").strip().lower()
                email = str(row.get("email") or f"{username}@{domain_name}").strip().lower()
                pic = str(row.get("profile_pic_url") or "").strip()
                if email and pic:
                    upload_pairs.append((email, pic))

            if upload_pairs:
                op_client = OnePasswordCliClient.from_env()
                with GoogleAdminPlaywrightClient(headless=self.playwright_headless) as browser:
                    browser._login_google_account(admin_email, admin_password, onepassword=op_client)
                    upload_response = browser.upload_profile_photos(upload_pairs)
            else:
                upload_response = {"uploaded": 0, "failed": 0}

            if int(upload_response.get("failed") or 0) > 0 and not self.profile_photo_optional:
                fail_step(step, f"Playwright profile upload failed for {upload_response.get('failed')} user(s)")
                persist_progress()
                raise RuntimeError(f"Playwright profile upload failed: {upload_response}")

            synced_count = int(upload_response.get("uploaded") or 0)
            complete_step(step, {"uploaded": synced_count, "failed": int(upload_response.get("failed") or 0)})
            persist_progress()
            log_event(
                "step_completed",
                "info",
                f"[upload_profile_photos_google] uploaded {synced_count} profile photos",
                upload_response,
            )

        checkpoint_data = checkpoint("finalize_profile_photos")
        if checkpoint_data:
            return checkpoint_data

        step = start_step("finalize_profile_photos")
        for inbox_id in affected_ids:
            self.client.update_inbox(inbox_id, {"status": "active"})
        result = {"updated": len(affected_ids), "synced_users": synced_count}
        complete_step(step, result)
        persist_progress()
        return result

    def _build_partnerhub_users(
        self,
        domain_name: str,
        inboxes: List[Dict[str, Any]],
        default_user_type: str = "admin",
    ) -> List[Dict[str, Any]]:
        users: List[Dict[str, Any]] = []
        sorted_rows = sorted(inboxes, key=lambda row: str(row.get("created_at") or ""))
        for idx, row in enumerate(sorted_rows):
            username = str(row.get("username") or "").strip().lower()
            if not username:
                continue
            email = str(row.get("email") or f"{username}@{domain_name}").strip().lower()
            first_name = str(row.get("first_name") or username).strip() or username
            last_name = str(row.get("last_name") or "").strip()
            password = str(row.get("password") or "").strip() or self._generate_password()

            payload: Dict[str, Any] = {
                "email": email,
                "password": password,
                "firstName": first_name,
                "lastName": last_name,
                "userType": default_user_type if default_user_type != "admin" else ("admin" if idx == 0 else "normal"),
            }
            profile_pic_url = str(row.get("profile_pic_url") or "").strip()
            if profile_pic_url:
                payload["profilePicUrl"] = profile_pic_url
            users.append(payload)
        return users

    def _resolve_domain_shared_password(
        self,
        all_inboxes: List[Dict[str, Any]],
        target_rows: List[Dict[str, Any]],
    ) -> str:
        live_statuses = {str(v).lower() for v in LIVE_STATUSES}
        candidates: List[str] = []

        for row in all_inboxes:
            password = str(row.get("password") or "").strip()
            if not password:
                continue
            if str(row.get("status") or "").strip().lower() in live_statuses:
                candidates.append(password)

        for row in all_inboxes:
            password = str(row.get("password") or "").strip()
            if password:
                candidates.append(password)

        for row in target_rows:
            password = str(row.get("password") or "").strip()
            if password:
                candidates.append(password)

        return candidates[0] if candidates else self._generate_password()

    def _build_profile_photo_updates(self, domain_name: str, inboxes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        updates: List[Dict[str, Any]] = []
        for row in inboxes:
            pic_url = str(row.get("profile_pic_url") or "").strip()
            if not pic_url:
                continue
            username = str(row.get("username") or "").strip().lower()
            email = str(row.get("email") or f"{username}@{domain_name}").strip().lower()
            updates.append({"email": email, "profilePicUrl": pic_url})
        return updates

    def _build_mfa_users(self, domain_name: str, rows: List[Dict[str, Any]]) -> List[GoogleMfaUser]:
        users: List[GoogleMfaUser] = []
        for row in rows:
            username = str(row.get("username") or "").strip().lower()
            if not username:
                continue
            email = str(row.get("email") or f"{username}@{domain_name}").strip().lower()
            password = str(row.get("password") or "").strip()
            if not email or not password:
                continue
            users.append(
                GoogleMfaUser(
                    email=email,
                    password=password,
                    username=username,
                )
            )
        return users

    @staticmethod
    def _mfa_step_name(email: str) -> str:
        clean = str(email or "").strip().lower()
        safe = []
        for ch in clean:
            if ch.isalnum():
                safe.append(ch)
            else:
                safe.append("_")
        return f"mfa_user_{''.join(safe)}"

    def _get_partnerhub(self) -> PartnerHubClient:
        if self._partnerhub is None:
            self._partnerhub = PartnerHubClient(
                api_key=self.partnerhub_api_key,
                base_url=self.partnerhub_base_url,
                default_plan_id=self.partnerhub_plan_id,
            )
        return self._partnerhub

    @staticmethod
    def _generate_password(length: int = 14) -> str:
        chars = string.ascii_letters + string.digits + "!@#$%^&*"
        password = "".join(random.choice(chars) for _ in range(length))
        if not any(c.isupper() for c in password):
            password = "A" + password[1:]
        if not any(c.isdigit() for c in password):
            password = password[:-1] + "1"
        return password

    @staticmethod
    def _iso_now() -> str:
        return datetime.utcnow().isoformat() + "Z"

    @staticmethod
    def _as_bool(value: Optional[str], default: bool = False) -> bool:
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}
