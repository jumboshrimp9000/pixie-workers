import logging
import os
import random
import string
import time
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from app import get_order_logger
from app.workers.google_fulfillment_clients import (
    CloudflareClient,
    DynadotClient,
    PartnerHubClient,
)
from app.workers.google_admin_playwright import GoogleAdminPlaywrightClient, GoogleMfaUser
from app.workers.onepassword_client import OnePasswordCliClient
from app.workers.sending_tool_uploader import SendingToolUploader
from app.workers.supabase_client import SupabaseRestClient


logger = logging.getLogger(__name__)


INTERIM_STATUSES = {
    "DOMAIN_PURCHASED": "Both - Domain Purchased",
    "CF_ZONE": "Both - DNS Zone Created",
    "NS_MIGRATION": "Both - NS Migrated",
    "NS_PENDING": "Both - NS Propagating",
    "CF_ACTIVE": "Both - CF Zone Active",
    "GOOGLE_ORDER_CREATED": "Google - Order Created",
    "DNS_RECORDS": "Both - DNS Records Added",
    "USERS_CREATED": "Google - Users Created",
    "DOMAIN_VERIFIED": "Google - Domain Verified",
    "ADMIN_APPS": "Google - Admin Apps Configured",
    "DKIM_ENABLED": "Google - DKIM Enabled",
    "PROFILE_PHOTOS": "Google - Profile Photos Uploaded",
    "MFA_ENROLLMENT": "Google - 2FA Enrolling",
    "SENDING_TOOL_UPLOAD": "Both - Sending Tool Upload",
    "COMPLETE": "Both - Provisioning Complete",
    "FAILED": "Both - Failed",
}

DEFAULT_SMARTLEAD_APP_ID = "1021517043376-ipe8289dof3t2v9apjpae8hs2q9abetp.apps.googleusercontent.com"
DEFAULT_INSTANTLY_APP_ID = "536726988839-pt93oro4685dtb1emb0pp2vjgjol5mls.apps.googleusercontent.com"
OPTIONAL_GOOGLE_APP_IDS = {
    "master_inbox": "563322621692-2vfek77q0f6trjlt3afr7ag6cf0pvfeh.apps.googleusercontent.com",
    "warmy": "964878161904-5uqi9bsrj16frjku01ep27qs0504ujjr.apps.googleusercontent.com",
    "plusvibe": "915060167262-mt46cccq569tgg2rb5qk375pf95obh6e.apps.googleusercontent.com",
}
LIVE_STATUSES = {"pending", "provisioning", "active"}


class GoogleSupabaseWorker:
    def __init__(self):
        self.client = SupabaseRestClient.from_env()
        self.poll_interval_seconds = max(1.0, float(os.getenv("GOOGLE_WORKER_POLL_SECONDS", "10")))
        self.batch_size = max(1, int(os.getenv("GOOGLE_WORKER_BATCH_SIZE", "3")))
        self.max_retries = max(1, int(os.getenv("GOOGLE_WORKER_MAX_RETRIES", "5")))
        action_types = os.getenv("GOOGLE_WORKER_ACTION_TYPES", "google_provision")
        self.action_types = [t.strip() for t in action_types.split(",") if t.strip()]

        self.dry_run = self._as_bool(os.getenv("GOOGLE_WORKER_DRY_RUN"), default=False)
        self.require_cf_active = self._as_bool(os.getenv("GOOGLE_WORKER_REQUIRE_CF_ACTIVE"), default=True)

        self.partnerhub_base_url = os.getenv("PARTNERHUB_API_BASE_URL", "https://partnerhubapi.netstager.com/api")
        self.partnerhub_api_key = os.getenv("PARTNERHUB_API_KEY") or os.getenv("GOOGLE_NETSTAGER_API_KEY") or ""
        self.partnerhub_plan_id = os.getenv("PARTNERHUB_PLAN_ID", "94c835bb-a675-4249-8fba-e95cdb2ca4ed")
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
        default_required_ids = self._parse_google_app_ids(os.getenv("GOOGLE_REQUIRED_ADMIN_APP_IDS", ""))
        self.required_google_app_ids = default_required_ids or [DEFAULT_SMARTLEAD_APP_ID, DEFAULT_INSTANTLY_APP_ID]
        self.require_admin_apps = self._as_bool(os.getenv("GOOGLE_REQUIRE_ADMIN_APPS"), default=True)
        self.admin_apps_attempts = max(1, int(os.getenv("GOOGLE_ADMIN_APPS_ATTEMPTS", "3")))
        self.admin_apps_retry_delay_seconds = max(
            5.0,
            float(os.getenv("GOOGLE_ADMIN_APPS_RETRY_DELAY_SECONDS", "20")),
        )
        self.require_dkim_enabled = self._as_bool(os.getenv("GOOGLE_REQUIRE_DKIM_ENABLED"), default=True)
        self.profile_photo_optional = self._as_bool(os.getenv("GOOGLE_PROFILE_PHOTO_OPTIONAL"), default=False)
        self.sending_tool_playwright_oauth = self._as_bool(
            os.getenv("GOOGLE_SENDING_TOOL_USE_PLAYWRIGHT_OAUTH"),
            default=True,
        )
        self.sending_tool_require_1password = self._as_bool(
            os.getenv("GOOGLE_SENDING_TOOL_OAUTH_REQUIRE_1PASSWORD"),
            default=True,
        )
        self.domain_verify_attempts = max(1, int(os.getenv("GOOGLE_DOMAIN_VERIFY_ATTEMPTS", "12")))
        self.domain_verify_interval_seconds = max(
            5.0,
            float(os.getenv("GOOGLE_DOMAIN_VERIFY_INTERVAL_SECONDS", "15")),
        )
        self.domain_verify_dns_wait_seconds = max(
            0.0,
            float(os.getenv("GOOGLE_DOMAIN_VERIFY_DNS_WAIT_SECONDS", "10")),
        )
        self.dkim_auth_attempts = max(1, int(os.getenv("GOOGLE_DKIM_AUTH_ATTEMPTS", "5")))
        self.dkim_auth_interval_seconds = max(5.0, float(os.getenv("GOOGLE_DKIM_AUTH_INTERVAL_SECONDS", "45")))
        self.dkim_dns_wait_seconds = max(0.0, float(os.getenv("GOOGLE_DKIM_DNS_WAIT_SECONDS", "10")))
        self.admin_console_defer_seconds = max(
            30.0,
            float(os.getenv("GOOGLE_ADMIN_CONSOLE_DEFER_SECONDS", "90")),
        )

        self._dynadot: Optional[DynadotClient] = None
        self._cloudflare: Optional[CloudflareClient] = None
        self._partnerhub: Optional[PartnerHubClient] = None
        self._sending_tool_uploader = SendingToolUploader()

    def run_forever(self) -> None:
        logger.info(
            "Google Supabase worker started (types=%s, poll=%.1fs, dry_run=%s)",
            self.action_types,
            self.poll_interval_seconds,
            self.dry_run,
        )
        while True:
            try:
                processed = self._poll_once()
                if processed == 0:
                    time.sleep(self.poll_interval_seconds)
            except Exception:
                logger.exception("Worker poll failed")
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
        action_id = str(action.get("id"))
        action_logger = get_order_logger(action_id)
        payload = action.get("payload") or {}
        prior_result = action.get("result") or {}
        prior_steps_raw = prior_result.get("steps") if isinstance(prior_result, dict) else []
        if not isinstance(prior_steps_raw, list):
            prior_steps_raw = []

        domain_id: Optional[str] = action.get("domain_id")
        domain: Optional[Dict[str, Any]] = None
        inboxes: List[Dict[str, Any]] = []

        steps: List[Dict[str, Any]] = [s for s in prior_steps_raw if isinstance(s, dict)]
        last_step_status: Dict[str, str] = {}
        last_step_details: Dict[str, Dict[str, Any]] = {}
        for item in steps:
            name = str(item.get("step") or "").strip()
            if not name:
                continue
            last_step_status[name] = str(item.get("status") or "").strip().lower()
            details = item.get("details")
            if isinstance(details, dict):
                last_step_details[name] = details

        def start_step(step_name: str) -> Dict[str, Any]:
            step = {
                "step": step_name,
                "status": "in_progress",
                "startedAt": self._iso_now(),
            }
            steps.append(step)
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
            steps.append(
                {
                    "step": step_name,
                    "status": "skipped",
                    "details": {"reason": reason},
                }
            )

        def persist_progress(interim_status: Optional[str] = None, domain_status: Optional[str] = None) -> None:
            try:
                self.client.update_action(
                    action_id,
                    {
                        "result": {
                            "steps": steps,
                            "lastUpdated": self._iso_now(),
                        }
                    },
                )
                if domain_id and (interim_status or domain_status):
                    fields: Dict[str, Any] = {}
                    if interim_status:
                        fields["interim_status"] = interim_status
                    if domain_status:
                        fields["status"] = domain_status
                    # Conditional update when changing status: never overwrite a
                    # domain that has been moved into a cancellation/terminal
                    # state. Protects the cancel/provision race window.
                    if domain_status:
                        self.client.update_domain_if_active(domain_id, fields)
                    else:
                        self.client.update_domain(domain_id, fields)
            except Exception:
                # Progress is best-effort; fulfillment should continue.
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
                # Action log writes are best-effort.
                pass

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

        def domain_verification_confirmed(step_details: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
            if self.dry_run:
                return True, "dry_run"
            if not isinstance(step_details, dict) or not step_details:
                return False, "verification details missing"

            confirmation = step_details.get("confirmation")
            if isinstance(confirmation, dict) and bool(confirmation.get("verified")):
                return True, "confirmation.verified=true"
            if bool(step_details.get("already_verified")):
                return True, "already_verified=true"
            if bool(step_details.get("verified")):
                return True, "verified=true"
            if isinstance(confirmation, dict):
                return False, f"confirmation.verified={confirmation.get('verified')!r}"
            return False, "confirmation details missing"

        try:
            log_event("action_started", "info", f"Processing action {action.get('type')}")

            if not domain_id:
                raise ValueError("Action missing domain_id")

            domain = self.client.get_domain(domain_id)
            if not domain:
                raise ValueError(f"Domain {domain_id} not found")

            provider = str(domain.get("provider") or "").lower()
            if provider != "google":
                result = {"skipped": True, "reason": "Domain provider is not google"}
                self.client.complete_action(action_id, result)
                self.client.insert_action_log(action, "action_completed", "info", "Skipped non-google domain", result)
                return

            # Race guard: if a cancellation has already landed, do not start
            # provisioning. Free the cancel worker to handle teardown.
            entry_status = str(domain.get("status") or "").strip().lower()
            if entry_status in ("queued_for_cancellation", "cancelled", "suspended"):
                result = {"skipped": True, "reason": f"Domain is {entry_status}"}
                self.client.complete_action(action_id, result)
                self.client.insert_action_log(
                    action,
                    "action_skipped",
                    "warn",
                    f"Domain {domain_id} entry status is {entry_status}; aborting provisioning",
                    result,
                )
                return

            inboxes = self.client.get_domain_inboxes(domain_id)
            if not inboxes:
                result = {"skipped": True, "reason": "No inboxes found"}
                self.client.complete_action(action_id, result)
                self.client.insert_action_log(action, "action_completed", "info", "No inboxes to process", result)
                return

            # Domain enters in-progress while fulfillment runs.
            # Conditional update: only set in_progress if status is still in an
            # active/preparing state. This prevents a race where a cancellation
            # arrives between get_domain above and this update.
            self.client.update_domain_if_active(domain_id, {"status": "in_progress"})

            for inbox in inboxes:
                if inbox.get("status") in ("pending", "provisioning"):
                    self.client.update_inbox(inbox["id"], {"status": "provisioning"})

            domain_name = str(domain.get("domain") or "").strip().lower()
            if not domain_name:
                raise RuntimeError("Domain record missing domain value")

            source = str(domain.get("source") or "own").strip().lower()
            cloudflare_zone_id = domain.get("cloudflare_zone_id")
            nameservers_moved = bool(domain.get("nameservers_moved"))
            log_event(
                "action_context",
                "info",
                f"Google fulfillment started for {domain_name}",
                {
                    "domain": domain_name,
                    "source": source,
                    "existing_zone": bool(cloudflare_zone_id),
                    "nameservers_moved": nameservers_moved,
                    "inbox_count": len(inboxes),
                },
            )

            # ─────────────────────────────────────────────────────────────
            # Shared domain prep (idempotent)
            # ─────────────────────────────────────────────────────────────
            if source == "buy" and not domain.get("domain_expiry_date"):
                step = start_step("purchase_domain")
                if self.dry_run:
                    expiry_date = (datetime.utcnow().date() + timedelta(days=365)).isoformat()
                    complete_step(step, {"dry_run": True, "expiry_date": expiry_date})
                else:
                    register = self._get_dynadot().register_domain(domain_name)
                    if not register.get("success"):
                        message = str(register.get("error") or "Dynadot registration failed")
                        fail_step(step, message)
                        persist_progress(INTERIM_STATUSES["FAILED"])
                        raise RuntimeError(message)

                    expiry_date = (datetime.utcnow().date() + timedelta(days=365)).isoformat()
                    self.client.update_domain(domain_id, {"domain_expiry_date": expiry_date})
                    complete_step(
                        step,
                        {
                            "already_registered": bool(register.get("already_registered")),
                            "expiry_date": expiry_date,
                        },
                    )
                    persist_progress(INTERIM_STATUSES["DOMAIN_PURCHASED"], "in_progress")
            else:
                skip_step(
                    "purchase_domain",
                    "BYOD domain" if source != "buy" else "Domain already purchased (expiry date present)",
                )

            if not cloudflare_zone_id:
                step = start_step("cf_zone")
                if self.dry_run:
                    cloudflare_zone_id = f"dry-{domain_name.replace('.', '-')[:24]}"
                    complete_step(step, {"dry_run": True, "zone_id": cloudflare_zone_id})
                else:
                    cloudflare_zone_id, created = self._get_cloudflare().get_or_create_zone(domain_name)
                    complete_step(step, {"zone_id": cloudflare_zone_id, "created": created})
                self.client.update_domain(domain_id, {"cloudflare_zone_id": cloudflare_zone_id})
                persist_progress(INTERIM_STATUSES["CF_ZONE"], "in_progress")
            else:
                skip_step("cf_zone", "Zone already exists")

            step = start_step("get_cf_nameservers")
            if self.dry_run:
                ns1, ns2 = "abby.ns.cloudflare.com", "noel.ns.cloudflare.com"
            else:
                ns1, ns2 = self._get_cloudflare().get_zone_nameservers(cloudflare_zone_id)
            complete_step(step, {"ns1": ns1, "ns2": ns2})

            if source == "buy" and not nameservers_moved:
                step = start_step("move_ns_to_cloudflare")
                if self.dry_run:
                    complete_step(step, {"dry_run": True, "ns1": ns1, "ns2": ns2})
                else:
                    ns_resp = self._get_dynadot().set_nameservers(domain_name, ns1, ns2)
                    if not ns_resp.get("success"):
                        message = str(ns_resp.get("error") or "Dynadot nameserver migration failed")
                        fail_step(step, message)
                        persist_progress(INTERIM_STATUSES["FAILED"])
                        raise RuntimeError(message)
                    complete_step(step, {"ns1": ns1, "ns2": ns2})

                nameservers_moved = True
                self.client.update_domain(domain_id, {"nameservers_moved": True, "status": "ns_pending"})
                persist_progress(INTERIM_STATUSES["NS_MIGRATION"], "ns_pending")
            else:
                reason = "Nameservers already moved" if source == "buy" else "BYOD domain - registrar managed by customer"
                skip_step("move_ns_to_cloudflare", reason)

            step = start_step("wait_cf_zone_active")
            if self.dry_run:
                zone_active = True
            else:
                zone_active = self._get_cloudflare().is_zone_active(cloudflare_zone_id)

            if not zone_active and self.require_cf_active:
                fail_step(step, "Cloudflare zone is not active yet")
                persist_progress(INTERIM_STATUSES["NS_PENDING"], "ns_pending")
                raise RuntimeError(
                    f"Cloudflare zone for {domain_name} is not active yet. Waiting for nameserver propagation."
                )

            if zone_active and source == "own" and not nameservers_moved:
                nameservers_moved = True
                self.client.update_domain(domain_id, {"nameservers_moved": True})

            complete_step(step, {"active": zone_active})
            persist_progress(INTERIM_STATUSES["CF_ACTIVE"], "in_progress")

            # ─────────────────────────────────────────────────────────────
            # Google-specific provisioning
            # ─────────────────────────────────────────────────────────────
            credential_checkpoint = checkpoint("persist_inbox_credentials")
            if credential_checkpoint:
                inboxes = self.client.get_domain_inboxes(domain_id)
            else:
                step = start_step("persist_inbox_credentials")
                inboxes, credential_summary = self._persist_inbox_credentials(domain_name, inboxes, payload)
                complete_step(step, credential_summary)
                persist_progress(INTERIM_STATUSES["CF_ACTIVE"], "in_progress")
                log_event(
                    "step_completed",
                    "info",
                    f"[persist_inbox_credentials] Prepared {len(inboxes)} inbox credentials for {domain_name}",
                    step,
                )

            partnerhub_users = self._build_partnerhub_users(domain_name, inboxes, payload)
            organization_name = (
                str(payload.get("organization_name") or "").strip()
                or str(payload.get("client_name") or "").strip()
                or domain_name
            )
            preflight_partnerhub_order_id = str(
                payload.get("partnerhub_order_id") or domain.get("partnerhub_order_id") or ""
            ).strip()

            def persist_partnerhub_order_id(value: Any) -> None:
                order_id_value = str(value or "").strip()
                if not order_id_value:
                    return
                try:
                    self.client.update_domain(domain_id, {"partnerhub_order_id": order_id_value})
                except Exception as persist_exc:
                    log_event(
                        "step_warning",
                        "warn",
                        f"[create_google_order] Failed to persist partnerhub_order_id for {domain_name}",
                        {"order_id": order_id_value, "error": str(persist_exc)},
                    )

            order_checkpoint = checkpoint("create_google_order")
            if order_checkpoint:
                order_id = str(order_checkpoint.get("order_id") or "").strip()
                if not order_id and not self.dry_run:
                    order_payload = self._get_partnerhub().get_order_details(domain_name)
                    order_id = str(self._get_partnerhub().extract_order_id(order_payload) or "").strip()
                    if not order_id:
                        raise RuntimeError(
                            f"[create_google_order] Checkpoint found but order_id missing for {domain_name}"
                        )
                elif not order_id and self.dry_run:
                    order_id = f"dry-{action_id[:8]}"
                persist_partnerhub_order_id(order_id)
                persist_progress(INTERIM_STATUSES["GOOGLE_ORDER_CREATED"], "in_progress")
                log_event(
                    "step_resumed",
                    "info",
                    f"[create_google_order] Reusing checkpointed PartnerHub order for {domain_name}",
                    {"order_id": order_id},
                )
            else:
                step = start_step("create_google_order")
                log_event(
                    "step_started",
                    "info",
                    f"[create_google_order] Creating PartnerHub order for {domain_name}",
                    {"organization_name": organization_name, "user_count": len(partnerhub_users)},
                )
                if self.dry_run:
                    order_payload = {"success": True, "data": {"order": {"id": f"dry-{action_id[:8]}"}}}
                    order_id = f"dry-{action_id[:8]}"
                    complete_step(step, {"dry_run": True, "order_id": order_id, "users": len(partnerhub_users)})
                else:
                    existing_order = False
                    if preflight_partnerhub_order_id:
                        existing_order = True
                        order_id = preflight_partnerhub_order_id
                        log_event(
                            "step_info",
                            "info",
                            f"[create_google_order] Reusing preflight PartnerHub order for {domain_name}.",
                            {"order_id": order_id},
                        )
                        try:
                            order_payload = self._get_partnerhub().get_order_by_id(order_id)
                        except Exception:
                            order_payload = self._get_partnerhub().get_order_details(domain_name)
                    else:
                        try:
                            order_payload = self._get_partnerhub().create_order(
                                domain=domain_name,
                                organization_name=organization_name,
                                users=partnerhub_users,
                                plan_id=str(payload.get("plan_id") or self.partnerhub_plan_id),
                            )
                            order_id = self._get_partnerhub().extract_order_id(order_payload)
                        except Exception as create_exc:
                            if "domain already exists" in str(create_exc).lower():
                                existing_order = True
                                log_event(
                                    "step_info",
                                    "warn",
                                    f"[create_google_order] Domain already exists in PartnerHub for {domain_name}; reusing existing order.",
                                )
                                order_payload = self._get_partnerhub().get_order_details(domain_name)
                                order_id = self._get_partnerhub().extract_order_id(order_payload)
                                if not order_id:
                                    raise RuntimeError(
                                        f"PartnerHub returned 'domain already exists' but no order id was found for {domain_name}"
                                    )
                            else:
                                raise
                    complete_step(
                        step,
                        {
                            "order_id": order_id,
                            "users": len(partnerhub_users),
                            "existing_order": existing_order,
                            "preflight_order_id_used": bool(preflight_partnerhub_order_id),
                        },
                    )
                persist_partnerhub_order_id(order_id)
                persist_progress(INTERIM_STATUSES["GOOGLE_ORDER_CREATED"], "in_progress")
                log_event("step_completed", "info", f"[create_google_order] PartnerHub order ready for {domain_name}", step)

            dns_fetch_checkpoint = checkpoint("fetch_google_dns_records")
            if dns_fetch_checkpoint and self.dry_run:
                dns_records = self._default_google_dns_records(domain_name)
            else:
                if not dns_fetch_checkpoint:
                    step = start_step("fetch_google_dns_records")
                if self.dry_run:
                    dns_records = self._default_google_dns_records(domain_name)
                    if not dns_fetch_checkpoint:
                        complete_step(step, {"dry_run": True, "records": len(dns_records), "fallback": True})
                else:
                    order_details = self._get_partnerhub().get_order_details(domain_name)
                    dns_records = self._get_partnerhub().extract_dns_records(order_details, domain_name)
                    used_fallback = False
                    if not dns_records:
                        dns_records = self._default_google_dns_records(domain_name)
                        used_fallback = True

                    custom_dns = self._extract_custom_dns(payload)
                    if custom_dns:
                        dns_records.extend(custom_dns)

                    if not dns_fetch_checkpoint:
                        complete_step(
                            step,
                            {
                                "records": len(dns_records),
                                "fallback": used_fallback,
                                "custom_records": len(custom_dns),
                            },
                        )

            dns_write_checkpoint = checkpoint("add_dns_records")
            if dns_write_checkpoint:
                dns_summary = dns_write_checkpoint
                persist_progress(INTERIM_STATUSES["DNS_RECORDS"], "in_progress")
            else:
                step = start_step("add_dns_records")
                log_event("step_started", "info", f"[add_dns_records] Writing {len(dns_records)} DNS records to Cloudflare")
                if self.dry_run:
                    dns_summary = {"created": len(dns_records), "skipped": 0, "failed": 0, "dry_run": True}
                else:
                    dns_summary = self._get_cloudflare().upsert_dns_records(cloudflare_zone_id, dns_records)
                    if dns_summary.get("failed", 0) > 0 and dns_summary.get("created", 0) == 0:
                        dns_errors = dns_summary.get("errors") or []
                        auth_related = any(
                            "authentication" in str(err.get("message", "")).lower()
                            for err in dns_errors
                            if isinstance(err, dict)
                        )
                        if auth_related:
                            guidance = (
                                "Cloudflare DNS authentication failed. Fix CLOUDFLARE_API_TOKEN permissions "
                                "or configure CLOUDFLARE_GLOBAL_KEY and CLOUDFLARE_EMAIL."
                            )
                        else:
                            guidance = "All DNS record writes failed in Cloudflare."
                        fail_step(step, f"{guidance} summary={dns_summary}")
                        persist_progress(INTERIM_STATUSES["FAILED"])
                        log_event("step_failed", "error", f"[add_dns_records] {guidance}", {"dns_summary": dns_summary})
                        raise RuntimeError(f"{guidance} Domain={domain_name}. Summary={dns_summary}")

                complete_step(step, dns_summary)
                persist_progress(INTERIM_STATUSES["DNS_RECORDS"], "in_progress")
                log_event("step_completed", "info", f"[add_dns_records] DNS write completed for {domain_name}", step)

            create_users_checkpoint = checkpoint("create_users")
            user_updates = self._build_inbox_updates(domain_name, inboxes, partnerhub_users, payload)
            if create_users_checkpoint:
                persist_progress(INTERIM_STATUSES["USERS_CREATED"], "in_progress")
            else:
                step = start_step("create_users")
                updated = 0
                for inbox in inboxes:
                    update = user_updates.get(inbox["id"])
                    if not update:
                        continue
                    self.client.update_inbox(
                        inbox["id"],
                        {
                            "email": update["email"],
                            "password": update["password"],
                            "is_admin": bool(update.get("is_admin")),
                            "status": "active",
                        },
                    )
                    updated += 1
                complete_step(step, {"updated": updated, "total": len(inboxes)})
                persist_progress(INTERIM_STATUSES["USERS_CREATED"], "in_progress")

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
                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
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
                persist_progress(INTERIM_STATUSES["USERS_CREATED"], "in_progress")

            admin_rows = self.client.get_inboxes_by_ids(domain_id, [admin_inbox_id]) if admin_inbox_id else []
            if not admin_rows:
                raise RuntimeError(f"Admin inbox {admin_inbox_id} not found for domain {domain_name}")
            admin_email = str(admin_rows[0].get("email") or "").strip().lower()
            admin_password = str(admin_rows[0].get("password") or "").strip()
            if not admin_email or not admin_password:
                raise RuntimeError(
                    f"Admin inbox {admin_inbox_id} for {domain_name} is missing email/password in Supabase"
                )

            verify_domain_checkpoint = checkpoint("verify_domain")
            domain_verification_details: Dict[str, Any] = (
                dict(verify_domain_checkpoint)
                if isinstance(verify_domain_checkpoint, dict)
                else {}
            )
            app_config_checkpoint = checkpoint("configure_admin_apps")
            dkim_checkpoint = checkpoint("enable_dkim")
            profile_photo_checkpoint = checkpoint("upload_profile_photos")
            app_plan = self._resolve_google_admin_app_ids(payload)
            need_admin_browser = (
                (not verify_domain_checkpoint)
                or (not app_config_checkpoint)
                or (not dkim_checkpoint)
                or (not profile_photo_checkpoint)
            )

            if need_admin_browser:
                op_client: Optional[OnePasswordCliClient] = None
                try:
                    op_client = OnePasswordCliClient.from_env()
                except Exception as op_exc:
                    log_event(
                        "step_warning",
                        "warn",
                        "[google_admin_login] 1Password not configured; admin login retries may fail when 2FA is required.",
                        {"error": str(op_exc)},
                    )

                with GoogleAdminPlaywrightClient(headless=self.playwright_headless) as browser:
                    step = start_step("login_admin_console")
                    try:
                        browser.login(admin_email, admin_password, onepassword=op_client)
                    except Exception as exc:
                        fail_step(step, str(exc))
                        persist_progress(INTERIM_STATUSES["USERS_CREATED"], "in_progress")
                        raise
                    complete_step(step, {"admin_email": admin_email})
                    persist_progress(INTERIM_STATUSES["USERS_CREATED"], "in_progress")

                    if not verify_domain_checkpoint:
                        step = start_step("verify_domain")
                        step_details: Dict[str, Any] = {"domain": domain_name}
                        if self.dry_run:
                            step_details["dry_run"] = True
                            complete_step(step, step_details)
                            persist_progress(INTERIM_STATUSES["DOMAIN_VERIFIED"], "in_progress")
                        else:
                            log_event(
                                "step_started",
                                "info",
                                f"[verify_domain] Validating {domain_name} in Google Admin",
                                {"domain": domain_name},
                            )
                            verification = browser.fetch_domain_verification_txt_record(domain_name)
                            step_details.update(verification)

                            if not bool(verification.get("already_verified")):
                                verification_value = str(verification.get("verification_value") or "").strip()
                                if not verification_value:
                                    fail_step(step, f"Google Admin did not return a verification TXT value for {domain_name}")
                                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                                    raise RuntimeError(f"Missing Google domain verification TXT value for {domain_name}")

                                verification_dns_summary = self._get_cloudflare().upsert_dns_records(
                                    cloudflare_zone_id,
                                    [
                                        {
                                            "type": "TXT",
                                            "name": "@",
                                            "content": verification_value,
                                            "ttl": 3600,
                                        }
                                    ],
                                )
                                step_details["dns_write"] = {
                                    "name": "@",
                                    "summary": verification_dns_summary,
                                }
                                if (
                                    int(verification_dns_summary.get("failed") or 0) > 0
                                    and int(verification_dns_summary.get("created") or 0) == 0
                                    and int(verification_dns_summary.get("skipped") or 0) == 0
                                ):
                                    fail_message = f"Cloudflare domain verification TXT write failed: {verification_dns_summary}"
                                    fail_step(step, fail_message)
                                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                                    raise RuntimeError(fail_message)

                                if self.domain_verify_dns_wait_seconds > 0:
                                    time.sleep(self.domain_verify_dns_wait_seconds)

                                confirmation = browser.confirm_domain_verification(
                                    domain_name,
                                    attempts=self.domain_verify_attempts,
                                    sleep_seconds=self.domain_verify_interval_seconds,
                                )
                                step_details["confirmation"] = confirmation
                                if not bool(confirmation.get("verified")):
                                    fail_message = (
                                        f"Google Admin domain verification did not complete for {domain_name}. "
                                        f"details={confirmation}"
                                    )
                                    fail_step(step, fail_message)
                                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                                    raise RuntimeError(fail_message)
                            else:
                                step_details["confirmation"] = {
                                    "verified": True,
                                    "already_verified": True,
                                }

                            domain_verification_details = dict(step_details)
                            complete_step(step, step_details)
                            persist_progress(INTERIM_STATUSES["DOMAIN_VERIFIED"], "in_progress")

                    if not app_config_checkpoint:
                        step = start_step("configure_admin_apps")
                        step_details: Dict[str, Any] = {
                            "required_app_ids": app_plan["required"],
                            "optional_app_ids": app_plan["optional"],
                            "invalid_app_ids": app_plan["invalid"],
                            "max_attempts": self.admin_apps_attempts,
                        }
                        if self.dry_run:
                            step_details["dry_run"] = True
                            step_details["requested"] = app_plan["all"]
                            complete_step(step, step_details)
                            persist_progress(INTERIM_STATUSES["ADMIN_APPS"], "in_progress")
                        elif not app_plan["all"]:
                            step_details["requested"] = []
                            step_details["skipped"] = "No Google app client IDs requested"
                            complete_step(step, step_details)
                            persist_progress(INTERIM_STATUSES["ADMIN_APPS"], "in_progress")
                        else:
                            log_event(
                                "step_started",
                                "info",
                                f"[configure_admin_apps] Configuring {len(app_plan['all'])} Google app IDs",
                                {"requested": app_plan["all"]},
                            )
                            required_unresolved = list(app_plan["required"])
                            attempt_records: List[Dict[str, Any]] = []
                            added_ids: set = set()
                            already_configured_ids: set = set()
                            invalid_ids: set = set(app_plan["invalid"])
                            failed_by_id: Dict[str, Dict[str, str]] = {}

                            for attempt in range(1, self.admin_apps_attempts + 1):
                                request_ids = app_plan["all"] if attempt == 1 else required_unresolved
                                if not request_ids:
                                    break

                                apps_result = browser.add_trusted_apps(request_ids)
                                attempt_records.append(
                                    {
                                        "attempt": attempt,
                                        "requested": request_ids,
                                        "result": apps_result,
                                    }
                                )

                                for client_id in apps_result.get("added") or []:
                                    clean = str(client_id or "").strip()
                                    if clean:
                                        added_ids.add(clean)
                                        failed_by_id.pop(clean, None)
                                for client_id in apps_result.get("already_configured") or []:
                                    clean = str(client_id or "").strip()
                                    if clean:
                                        already_configured_ids.add(clean)
                                        failed_by_id.pop(clean, None)
                                for client_id in apps_result.get("invalid") or []:
                                    clean = str(client_id or "").strip()
                                    if clean:
                                        invalid_ids.add(clean)
                                        failed_by_id.pop(clean, None)
                                for row in apps_result.get("failed") or []:
                                    if not isinstance(row, dict):
                                        continue
                                    clean = str(row.get("client_id") or "").strip()
                                    if clean:
                                        failed_by_id[clean] = {
                                            "client_id": clean,
                                            "error": str(row.get("error") or "").strip(),
                                        }

                                required_unresolved = [
                                    client_id
                                    for client_id in app_plan["required"]
                                    if client_id not in added_ids and client_id not in already_configured_ids
                                ]
                                if required_unresolved and attempt < self.admin_apps_attempts:
                                    sleep_seconds = min(120.0, self.admin_apps_retry_delay_seconds * attempt)
                                    log_event(
                                        "step_warning",
                                        "warn",
                                        "[configure_admin_apps] Required app IDs still unresolved; retrying.",
                                        {
                                            "attempt": attempt,
                                            "remaining_required": required_unresolved,
                                            "sleep_seconds": sleep_seconds,
                                        },
                                    )
                                    time.sleep(sleep_seconds)

                            failed_rows: List[Dict[str, str]] = []
                            for client_id in app_plan["all"]:
                                if client_id in added_ids or client_id in already_configured_ids:
                                    continue
                                if client_id in failed_by_id:
                                    failed_rows.append(failed_by_id[client_id])
                                else:
                                    failed_rows.append(
                                        {
                                            "client_id": client_id,
                                            "error": "No success confirmation from Google Admin app flow",
                                        }
                                    )

                            required_failed = sorted(
                                client_id
                                for client_id in app_plan["required"]
                                if client_id not in added_ids and client_id not in already_configured_ids
                            )
                            step_details.update(
                                {
                                    "attempts": attempt_records,
                                    "requested": app_plan["all"],
                                    "added": sorted(added_ids),
                                    "already_configured": sorted(already_configured_ids),
                                    "invalid": sorted(invalid_ids),
                                    "failed": failed_rows,
                                    "required_failed": required_failed,
                                }
                            )
                            if required_failed and self.require_admin_apps:
                                fail_message = (
                                    "Required Google app allowlist failed for "
                                    f"{required_failed}. failed={failed_rows}"
                                )
                                fail_step(step, fail_message)
                                persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                                raise RuntimeError(fail_message)

                            if failed_rows:
                                log_event(
                                    "step_warning",
                                    "warn",
                                    "[configure_admin_apps] Some app IDs failed to configure",
                                    {"failed": failed_rows},
                                )
                            complete_step(step, step_details)
                            persist_progress(INTERIM_STATUSES["ADMIN_APPS"], "in_progress")

                    if not dkim_checkpoint:
                        step = start_step("enable_dkim")
                        step_details: Dict[str, Any] = {"domain": domain_name}
                        if self.dry_run:
                            step_details["dry_run"] = True
                            complete_step(step, step_details)
                            persist_progress(INTERIM_STATUSES["DKIM_ENABLED"], "in_progress")
                        else:
                            log_event("step_started", "info", f"[enable_dkim] Resolving DKIM for {domain_name}")
                            dkim_record = browser.fetch_dkim_txt_record(domain_name)
                            step_details.update(dkim_record)

                            if not bool(dkim_record.get("already_enabled")):
                                dns_host = str(dkim_record.get("dns_host") or "").strip()
                                dns_value = str(dkim_record.get("dns_value") or "").strip()
                                if not dns_host or not dns_value:
                                    fail_step(step, f"Google Admin did not return DKIM DNS values for {domain_name}")
                                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                                    raise RuntimeError(f"Missing DKIM DNS values for {domain_name}")

                                dkim_name = self._normalize_dns_name_for_zone(dns_host, domain_name)
                                dkim_record_payload = {
                                    "type": "TXT",
                                    "name": dkim_name,
                                    "content": dns_value,
                                    "ttl": 3600,
                                }
                                dkim_dns_summary = self._get_cloudflare().upsert_dns_records(
                                    cloudflare_zone_id,
                                    [dkim_record_payload],
                                )
                                step_details["dns_write"] = {
                                    "name": dkim_name,
                                    "summary": dkim_dns_summary,
                                }

                                if (
                                    int(dkim_dns_summary.get("failed") or 0) > 0
                                    and int(dkim_dns_summary.get("created") or 0) == 0
                                    and int(dkim_dns_summary.get("skipped") or 0) == 0
                                ):
                                    fail_message = f"Cloudflare DKIM TXT write failed: {dkim_dns_summary}"
                                    fail_step(step, fail_message)
                                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                                    raise RuntimeError(fail_message)

                                if self.dkim_dns_wait_seconds > 0:
                                    time.sleep(self.dkim_dns_wait_seconds)

                                dkim_auth = browser.start_dkim_authentication(
                                    domain_name,
                                    attempts=self.dkim_auth_attempts,
                                    sleep_seconds=self.dkim_auth_interval_seconds,
                                )
                                step_details["authentication"] = dkim_auth
                                if not bool(dkim_auth.get("enabled")) and self.require_dkim_enabled:
                                    fail_message = (
                                        f"DKIM authentication did not reach enabled state for {domain_name}. "
                                        f"details={dkim_auth}"
                                    )
                                    fail_step(step, fail_message)
                                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                                    raise RuntimeError(fail_message)
                            else:
                                step_details["authentication"] = {
                                    "enabled": True,
                                    "already_enabled": True,
                                }

                            complete_step(step, step_details)
                            persist_progress(INTERIM_STATUSES["DKIM_ENABLED"], "in_progress")

                    if not profile_photo_checkpoint:
                        step = start_step("upload_profile_photos")
                        step_details: Dict[str, Any] = {
                            "optional": self.profile_photo_optional,
                        }
                        upload_pairs = self._build_profile_photo_upload_pairs(
                            domain_name,
                            inboxes,
                            user_updates,
                        )
                        step_details["requested"] = len(upload_pairs)

                        if self.dry_run:
                            step_details["dry_run"] = True
                            complete_step(step, step_details)
                            persist_progress(INTERIM_STATUSES["PROFILE_PHOTOS"], "in_progress")
                        elif not upload_pairs:
                            step_details["skipped"] = "No profile photos requested"
                            complete_step(step, step_details)
                            persist_progress(INTERIM_STATUSES["PROFILE_PHOTOS"], "in_progress")
                        else:
                            log_event(
                                "step_started",
                                "info",
                                f"[upload_profile_photos] Uploading {len(upload_pairs)} profile photos",
                                {"emails": [email for email, _ in upload_pairs]},
                            )
                            photo_response = browser.upload_profile_photos(upload_pairs)
                            step_details.update(photo_response)

                            if int(photo_response.get("failed") or 0) > 0:
                                if not self.profile_photo_optional:
                                    fail_message = (
                                        f"Playwright profile photo upload failed for "
                                        f"{photo_response.get('failed')} user(s)"
                                    )
                                    fail_step(step, fail_message)
                                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                                    raise RuntimeError(f"{fail_message}. details={photo_response}")

                                log_event(
                                    "step_warning",
                                    "warn",
                                    "[upload_profile_photos] Some profile photos failed to upload",
                                    {"failed": photo_response.get("failed")},
                                )

                            complete_step(step, step_details)
                            persist_progress(INTERIM_STATUSES["PROFILE_PHOTOS"], "in_progress")

            mfa_checkpoint = checkpoint("enroll_1password_2fa")
            if mfa_checkpoint:
                log_event(
                    "step_resumed",
                    "info",
                    "[enroll_1password_2fa] Reusing previous checkpoint.",
                    mfa_checkpoint,
                )
            else:
                step = start_step("enroll_1password_2fa")
                resumed_detail = last_step_details.get("enroll_1password_2fa") or {}
                completed_emails = {
                    str(v).strip().lower()
                    for v in (resumed_detail.get("completed_emails") or [])
                    if str(v or "").strip()
                }
                step_details: Dict[str, Any] = {
                    "completed_emails": sorted(completed_emails),
                    "failed_emails": [],
                    "items": dict(resumed_detail.get("items") or {}),
                    "required": self.require_mfa_enrollment,
                    "headless": self.playwright_headless,
                    "non_headless_fallback_enabled": self.mfa_non_headless_fallback,
                }
                step["details"] = step_details
                persist_progress(INTERIM_STATUSES["MFA_ENROLLMENT"], "in_progress")

                if self.dry_run:
                    step_details["dry_run"] = True
                    complete_step(step, step_details)
                    persist_progress(INTERIM_STATUSES["MFA_ENROLLMENT"], "in_progress")
                else:
                    pending_users = self._build_mfa_users(inboxes, user_updates, completed_emails)
                    if not pending_users:
                        step_details["skipped"] = "No pending users for MFA enrollment"
                        complete_step(step, step_details)
                        persist_progress(INTERIM_STATUSES["MFA_ENROLLMENT"], "in_progress")
                    else:
                        email_to_inbox_id: Dict[str, str] = {}
                        for inbox in inboxes:
                            inbox_id = str(inbox.get("id") or "").strip()
                            mapped = user_updates.get(inbox_id) or {}
                            email_key = str(mapped.get("email") or inbox.get("email") or "").strip().lower()
                            if inbox_id and email_key:
                                email_to_inbox_id[email_key] = inbox_id

                        try:
                            op_client = OnePasswordCliClient.from_env()
                        except Exception as op_exc:
                            message = f"1Password is not configured: {op_exc}"
                            if self.require_mfa_enrollment:
                                fail_step(step, message)
                                persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                                raise RuntimeError(message) from op_exc
                            step_details["skipped"] = message
                            complete_step(step, step_details)
                            persist_progress(INTERIM_STATUSES["MFA_ENROLLMENT"], "in_progress")
                            log_event("step_warning", "warn", f"[enroll_1password_2fa] {message}")
                            op_client = None

                        if op_client is not None:
                            def _progress(state: str, email: str, details: Dict[str, Any]) -> None:
                                clean_email = str(email or "").strip().lower()
                                if not clean_email:
                                    return
                                if state == "completed":
                                    if clean_email in step_details["failed_emails"]:
                                        step_details["failed_emails"] = [
                                            value for value in step_details["failed_emails"] if value != clean_email
                                        ]
                                    if clean_email not in step_details["completed_emails"]:
                                        step_details["completed_emails"].append(clean_email)
                                        step_details["completed_emails"] = sorted(step_details["completed_emails"])
                                    item_id = str(details.get("item_id") or "").strip()
                                    if item_id:
                                        step_details["items"][clean_email] = item_id
                                    inbox_id = email_to_inbox_id.get(clean_email)
                                    if inbox_id:
                                        inbox_updates: Dict[str, Any] = {}
                                        if item_id:
                                            inbox_updates["onepassword_item_id"] = item_id
                                        otp_secret = str(details.get("otp_secret") or "").strip()
                                        if otp_secret:
                                            inbox_updates["otp_secret"] = otp_secret
                                        if inbox_updates:
                                            try:
                                                self.client.update_inbox(inbox_id, inbox_updates)
                                            except Exception as inbox_exc:
                                                log_event(
                                                    "step_warning",
                                                    "warn",
                                                    f"[enroll_1password_2fa] Failed to persist MFA fields for {clean_email}",
                                                    {"email": clean_email, "error": str(inbox_exc)},
                                                )
                                else:
                                    if (
                                        clean_email not in step_details["failed_emails"]
                                        and clean_email not in step_details["completed_emails"]
                                    ):
                                        step_details["failed_emails"].append(clean_email)
                                persist_progress(INTERIM_STATUSES["MFA_ENROLLMENT"], "in_progress")

                            with GoogleAdminPlaywrightClient(headless=self.playwright_headless) as browser:
                                enroll_result = browser.enroll_users_mfa_with_1password(
                                    pending_users,
                                    op_client,
                                    progress_hook=_progress,
                                )
                            combined_results = dict(enroll_result.get("results") or {})
                            failed_emails = sorted(
                                email
                                for email, row in combined_results.items()
                                if str(row.get("status") or "").lower() == "failed"
                            )

                            if (
                                failed_emails
                                and self.playwright_headless
                                and self.mfa_non_headless_fallback
                                and not self.dry_run
                            ):
                                fallback_users = [
                                    user for user in pending_users if str(user.email or "").strip().lower() in failed_emails
                                ]
                                if fallback_users:
                                    log_event(
                                        "step_warning",
                                        "warn",
                                        "[enroll_1password_2fa] Retrying failed users in non-headless mode.",
                                        {"failed_users": failed_emails},
                                    )
                                    with GoogleAdminPlaywrightClient(headless=False) as fallback_browser:
                                        fallback_result = fallback_browser.enroll_users_mfa_with_1password(
                                            fallback_users,
                                            op_client,
                                            progress_hook=_progress,
                                            max_attempts=self.mfa_non_headless_max_attempts,
                                        )
                                    step_details["fallback_non_headless"] = {
                                        "attempted": sorted(
                                            str(user.email or "").strip().lower() for user in fallback_users
                                        ),
                                        "result": {
                                            "completed": int(fallback_result.get("completed") or 0),
                                            "failed": int(fallback_result.get("failed") or 0),
                                        },
                                    }
                                    combined_results.update(fallback_result.get("results") or {})

                            step_details["failed_results"] = {
                                email: row
                                for email, row in combined_results.items()
                                if str(row.get("status") or "").lower() == "failed"
                            }
                            step_details["result"] = {
                                "completed": len(step_details.get("completed_emails") or []),
                                "failed": len(step_details["failed_results"]),
                            }

                            if step_details["failed_results"] and self.require_mfa_enrollment:
                                fail_message = (
                                    f"MFA enrollment failed for {len(step_details['failed_results'])} "
                                    f"user(s): {sorted(step_details['failed_results'].keys())}"
                                )
                                fail_step(step, fail_message)
                                persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                                raise RuntimeError(fail_message)

                            complete_step(step, step_details)
                            persist_progress(INTERIM_STATUSES["MFA_ENROLLMENT"], "in_progress")

            verified, verification_reason = domain_verification_confirmed(domain_verification_details)
            if not verified:
                step = start_step("assert_domain_verified_before_upload")
                fail_message = (
                    f"Domain verification not confirmed for {domain_name}; stopping before sending-tool upload."
                )
                fail_step(step, fail_message)
                step["details"] = {
                    "domain": domain_name,
                    "reason": verification_reason,
                    "verify_domain_checkpoint_present": bool(verify_domain_checkpoint),
                    "verify_domain_details": domain_verification_details,
                    "ops_next_steps": [
                        "Review verify_domain step logs and Google Admin state.",
                        "Confirm the domain shows as verified in Google Admin > Domains.",
                        "Retry the action after verification succeeds.",
                    ],
                }
                persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                log_event(
                    "step_failed",
                    "error",
                    f"[assert_domain_verified_before_upload] {fail_message}",
                    step["details"],
                )
                raise RuntimeError(fail_message)

            sending_tool_checkpoint = checkpoint("upload_sending_tool")
            if sending_tool_checkpoint:
                persist_progress(INTERIM_STATUSES["SENDING_TOOL_UPLOAD"], "in_progress")
            else:
                step = start_step("upload_sending_tool")
                try:
                    upload_result = self._upload_domain_inboxes_to_sending_tool(
                        domain_id=domain_id,
                        domain_name=domain_name,
                        provider=provider,
                        inboxes=inboxes,
                        user_updates=user_updates,
                        tool_settings=domain.get("fulfillment_settings"),
                    )
                except Exception as upload_exc:
                    fail_message = str(upload_exc)
                    fail_step(step, fail_message)
                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                    raise RuntimeError(fail_message) from upload_exc

                strict_validation = bool(upload_result.get("strict_validation"))
                failed_uploads = upload_result.get("failed_uploads") or []
                if strict_validation and failed_uploads:
                    fail_message = (
                        f"{upload_result.get('tool') or 'sending tool'} upload validation failed for "
                        f"{len(failed_uploads)}/{upload_result.get('total_candidates') or 0} inbox(es): {failed_uploads}"
                    )
                    fail_step(step, fail_message)
                    persist_progress(INTERIM_STATUSES["FAILED"], "in_progress")
                    raise RuntimeError(fail_message)

                complete_step(step, upload_result)
                persist_progress(INTERIM_STATUSES["SENDING_TOOL_UPLOAD"], "in_progress")
                log_event(
                    "step_completed",
                    "info",
                    f"[upload_sending_tool] Upload result for {domain_name}",
                    upload_result,
                )

            step = start_step("finalize")

            # Re-check domain status before finalize. A cancellation may have
            # landed mid-flight; if so we must not flip the domain back to
            # 'active'. Skip finalize and let the cancel worker take over.
            fresh_domain = self.client.get_domain(domain_id) or {}
            fresh_status = str(fresh_domain.get("status") or "").strip().lower()
            if fresh_status in ("queued_for_cancellation", "cancelled", "suspended"):
                log_event(
                    "action_skipped",
                    "warn",
                    f"Domain {domain_name} became {fresh_status} during provisioning; aborting finalize",
                    {"domain_status": fresh_status},
                )
                complete_step(step, {"skipped": True, "reason": f"Domain is {fresh_status}"})
                self.client.complete_action(
                    action_id,
                    {
                        "skipped": True,
                        "reason": f"Domain is {fresh_status}",
                        "domain": domain_name,
                    },
                )
                return

            # Conditional update: never flip to active if status changed under us.
            self.client.update_domain_if_active(
                domain_id,
                {
                    "status": "active",
                    "interim_status": INTERIM_STATUSES["COMPLETE"],
                },
            )
            complete_step(step, {"status": "active"})
            persist_progress(INTERIM_STATUSES["COMPLETE"], "active")

            result = {
                "domain": domain_name,
                "order_id": order_id,
                "inboxes": len(inboxes),
                "dns": dns_summary,
                "dry_run": self.dry_run,
                "steps": steps,
            }
            self.client.complete_action(action_id, result)
            log_event("action_completed", "info", "Google provisioning completed", result)

        except Exception as exc:
            error_message = str(exc)
            action_logger.exception("Google worker action failed")
            hint = self._failure_hint(error_message)
            deferred_retry_seconds = self._deferred_retry_seconds(error_message)

            # Reset non-active inboxes so retry has clean ownership.
            try:
                for inbox in inboxes:
                    if inbox.get("status") != "active":
                        self.client.update_inbox(inbox["id"], {"status": "pending"})
            except Exception:
                pass

            if deferred_retry_seconds is not None:
                try:
                    if domain_id:
                        self.client.update_domain(
                            domain_id,
                            {
                                "interim_status": INTERIM_STATUSES["USERS_CREATED"],
                                "status": "in_progress",
                            },
                        )
                except Exception:
                    pass

                self.client.defer_action(
                    action,
                    error_message,
                    delay_seconds=deferred_retry_seconds,
                    consume_attempt=False,
                )
                log_event(
                    "action_deferred",
                    "warn",
                    f"Google provisioning deferred: {error_message}",
                    {
                        "error": error_message,
                        "hint": hint,
                        "steps": steps,
                        "retry_delay_seconds": deferred_retry_seconds,
                    },
                )
                return

            try:
                if domain_id:
                    self.client.update_domain(
                        domain_id,
                        {
                            "interim_status": INTERIM_STATUSES["FAILED"],
                            "status": "in_progress",
                        },
                    )
            except Exception:
                pass

            self.client.fail_action(action, error_message, max_retries=self.max_retries)
            log_event(
                "action_failed",
                "error",
                f"Google provisioning failed: {error_message}",
                {
                    "error": error_message,
                    "hint": hint,
                    "steps": steps,
                },
            )

    def _deferred_retry_seconds(self, error_message: str) -> Optional[float]:
        message = str(error_message or "").strip().lower()
        if "google admin login was not completed" in message:
            return self.admin_console_defer_seconds
        if "google admin page did not load" in message:
            return self.admin_console_defer_seconds
        if "required google app allowlist failed" in message:
            return self.admin_console_defer_seconds
        if "trusted-app entry point not found" in message and (
            "accounts.google.com" in message or "use your google account" in message
        ):
            return self.admin_console_defer_seconds
        return None

    def _build_partnerhub_users(
        self,
        domain_name: str,
        inboxes: List[Dict[str, Any]],
        payload: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        users: List[Dict[str, Any]] = []
        shared_password = str(payload.get("new_password") or payload.get("default_password") or "").strip()

        for index, inbox in enumerate(inboxes):
            username = str(inbox.get("username") or f"user{index + 1}").strip().lower()
            first_name = str(inbox.get("first_name") or payload.get("default_first_name") or "User").strip()
            last_name = str(inbox.get("last_name") or payload.get("default_last_name") or str(index + 1)).strip()

            email = str(inbox.get("email") or f"{username}@{domain_name}").strip().lower()
            password = str(inbox.get("password") or shared_password or self._generate_password()).strip()

            users.append(
                {
                    "email": email,
                    "password": password,
                    "firstName": first_name,
                    "lastName": last_name,
                    "userType": "admin" if index == 0 else "normal",
                }
            )

        return users

    def _persist_inbox_credentials(
        self,
        domain_name: str,
        inboxes: List[Dict[str, Any]],
        payload: Dict[str, Any],
    ) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        prepared: List[Dict[str, Any]] = []
        shared_password = str(payload.get("new_password") or payload.get("default_password") or "").strip()
        updated_rows = 0
        generated_passwords = 0
        shared_password_rows = 0

        for index, inbox in enumerate(inboxes):
            row = dict(inbox)
            username = str(row.get("username") or f"user{index + 1}").strip().lower()
            email = str(row.get("email") or f"{username}@{domain_name}").strip().lower()
            existing_password = str(row.get("password") or "").strip()
            password = existing_password or shared_password or self._generate_password()
            updates: Dict[str, Any] = {}

            if email and email != str(row.get("email") or "").strip().lower():
                updates["email"] = email
                row["email"] = email

            if password and password != existing_password:
                updates["password"] = password
                row["password"] = password
                updated_rows += 1
                if shared_password and password == shared_password:
                    shared_password_rows += 1
                elif not existing_password:
                    generated_passwords += 1

            if updates and row.get("id"):
                self.client.update_inbox(str(row["id"]), updates)

            prepared.append(row)

        return prepared, {
            "inboxes": len(prepared),
            "updated_rows": updated_rows,
            "generated_passwords": generated_passwords,
            "shared_password_rows": shared_password_rows,
        }

    def _build_inbox_updates(
        self,
        domain_name: str,
        inboxes: List[Dict[str, Any]],
        partnerhub_users: List[Dict[str, Any]],
        payload: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        updates: Dict[str, Dict[str, Any]] = {}
        default_password = str(payload.get("new_password") or payload.get("default_password") or "").strip()

        for idx, inbox in enumerate(inboxes):
            mapped = partnerhub_users[idx] if idx < len(partnerhub_users) else {}
            email = str(mapped.get("email") or inbox.get("email") or f"{inbox.get('username')}@{domain_name}")
            password = str(mapped.get("password") or inbox.get("password") or default_password or self._generate_password())
            user_type = str(mapped.get("userType") or "normal").strip().lower()
            updates[inbox["id"]] = {
                "email": email,
                "password": password,
                "is_admin": user_type == "admin",
            }
        return updates

    def _build_mfa_users(
        self,
        inboxes: List[Dict[str, Any]],
        user_updates: Dict[str, Dict[str, Any]],
        completed_emails: Optional[set] = None,
    ) -> List[GoogleMfaUser]:
        done = {str(v).strip().lower() for v in (completed_emails or set()) if str(v or "").strip()}
        users: List[GoogleMfaUser] = []

        for inbox in inboxes:
            inbox_id = str(inbox.get("id") or "").strip()
            mapped = user_updates.get(inbox_id) or {}
            email = str(mapped.get("email") or inbox.get("email") or "").strip().lower()
            password = str(mapped.get("password") or inbox.get("password") or "").strip()
            username = str(inbox.get("username") or "").strip().lower()
            if not email or not password:
                continue
            if email in done:
                continue
            users.append(
                GoogleMfaUser(
                    email=email,
                    password=password,
                    username=username,
                )
            )
        return users

    def _build_profile_photo_upload_pairs(
        self,
        domain_name: str,
        inboxes: List[Dict[str, Any]],
        user_updates: Dict[str, Dict[str, Any]],
    ) -> List[tuple[str, str]]:
        seen = set()
        uploads: List[tuple[str, str]] = []

        for inbox in inboxes:
            inbox_id = str(inbox.get("id") or "").strip()
            mapped = user_updates.get(inbox_id) or {}
            username = str(inbox.get("username") or "").strip().lower()
            email = str(mapped.get("email") or inbox.get("email") or f"{username}@{domain_name}").strip().lower()
            profile_pic_url = str(inbox.get("profile_pic_url") or "").strip()
            if not email or not profile_pic_url or email in seen:
                continue
            seen.add(email)
            uploads.append((email, profile_pic_url))

        return uploads

    def _parse_google_app_ids(self, value: Any) -> List[str]:
        parsed: List[str] = []
        seen = set()

        for entry in self._flatten_google_app_id_input(value):
            clean = str(entry or "").strip()
            if not clean:
                continue
            if not self._is_valid_google_app_id(clean):
                logger.warning("Ignoring invalid GOOGLE_REQUIRED_ADMIN_APP_IDS entry: %s", clean)
                continue
            if clean in seen:
                continue
            seen.add(clean)
            parsed.append(clean)

        return parsed

    def _resolve_google_admin_app_ids(self, payload: Dict[str, Any]) -> Dict[str, List[str]]:
        required = [v for v in self.required_google_app_ids if self._is_valid_google_app_id(v)]
        optional: List[str] = []
        invalid: List[str] = []

        if self._as_bool(payload.get("master_inbox_enable"), default=False):
            optional.append(OPTIONAL_GOOGLE_APP_IDS["master_inbox"])
        if self._as_bool(payload.get("warmy_enable"), default=False) or self._as_bool(
            payload.get("warmy_enabled"),
            default=False,
        ):
            optional.append(OPTIONAL_GOOGLE_APP_IDS["warmy"])
        if self._as_bool(payload.get("plusvibe_enable"), default=False) or self._as_bool(
            payload.get("plusvibe_enabled"),
            default=False,
        ):
            optional.append(OPTIONAL_GOOGLE_APP_IDS["plusvibe"])

        bison_app_id = str(payload.get("bison_app_id") or "").strip()
        if bison_app_id:
            if self._is_valid_google_app_id(bison_app_id):
                optional.append(bison_app_id)
            else:
                invalid.append(bison_app_id)

        raw_additional = (
            payload.get("additional_tools_id")
            or payload.get("additional_app_ids")
            or payload.get("google_app_ids")
            or payload.get("admin_app_ids")
        )
        for entry in self._flatten_google_app_id_input(raw_additional):
            if self._is_valid_google_app_id(entry):
                optional.append(entry)
            else:
                invalid.append(entry)

        merged: List[str] = []
        seen = set()
        for candidate in required + optional:
            clean = str(candidate or "").strip()
            if not clean or clean in seen:
                continue
            seen.add(clean)
            merged.append(clean)

        return {
            "required": [v for v in required if v in merged],
            "optional": [v for v in optional if v in merged and v not in required],
            "invalid": sorted({v for v in invalid if v}),
            "all": merged,
        }

    def _flatten_google_app_id_input(self, value: Any) -> List[str]:
        if value is None:
            return []

        parsed = value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            if text.startswith("["):
                try:
                    parsed = json.loads(text)
                except Exception:
                    parsed = text
            elif "," in text:
                parsed = [v.strip() for v in text.split(",") if v.strip()]
            else:
                parsed = text

        if isinstance(parsed, list):
            return [str(v).strip() for v in parsed if str(v or "").strip()]
        if isinstance(parsed, dict):
            values = []
            for candidate in parsed.values():
                values.extend(self._flatten_google_app_id_input(candidate))
            return values

        text = str(parsed or "").strip()
        return [text] if text else []

    @staticmethod
    def _is_valid_google_app_id(value: str) -> bool:
        text = str(value or "").strip().lower()
        return bool(text and text.endswith(".apps.googleusercontent.com"))

    @staticmethod
    def _normalize_dns_name_for_zone(dns_host: str, domain_name: str) -> str:
        host = str(dns_host or "").strip().rstrip(".").lower()
        domain = str(domain_name or "").strip().rstrip(".").lower()
        if not host:
            return "@"
        if host == domain:
            return "@"
        suffix = f".{domain}"
        if host.endswith(suffix):
            relative = host[: -len(suffix)].strip(".")
            return relative or "@"
        return host

    def _extract_custom_dns(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        custom_dns = payload.get("custom_dns_records")
        if not isinstance(custom_dns, list):
            return []

        valid: List[Dict[str, Any]] = []
        for record in custom_dns:
            if not isinstance(record, dict):
                continue
            rec_type = str(record.get("type") or "").upper().strip()
            name = str(record.get("name") or "").strip()
            content = str(record.get("content") or "").strip()
            if rec_type not in {"A", "AAAA", "CNAME", "TXT", "MX", "SRV"}:
                continue
            if not name or not content:
                continue

            normalized: Dict[str, Any] = {
                "type": rec_type,
                "name": name,
                "content": content,
                "ttl": int(record.get("ttl") or 3600),
            }
            if rec_type == "MX" and record.get("priority") is not None:
                normalized["priority"] = int(record.get("priority") or 1)
            valid.append(normalized)
        return valid

    def _default_google_dns_records(self, _domain_name: str) -> List[Dict[str, Any]]:
        return [
            {"type": "MX", "name": "@", "content": "ASPMX.L.GOOGLE.COM", "priority": 1, "ttl": 3600},
            {"type": "MX", "name": "@", "content": "ALT1.ASPMX.L.GOOGLE.COM", "priority": 5, "ttl": 3600},
            {"type": "MX", "name": "@", "content": "ALT2.ASPMX.L.GOOGLE.COM", "priority": 5, "ttl": 3600},
            {"type": "MX", "name": "@", "content": "ALT3.ASPMX.L.GOOGLE.COM", "priority": 10, "ttl": 3600},
            {"type": "MX", "name": "@", "content": "ALT4.ASPMX.L.GOOGLE.COM", "priority": 10, "ttl": 3600},
            {"type": "TXT", "name": "@", "content": "v=spf1 include:_spf.google.com ~all", "ttl": 3600},
            {"type": "TXT", "name": "_dmarc", "content": "v=DMARC1; p=none", "ttl": 3600},
        ]

    def _get_dynadot(self) -> DynadotClient:
        if self._dynadot is None:
            api_key = os.getenv("DYNADOT_API_KEY", "").strip()
            self._dynadot = DynadotClient(api_key)
        return self._dynadot

    def _get_cloudflare(self) -> CloudflareClient:
        if self._cloudflare is None:
            api_token = os.getenv("CLOUDFLARE_API_TOKEN", "").strip()
            account_id = os.getenv("CLOUDFLARE_ACCOUNT_ID", "").strip()
            global_key = (
                os.getenv("CLOUDFLARE_GLOBAL_KEY", "").strip()
                or os.getenv("CLOUDFLARE_GLOBAL_API_KEY", "").strip()
            )
            global_email = os.getenv("CLOUDFLARE_EMAIL", "").strip()
            self._cloudflare = CloudflareClient(
                api_token=api_token,
                account_id=account_id,
                global_api_key=global_key,
                global_email=global_email,
            )
            logger.info(
                "Cloudflare auth configured (mode=%s, global_fallback=%s, account_id_set=%s)",
                "token" if api_token else "global",
                bool(api_token and global_key and global_email),
                bool(account_id),
            )
        return self._cloudflare

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

    @staticmethod
    def _failure_hint(error_message: str) -> str:
        text = str(error_message or "").lower()
        if "cloudflare" in text and "authentication" in text:
            return (
                "Cloudflare auth failed. Use valid CLOUDFLARE_API_TOKEN or set "
                "CLOUDFLARE_GLOBAL_KEY + CLOUDFLARE_EMAIL."
            )
        if "does not currently support this domain name" in text:
            return "PartnerHub rejected this domain/TLD for Google Workspace. Try a different domain."
        if "not active yet" in text:
            return "Cloudflare zone is not active yet. Wait for nameserver propagation and retry."
        if "domain already exists" in text:
            return "Domain already exists in PartnerHub; worker can reuse existing order details."
        return "Check action_logs metadata for the exact failing step and API response."

    def _upload_domain_inboxes_to_sending_tool(
        self,
        *,
        domain_id: str,
        domain_name: str,
        provider: str,
        inboxes: List[Dict[str, Any]],
        user_updates: Dict[str, Dict[str, Any]],
        tool_settings: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        tool_bundle = self.client.get_domain_tool_credentials(domain_id)
        if not tool_bundle:
            return {
                "tool": None,
                "strict_validation": False,
                "total_candidates": 0,
                "uploaded_emails": [],
                "failed_uploads": [],
                "skipped_already_uploaded": 0,
                "skipped": "No sending tool credentials assigned to domain",
            }

        tool_slug = self._normalize_sending_tool_slug(str(tool_bundle.get("slug") or ""))
        credential = tool_bundle.get("credential") or {}
        strict_validation = tool_slug in {"instantly.ai", "smartlead.ai"}
        if not tool_slug:
            return {
                "tool": None,
                "strict_validation": False,
                "total_candidates": 0,
                "uploaded_emails": [],
                "failed_uploads": [],
                "skipped_already_uploaded": 0,
                "skipped": "Sending tool slug missing on credential",
            }

        if tool_slug not in {"instantly.ai", "smartlead.ai"}:
            return {
                "tool": tool_slug,
                "strict_validation": False,
                "total_candidates": 0,
                "uploaded_emails": [],
                "failed_uploads": [],
                "skipped_already_uploaded": 0,
                "skipped": f"Automated upload not implemented for {tool_slug}",
            }

        api_key = str(credential.get("api_key") or "").strip()
        if not api_key:
            raise RuntimeError(
                f"Missing API key for {tool_slug} credential on domain {domain_name}. "
                "Provide toolCredentials.api when placing the order."
            )

        payloads: List[Dict[str, Any]] = []
        failed_uploads: List[Dict[str, str]] = []
        seen_emails = set()
        for inbox in inboxes:
            inbox_id = str(inbox.get("id") or "").strip()
            mapped = user_updates.get(inbox_id) or {}
            username = str(inbox.get("username") or "").strip().lower()
            email = str(mapped.get("email") or inbox.get("email") or f"{username}@{domain_name}").strip().lower()
            password = str(mapped.get("password") or inbox.get("password") or "").strip()
            if not email:
                continue
            if email in seen_emails:
                continue
            seen_emails.add(email)
            if not password:
                failed_uploads.append({"email": email, "error": "Missing inbox password"})
                continue
            payloads.append(
                {
                    "email": email,
                    "first_name": str(inbox.get("first_name") or "").strip(),
                    "last_name": str(inbox.get("last_name") or "").strip(),
                    "password": password,
                    "provider": provider,
                }
            )

        if not payloads and failed_uploads:
            return {
                "tool": tool_slug,
                "strict_validation": strict_validation,
                "total_candidates": len(failed_uploads),
                "uploaded_emails": [],
                "failed_uploads": failed_uploads,
                "skipped_already_uploaded": 0,
            }
        if not payloads:
            return {
                "tool": tool_slug,
                "strict_validation": strict_validation,
                "total_candidates": 0,
                "uploaded_emails": [],
                "failed_uploads": [],
                "skipped_already_uploaded": 0,
                "skipped": "No inbox payloads available for upload",
            }

        provider_name = str(provider or "").strip().lower()
        op_client: Optional[OnePasswordCliClient] = None
        if (
            tool_slug in {"instantly.ai", "smartlead.ai"}
            and provider_name == "google"
            and self.sending_tool_playwright_oauth
        ):
            try:
                op_client = OnePasswordCliClient.from_env()
            except Exception as op_exc:
                if self.sending_tool_require_1password:
                    raise RuntimeError(
                        f"1Password is required for {tool_slug} Google OAuth upload but not configured: {op_exc}"
                    ) from op_exc
                logger.warning(
                    "[upload_sending_tool] Proceeding without 1Password during Google OAuth upload for %s: %s",
                    domain_name,
                    op_exc,
                )

        result = self._sending_tool_uploader.upload_and_validate(
            tool=tool_slug,
            api_key=api_key,
            inboxes=payloads,
            provider=provider_name,
            credential=credential,
            settings=tool_settings if isinstance(tool_settings, dict) else {},
            onepassword=op_client,
            headless=self.playwright_headless,
            use_playwright_oauth=self.sending_tool_playwright_oauth,
        )

        merged_failures = list(result.get("failed_uploads") or [])
        merged_failures.extend(failed_uploads)
        result["failed_uploads"] = merged_failures
        result["strict_validation"] = strict_validation
        result["total_candidates"] = max(
            int(result.get("total_candidates") or 0),
            len(payloads) + len(failed_uploads),
        )
        return result

    @staticmethod
    def _normalize_sending_tool_slug(raw: str) -> str:
        text = str(raw or "").strip().lower()
        if "instantly" in text:
            return "instantly.ai"
        if "smartlead" in text:
            return "smartlead.ai"
        if "plusvibe" in text:
            return "plusvibe"
        if "bison" in text:
            return "email-bison"
        if "master" in text:
            return "masterinbox.com"
        return text
