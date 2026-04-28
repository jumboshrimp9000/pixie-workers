import os
import re
import time
import logging
import threading
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any, Dict, List, Optional, Union

import requests


logger = logging.getLogger(__name__)

APP_URL = os.getenv("APP_URL", "https://app.simpleinboxes.com").rstrip("/")

INTERNAL_FAILURE_PATTERNS = [
    re.compile(r"cannot find module", re.I),
    re.compile(r"playwright dependency", re.I),
    re.compile(r"missing dependency", re.I),
    re.compile(r"module not found", re.I),
    re.compile(r"syntaxerror|referenceerror|typeerror", re.I),
    re.compile(r"supabase .* failed", re.I),
]

TEMPORARY_FAILURE_PATTERNS = [
    re.compile(r"job in progress", re.I),
    re.compile(r"temporar", re.I),
    re.compile(r"rate limit|too many requests", re.I),
    re.compile(r"timed out|timeout", re.I),
    re.compile(r"pending|propagat|not ready yet|still processing", re.I),
]

CREDENTIAL_FAILURE_PATTERNS = [
    re.compile(r"credential", re.I),
    re.compile(r"api key|apikey", re.I),
    re.compile(r"unauthori[sz]ed|forbidden|\b401\b|\b403\b", re.I),
    re.compile(r"invalid token|invalid key|oauth", re.I),
    re.compile(r"workspace.*not found", re.I),
    re.compile(r"login failed|password|auth|permission|access denied", re.I),
]

UPLOAD_FAILURE_PATTERNS = [
    re.compile(r"sending[- ]?tool|upload|re-?upload", re.I),
    re.compile(r"instantly|smartlead|email[- ]?bison|plusvibe|pipl|masterinbox", re.I),
]


class SupabaseError(RuntimeError):
    pass


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value)
    except Exception:
        return None


class _ActionLeaseHeartbeat:
    def __init__(self, client: Any, action: Dict[str, Any], interval_seconds: float):
        self.client = client
        self.action = action
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def __enter__(self) -> "_ActionLeaseHeartbeat":
        if "_lease_heartbeat_lock" not in self.action:
            self.action["_lease_heartbeat_lock"] = threading.RLock()
        self._thread = threading.Thread(target=self._run, name=f"action-lease-{self.action.get('id')}", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop()

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                lock = self.action.get("_lease_heartbeat_lock")
                if lock:
                    with lock:
                        refreshed = self.client.heartbeat_action(self.action)
                else:
                    refreshed = self.client.heartbeat_action(self.action)
                if not refreshed:
                    logger.warning("Lease heartbeat lost fence for action %s", self.action.get("id"))
                    self._stop.set()
                    return
            except Exception as exc:
                logger.warning("Lease heartbeat failed for action %s: %s", self.action.get("id"), exc)


class SupabaseRestClient:
    def __init__(self, supabase_url: str, service_role_key: str, timeout_seconds: int = 30):
        self.base_url = f"{supabase_url.rstrip('/')}/rest/v1"
        self.timeout_seconds = timeout_seconds
        self.max_request_attempts = max(1, int(os.getenv("SUPABASE_HTTP_RETRIES", "3")))
        self.retry_backoff_seconds = max(0.25, float(os.getenv("SUPABASE_HTTP_BACKOFF_SECONDS", "1")))
        self.action_lease_seconds = max(30.0, float(os.getenv("WORKER_ACTION_LEASE_SECONDS", "600")))
        self.base_headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json",
        }
        self.session = requests.Session()
        self.action_heartbeat_seconds = max(
            5.0,
            min(
                self.action_lease_seconds / 3.0,
                float(os.getenv("WORKER_ACTION_HEARTBEAT_SECONDS", "120")),
            ),
        )

    @classmethod
    def from_env(cls) -> "SupabaseRestClient":
        supabase_url = os.getenv("SUPABASE_URL", "").strip()
        service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        if not supabase_url or not service_role_key:
            raise SupabaseError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
        return cls(supabase_url, service_role_key)

    def _request(
        self,
        method: str,
        table: str,
        params: Optional[Dict[str, str]] = None,
        payload: Optional[Dict[str, Any]] = None,
        prefer: str = "return=representation",
    ) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/{table}"
        headers = dict(self.base_headers)
        headers["Prefer"] = prefer

        response: Optional[requests.Response] = None
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_request_attempts + 1):
            try:
                response = self.session.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    params=params or {},
                    json=payload,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.max_request_attempts:
                    raise SupabaseError(
                        f"Supabase {method.upper()} {table} transport error after {attempt} attempts: {exc}"
                    ) from exc
                time.sleep(min(10.0, self.retry_backoff_seconds * attempt))
                continue

            if response.status_code in {408, 425, 429, 500, 502, 503, 504} and attempt < self.max_request_attempts:
                time.sleep(min(10.0, self.retry_backoff_seconds * attempt))
                continue

            break

        if response is None:
            raise SupabaseError(
                f"Supabase {method.upper()} {table} transport error: {last_error or 'Unknown request failure'}"
            )

        if response.status_code >= 300:
            body = response.text
            raise SupabaseError(
                f"Supabase {method.upper()} {table} failed ({response.status_code}): {body}"
            )

        if not response.text:
            return []

        data = response.json()
        return data if isinstance(data, list) else [data]

    def get_actions(self, action_types: List[str], limit: int = 5) -> List[Dict[str, Any]]:
        if not action_types:
            return []
        encoded_types = ",".join(action_types)
        pending_rows = self._request(
            "GET",
            "actions",
            params={
                "select": "*",
                "status": "eq.pending",
                "type": f"in.({encoded_types})",
                "order": "created_at.asc",
                "limit": str(limit),
            },
        )
        reclaim_before = _to_iso(_utc_now() - timedelta(seconds=self.action_lease_seconds))
        stale_rows = self._request(
            "GET",
            "actions",
            params={
                "select": "*",
                "status": "eq.in_progress",
                "type": f"in.({encoded_types})",
                "or": f"(started_at.is.null,started_at.lte.{reclaim_before})",
                "order": "created_at.asc",
                "limit": str(limit),
            },
        )

        now = _utc_now()
        eligible: List[Dict[str, Any]] = []
        for row in [*pending_rows, *stale_rows]:
            attempts = int(row.get("attempts") or 0)
            max_attempts = int(row.get("max_attempts") or 3)
            if attempts >= max_attempts:
                continue
            if str(row.get("status") or "") == "pending":
                next_retry_at = _parse_ts(row.get("next_retry_at"))
                if next_retry_at and next_retry_at > now:
                    continue
            eligible.append(row)
        eligible.sort(key=lambda row: str(row.get("created_at") or ""))
        return eligible[:limit]

    def claim_action(self, action: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        action_id = action["id"]
        attempts = int(action.get("attempts") or 0)
        params = {
            "id": f"eq.{action_id}",
            "attempts": f"eq.{attempts}",
            "status": f"eq.{action.get('status')}",
        }
        if str(action.get("status") or "") == "in_progress":
            reclaim_before = _to_iso(_utc_now() - timedelta(seconds=self.action_lease_seconds))
            if action.get("started_at"):
                params["started_at"] = f"lte.{reclaim_before}"
            else:
                params["started_at"] = "is.null"
        claimed_rows = self._request(
            "PATCH",
            "actions",
            params=params,
            payload={
                "status": "in_progress",
                "attempts": attempts + 1,
                "started_at": _to_iso(_utc_now()),
                "error": None,
                "next_retry_at": None,
            },
        )
        if not claimed_rows:
            return None
        return claimed_rows[0]

    def _action_fence_params(self, action: Dict[str, Any]) -> Dict[str, str]:
        params = {
            "id": f"eq.{action['id']}",
            "status": "eq.in_progress",
            "attempts": f"eq.{int(action.get('attempts') or 0)}",
        }
        if action.get("started_at"):
            params["started_at"] = f"eq.{action.get('started_at')}"
        else:
            params["started_at"] = "is.null"
        return params

    def _action_params(self, action_or_id: Union[str, Dict[str, Any]]) -> Dict[str, str]:
        if isinstance(action_or_id, dict):
            return self._action_fence_params(action_or_id)
        return {"id": f"eq.{action_or_id}"}

    def heartbeat_action(self, action: Dict[str, Any]) -> bool:
        next_started_at = _to_iso(_utc_now())
        rows = self._request(
            "PATCH",
            "actions",
            params=self._action_fence_params(action),
            payload={
                "started_at": next_started_at,
            },
        )
        if not rows:
            return False
        action["started_at"] = rows[0].get("started_at") or next_started_at
        return True

    def action_lease_heartbeat(self, action: Dict[str, Any]) -> "_ActionLeaseHeartbeat":
        return _ActionLeaseHeartbeat(self, action, self.action_heartbeat_seconds)

    def complete_action(self, action_or_id: Union[str, Dict[str, Any]], result: Dict[str, Any]) -> bool:
        lock = action_or_id.get("_lease_heartbeat_lock") if isinstance(action_or_id, dict) else None
        if lock:
            with lock:
                rows = self._request(
                    "PATCH",
                    "actions",
                    params=self._action_params(action_or_id),
                    payload={
                        "status": "completed",
                        "result": result,
                        "completed_at": _to_iso(_utc_now()),
                        "error": None,
                        "next_retry_at": None,
                    },
                )
        else:
            rows = self._request(
                "PATCH",
                "actions",
                params=self._action_params(action_or_id),
                payload={
                    "status": "completed",
                    "result": result,
                    "completed_at": _to_iso(_utc_now()),
                    "error": None,
                    "next_retry_at": None,
                },
            )
        if not rows and isinstance(action_or_id, dict):
            logger.warning("Skipped completion for action %s because the lease fence no longer matched", action_or_id.get("id"))
        if rows and isinstance(action_or_id, dict):
            self._resolve_fulfillment_notifications_for_completed_action(action_or_id)
        return bool(rows)

    def update_action(self, action_or_id: Union[str, Dict[str, Any]], fields: Dict[str, Any]) -> bool:
        lock = action_or_id.get("_lease_heartbeat_lock") if isinstance(action_or_id, dict) else None
        if lock:
            with lock:
                rows = self._request(
                    "PATCH",
                    "actions",
                    params=self._action_params(action_or_id),
                    payload=fields,
                )
        else:
            rows = self._request(
                "PATCH",
                "actions",
                params=self._action_params(action_or_id),
                payload=fields,
            )
        if not rows and isinstance(action_or_id, dict):
            logger.warning("Skipped action update for %s because the lease fence no longer matched", action_or_id.get("id"))
        return bool(rows)

    def fail_action(self, action: Dict[str, Any], error_message: str, max_retries: int = 5) -> bool:
        attempts = int(action.get("attempts") or 1)
        is_final = attempts >= max_retries
        delay_seconds = min(2 ** max(0, attempts - 1), 300)
        next_retry = None if is_final else _to_iso(_utc_now() + timedelta(seconds=delay_seconds))

        lock = action.get("_lease_heartbeat_lock")
        if lock:
            with lock:
                rows = self._request(
                    "PATCH",
                    "actions",
                    params=self._action_fence_params(action),
                    payload={
                        "status": "failed" if is_final else "pending",
                        "error": error_message[:4000],
                        "next_retry_at": next_retry,
                        "started_at": action.get("started_at") if is_final else None,
                    },
                )
        else:
            rows = self._request(
                "PATCH",
                "actions",
                params=self._action_fence_params(action),
                payload={
                    "status": "failed" if is_final else "pending",
                    "error": error_message[:4000],
                    "next_retry_at": next_retry,
                    "started_at": action.get("started_at") if is_final else None,
                    },
                )
        if not rows:
            logger.warning("Skipped failure update for action %s because the lease fence no longer matched", action.get("id"))
        elif is_final:
            self.notify_fulfillment_blocker_if_customer_action_required(action, error_message)
        return bool(rows)

    def _matches_any(self, text: str, patterns: List[re.Pattern]) -> bool:
        return any(pattern.search(text) for pattern in patterns)

    def _tool_label_for_domain(self, domain_id: str) -> str:
        try:
            rows = self._request(
                "GET",
                "domain_credentials",
                params={
                    "select": "sending_tool_credentials(sending_tools(slug,name))",
                    "domain_id": f"eq.{domain_id}",
                    "limit": "1",
                },
            )
            tool = rows[0].get("sending_tool_credentials", {}).get("sending_tools", {}) if rows else {}
            return str(tool.get("name") or tool.get("slug") or "your sending tool").strip()
        except Exception as exc:
            logger.warning("Failed to load sending-tool label for %s: %s", domain_id, exc)
            return "your sending tool"

    def _active_inbox_count(self, domain_id: str) -> int:
        try:
            rows = self._request(
                "GET",
                "inboxes",
                params={
                    "select": "id",
                    "domain_id": f"eq.{domain_id}",
                    "status": "eq.active",
                },
                prefer="count=exact",
            )
            return len(rows)
        except Exception:
            return 0

    def _customer_email(self, customer_id: str) -> str:
        rows = self._request(
            "GET",
            "customers",
            params={"select": "email", "id": f"eq.{customer_id}", "limit": "1"},
        )
        return str(rows[0].get("email") or "").strip().lower() if rows else ""

    def _notification_exists(self, customer_id: str, dedupe_key: str) -> bool:
        rows = self._request(
            "GET",
            "notifications",
            params={
                "select": "id",
                "customer_id": f"eq.{customer_id}",
                "dedupe_key": f"eq.{dedupe_key}",
                "limit": "1",
            },
        )
        return bool(rows)

    def _send_action_required_email(self, to_email: str, title: str, message: str, required_action: str, action_url: str) -> None:
        api_key = os.getenv("RESEND_API_KEY", "").strip()
        if not api_key:
            logger.info("RESEND_API_KEY missing; skipped customer blocker email to %s: %s", to_email, title)
            return
        from_email = os.getenv("EMAIL_FROM", "SimpleInboxes <noreply@simpleinboxes.com>")
        html = f"""
        <div style="font-family:Arial,sans-serif;background:#f5f8ff;padding:28px;">
          <div style="max-width:620px;margin:0 auto;background:white;border:1px solid #d8e4f3;border-radius:16px;overflow:hidden;">
            <div style="padding:26px 30px;background:#fff7ed;border-bottom:1px solid #fed7aa;">
              <div style="font-size:12px;font-weight:800;color:#dc2626;text-transform:uppercase;letter-spacing:.08em;">Action required</div>
              <h1 style="margin:14px 0 0;color:#0f172a;font-size:28px;line-height:1.15;">{escape(title)}</h1>
            </div>
            <div style="padding:28px 30px;color:#334155;font-size:15px;line-height:1.6;">
              <p>{escape(message)}</p>
              <div style="margin:18px 0;padding:14px 16px;border:1px solid #fecaca;background:#fef2f2;border-radius:10px;color:#991b1b;">
                {escape(required_action)}
              </div>
              <p>Once this is fixed, SimpleInboxes will retry the blocked fulfillment step automatically.</p>
              <a href="{escape(action_url)}" style="display:inline-block;margin-top:10px;padding:12px 18px;border-radius:10px;background:#dc2626;color:white;text-decoration:none;font-weight:800;">Fix this now</a>
            </div>
          </div>
        </div>
        """
        response = self.session.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "from": from_email,
                "to": [to_email],
                "subject": title,
                "html": html,
                "text": f"{message}\n\nAction needed: {required_action}\n\nOpen: {action_url}",
            },
            timeout=self.timeout_seconds,
        )
        if response.status_code >= 300:
            logger.warning("Resend failed for customer blocker email (%s): %s", response.status_code, response.text)

    def notify_fulfillment_blocker_if_customer_action_required(self, action: Dict[str, Any], error_message: str) -> bool:
        domain_id = str(action.get("domain_id") or "").strip()
        customer_id = str(action.get("customer_id") or "").strip()
        if not domain_id or not customer_id:
            return False

        try:
            domain = self.get_domain(domain_id)
        except Exception as exc:
            logger.warning("Failed to load domain for customer blocker notification: %s", exc)
            return False
        if not domain:
            return False

        text = " ".join([
            str(action.get("type") or ""),
            str(action.get("status") or ""),
            str(domain.get("status") or ""),
            str(domain.get("interim_status") or ""),
            str(error_message or ""),
        ])
        if self._matches_any(text, INTERNAL_FAILURE_PATTERNS):
            return False
        if self._matches_any(text, TEMPORARY_FAILURE_PATTERNS) and not self._matches_any(text, CREDENTIAL_FAILURE_PATTERNS):
            return False
        if not (self._matches_any(text, UPLOAD_FAILURE_PATTERNS) or self._matches_any(text, CREDENTIAL_FAILURE_PATTERNS)):
            return False

        domain_name = str(domain.get("domain") or "your domain").strip()
        tool = self._tool_label_for_domain(domain_id)
        inbox_count = self._active_inbox_count(domain_id)
        inbox_text = f"{inbox_count} inbox{'es' if inbox_count != 1 else ''}" if inbox_count > 0 else "Your inboxes"
        inbox_verb = "was" if inbox_count == 1 else "were"
        action_url = f"/credential-issues?domain={domain_name}"
        dedupe_tool = re.sub(r"[^a-z0-9]+", "-", tool.lower()).strip("-") or "sending-tool"
        dedupe_key = f"fulfillment-blocker:{domain_id}:sending-tool-credentials:{dedupe_tool}"

        try:
            already_notified = self._notification_exists(customer_id, dedupe_key)
            if not already_notified:
                self._request(
                    "POST",
                    "notifications",
                    payload={
                        "customer_id": customer_id,
                        "workspace_id": domain.get("workspace_id"),
                        "category": "action_required",
                        "type": "sending_tool_credentials_required",
                        "severity": "action_required",
                        "title": f"Update {tool} credentials for {domain_name}",
                        "body": f"{inbox_text} {inbox_verb} created, but upload to {tool} is blocked. Update the credential and we will retry automatically.",
                        "entity_type": "domain",
                        "entity_id": domain_id,
                        "domain_id": domain_id,
                        "order_batch_id": domain.get("order_batch_id") or action.get("order_batch_id"),
                        "action_url": action_url,
                        "dedupe_key": dedupe_key,
                        "metadata": {
                            "domain": domain_name,
                            "tool": tool,
                            "inboxCount": inbox_count,
                            "latestActionType": action.get("type"),
                            "latestActionError": str(error_message or "")[:1000],
                            "owner": "customer",
                            "requiredAction": f"Update the {tool} credential, then retry upload.",
                        },
                    },
                )
                to_email = self._customer_email(customer_id)
                if to_email:
                    self._send_action_required_email(
                        to_email,
                        f"Update {tool} credentials for {domain_name}",
                        f"{inbox_text} {inbox_verb} created, but upload to {tool} is blocked. Update the {tool} credential and we will retry automatically.",
                        f"Update the {tool} credential, then retry upload.",
                        f"{APP_URL}{action_url}",
                    )
            return True
        except Exception as exc:
            logger.warning("Failed to create customer blocker notification for %s: %s", domain_name, exc)
            return False

    def _resolve_fulfillment_notifications_for_completed_action(self, action: Dict[str, Any]) -> None:
        action_type = str(action.get("type") or "").lower()
        if not any(token in action_type for token in ["provision", "reupload"]):
            return
        domain_id = str(action.get("domain_id") or "").strip()
        customer_id = str(action.get("customer_id") or "").strip()
        if not domain_id or not customer_id:
            return
        now = _to_iso(_utc_now())
        try:
            self._request(
                "PATCH",
                "notifications",
                params={
                    "customer_id": f"eq.{customer_id}",
                    "domain_id": f"eq.{domain_id}",
                    "category": "eq.action_required",
                    "dismissed_at": "is.null",
                },
                payload={
                    "read_at": now,
                    "dismissed_at": now,
                    "metadata": {
                        "resolved": True,
                        "resolvedAt": now,
                        "resolvedBy": f"{action_type}_completed",
                    },
                },
            )
        except Exception as exc:
            logger.warning("Failed to resolve customer blocker notifications for %s: %s", domain_id, exc)

    def defer_action(
        self,
        action: Dict[str, Any],
        error_message: str,
        *,
        delay_seconds: float = 60,
        consume_attempt: bool = False,
    ) -> bool:
        claimed_attempts = int(action.get("attempts") or 1)
        effective_attempts = claimed_attempts if consume_attempt else max(0, claimed_attempts - 1)
        next_retry = _to_iso(_utc_now() + timedelta(seconds=max(5.0, float(delay_seconds or 60))))

        payload: Dict[str, Any] = {
            "status": "pending",
            "error": error_message[:4000],
            "next_retry_at": next_retry,
        }
        if effective_attempts != claimed_attempts:
            payload["attempts"] = effective_attempts

        lock = action.get("_lease_heartbeat_lock")
        if lock:
            with lock:
                rows = self._request(
                    "PATCH",
                    "actions",
                    params=self._action_fence_params(action),
                    payload=payload,
                )
        else:
            rows = self._request(
                "PATCH",
                "actions",
                params=self._action_fence_params(action),
                payload=payload,
            )
        if not rows:
            logger.warning("Skipped defer update for action %s because the lease fence no longer matched", action.get("id"))
        return bool(rows)

    def insert_action_log(
        self,
        action: Dict[str, Any],
        event_type: str,
        severity: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._request(
            "POST",
            "action_logs",
            payload={
                "action_id": action.get("id"),
                "domain_id": action.get("domain_id"),
                "inbox_id": action.get("inbox_id"),
                "customer_id": action.get("customer_id"),
                "event_type": event_type,
                "severity": severity,
                "message": message,
                "metadata": metadata or {},
            },
        )

    def get_domain(self, domain_id: str) -> Optional[Dict[str, Any]]:
        rows = self._request(
            "GET",
            "domains",
            params={"select": "*", "id": f"eq.{domain_id}", "limit": "1"},
        )
        return rows[0] if rows else None

    def update_domain(self, domain_id: str, fields: Dict[str, Any]) -> None:
        self._request(
            "PATCH",
            "domains",
            params={"id": f"eq.{domain_id}"},
            payload=fields,
        )

    def update_domain_if_active(self, domain_id: str, fields: Dict[str, Any]) -> None:
        """Update a domain only if its current status is NOT in a cancellation
        or terminal state. Used by progress writes to prevent the cancel/provision
        race where a worker overwrites queued_for_cancellation back to in_progress.
        """
        self._request(
            "PATCH",
            "domains",
            params={
                "id": f"eq.{domain_id}",
                "status": "not.in.(queued_for_cancellation,suspended,cancelled,expired)",
            },
            payload=fields,
        )

    def get_domain_inboxes(self, domain_id: str) -> List[Dict[str, Any]]:
        return self._request(
            "GET",
            "inboxes",
            params={
                "select": "*",
                "domain_id": f"eq.{domain_id}",
                "status": "in.(pending,provisioning,active)",
                "order": "created_at.asc",
            },
        )

    def get_domain_inboxes_all(self, domain_id: str) -> List[Dict[str, Any]]:
        return self._request(
            "GET",
            "inboxes",
            params={
                "select": "*",
                "domain_id": f"eq.{domain_id}",
                "order": "created_at.asc",
            },
        )

    def get_inboxes_by_ids(self, domain_id: str, inbox_ids: List[str]) -> List[Dict[str, Any]]:
        valid_ids = [str(v).strip() for v in inbox_ids if str(v or "").strip()]
        if not valid_ids:
            return []
        encoded = ",".join(valid_ids)
        return self._request(
            "GET",
            "inboxes",
            params={
                "select": "*",
                "domain_id": f"eq.{domain_id}",
                "id": f"in.({encoded})",
                "order": "created_at.asc",
            },
        )

    def insert_inboxes(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return []
        return self._request(
            "POST",
            "inboxes",
            payload=rows,
        )

    def update_inbox(self, inbox_id: str, fields: Dict[str, Any]) -> None:
        self._request(
            "PATCH",
            "inboxes",
            params={"id": f"eq.{inbox_id}"},
            payload=fields,
        )

    def update_inboxes_by_ids(self, inbox_ids: List[str], fields: Dict[str, Any]) -> None:
        valid_ids = [str(v).strip() for v in inbox_ids if str(v or "").strip()]
        if not valid_ids:
            return
        encoded = ",".join(valid_ids)
        self._request(
            "PATCH",
            "inboxes",
            params={"id": f"in.({encoded})"},
            payload=fields,
        )

    def get_admin_credential(self, provider: str) -> Optional[Dict[str, Any]]:
        rows = self._request(
            "GET",
            "admin_credentials",
            params={
                "select": "*",
                "provider": f"eq.{provider}",
                "active": "eq.true",
                "order": "usage_count.asc",
                "limit": "1",
            },
        )
        return rows[0] if rows else None

    def increment_admin_usage(self, admin_id: str, increment_by: int = 1) -> None:
        rows = self._request(
            "GET",
            "admin_credentials",
            params={"select": "id,usage_count", "id": f"eq.{admin_id}", "limit": "1"},
        )
        if not rows:
            return
        usage_count = int(rows[0].get("usage_count") or 0) + max(1, increment_by)
        self._request(
            "PATCH",
            "admin_credentials",
            params={"id": f"eq.{admin_id}"},
            payload={"usage_count": usage_count, "last_used": _to_iso(_utc_now())},
        )

    def assign_domain_admin(self, domain_id: str, admin_cred_id: str) -> None:
        self._request(
            "POST",
            "domain_admin_assignments",
            payload={
                "domain_id": domain_id,
                "admin_cred_id": admin_cred_id,
            },
        )

    def get_domain_tool_credentials_list(self, domain_id: str) -> List[Dict[str, Any]]:
        assignments = self._request(
            "GET",
            "domain_credentials",
            params={"select": "*", "domain_id": f"eq.{domain_id}"},
        )
        if not assignments:
            return []

        bundles: List[Dict[str, Any]] = []
        for assignment in assignments:
            credential_id = assignment.get("credential_id")
            if not credential_id:
                continue

            cred_rows = self._request(
                "GET",
                "sending_tool_credentials",
                params={"select": "*", "id": f"eq.{credential_id}", "limit": "1"},
            )
            if not cred_rows:
                continue
            credential = cred_rows[0]

            tool_id = credential.get("sending_tool_id")
            slug = None
            if tool_id:
                tool_rows = self._request(
                    "GET",
                    "sending_tools",
                    params={"select": "id,slug,name", "id": f"eq.{tool_id}", "limit": "1"},
                )
                if tool_rows:
                    slug = tool_rows[0].get("slug")

            bundles.append({"slug": slug, "credential": credential})

        return bundles

    def get_domain_tool_credentials(self, domain_id: str) -> Optional[Dict[str, Any]]:
        bundles = self.get_domain_tool_credentials_list(domain_id)
        return bundles[0] if bundles else None

    def get_mutation_request(self, request_id: str) -> Optional[Dict[str, Any]]:
        rows = self._request(
            "GET",
            "domain_mutation_requests",
            params={"select": "*", "id": f"eq.{request_id}", "limit": "1"},
        )
        return rows[0] if rows else None

    def update_mutation_request(self, request_id: str, fields: Dict[str, Any]) -> None:
        self._request(
            "PATCH",
            "domain_mutation_requests",
            params={"id": f"eq.{request_id}"},
            payload=fields,
        )

    def get_mutation_items(self, request_id: str) -> List[Dict[str, Any]]:
        return self._request(
            "GET",
            "domain_mutation_items",
            params={
                "select": "*",
                "request_id": f"eq.{request_id}",
                "order": "sort_order.asc",
            },
        )

    def update_mutation_item(self, item_id: str, fields: Dict[str, Any]) -> None:
        self._request(
            "PATCH",
            "domain_mutation_items",
            params={"id": f"eq.{item_id}"},
            payload=fields,
        )

    def insert_mutation_event(self, payload: Dict[str, Any]) -> None:
        self._request(
            "POST",
            "domain_mutation_events",
            payload=payload,
        )

    def upsert_inbox_email_alias(
        self,
        *,
        inbox_id: str,
        email: str,
        status: str,
        source: str = "mutation",
        provider_alias_id: Optional[str] = None,
    ) -> None:
        clean_email = str(email or "").strip().lower()
        if not clean_email:
            return

        existing = self._request(
            "GET",
            "inbox_email_aliases",
            params={"select": "id", "email": f"eq.{clean_email}", "limit": "1"},
        )
        payload = {
            "inbox_id": inbox_id,
            "email": clean_email,
            "source": source,
            "status": status,
            "provider_alias_id": provider_alias_id,
        }
        if existing:
            alias_id = str(existing[0].get("id") or "").strip()
            if alias_id:
                self._request(
                    "PATCH",
                    "inbox_email_aliases",
                    params={"id": f"eq.{alias_id}"},
                    payload=payload,
                )
                return

        self._request(
            "POST",
            "inbox_email_aliases",
            payload=payload,
        )

    def refresh_mutation_submission(self, submission_id: str) -> Optional[Dict[str, Any]]:
        rows = self._request(
            "GET",
            "domain_mutation_requests",
            params={
                "select": "id,status,started_at,completed_at,failed_at",
                "submission_id": f"eq.{submission_id}",
                "order": "requested_at.asc",
            },
        )
        if not rows:
            return None

        statuses = [str(row.get("status") or "").strip().lower() for row in rows]
        aggregate_status = "queued"
        if statuses and all(status == "completed" for status in statuses):
            aggregate_status = "completed"
        elif "processing" in statuses:
            aggregate_status = "processing"
        elif "queued" in statuses:
            aggregate_status = "queued"
        elif statuses and all(status == "cancelled" for status in statuses):
            aggregate_status = "cancelled"
        elif "failed" in statuses and "completed" in statuses:
            aggregate_status = "partially_completed"
        elif "failed" in statuses:
            aggregate_status = "failed"
        elif "needs_attention" in statuses:
            aggregate_status = "processing"

        started_values = [str(row.get("started_at") or "").strip() for row in rows if row.get("started_at")]
        completed_values = [str(row.get("completed_at") or "").strip() for row in rows if row.get("completed_at")]
        failed_values = [str(row.get("failed_at") or "").strip() for row in rows if row.get("failed_at")]

        payload: Dict[str, Any] = {
            "status": aggregate_status,
            "request_count": len(rows),
        }
        if started_values:
            payload["started_at"] = min(started_values)
        if aggregate_status == "completed" and completed_values:
            payload["completed_at"] = max(completed_values)
            payload["failed_at"] = None
            payload["last_error"] = None
        elif aggregate_status in {"failed", "partially_completed"} and failed_values:
            payload["failed_at"] = max(failed_values)

        updated_rows = self._request(
            "PATCH",
            "inbox_mutation_submissions",
            params={"id": f"eq.{submission_id}"},
            payload=payload,
        )
        return updated_rows[0] if updated_rows else None
