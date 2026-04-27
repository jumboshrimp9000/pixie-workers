import logging
import os
import socket
from typing import Any, Dict, List, Optional, Tuple

import dns.resolver
import requests


logger = logging.getLogger(__name__)


class DynadotClient:
    API_BASE = "https://api.dynadot.com/api3.json"

    def __init__(self, api_key: str, timeout_seconds: int = 30):
        self.api_key = (api_key or "").strip()
        self.timeout_seconds = timeout_seconds
        if not self.api_key:
            raise ValueError("DYNADOT_API_KEY is required")

    def register_domain(self, domain: str) -> Dict[str, Any]:
        params = {
            "key": self.api_key,
            "command": "register",
            "domain": domain,
            "duration": 1,
            "currency": "USD",
        }
        try:
            resp = requests.get(self.API_BASE, params=params, timeout=self.timeout_seconds)
            data = resp.json()
        except Exception as exc:
            return {"success": False, "error": f"Dynadot register request failed: {exc}"}

        register = data.get("RegisterResponse", {}) if isinstance(data, dict) else {}
        code = register.get("ResponseCode")
        error = str(register.get("Error") or register.get("Status") or "")
        if code == 0:
            return {"success": True}

        lower = error.lower()
        if "already registered" in lower or "already exists" in lower:
            return {"success": True, "already_registered": True}
        return {"success": False, "error": error or f"Dynadot register failed (code={code})"}

    def set_nameservers(self, domain: str, ns1: str, ns2: str) -> Dict[str, Any]:
        params = {
            "key": self.api_key,
            "command": "set_ns",
            "domain": domain,
            "ns0": ns1,
            "ns1": ns2,
        }
        try:
            resp = requests.get(self.API_BASE, params=params, timeout=self.timeout_seconds)
            data = resp.json()
        except Exception as exc:
            return {"success": False, "error": f"Dynadot set_ns request failed: {exc}"}

        ns_resp = data.get("SetNsResponse", {}) if isinstance(data, dict) else {}
        code = ns_resp.get("ResponseCode", data.get("ResponseCode"))
        if code == 0:
            return {"success": True}

        error = str(ns_resp.get("Error") or ns_resp.get("Status") or "")
        return {"success": False, "error": error or f"Dynadot set_ns failed (code={code})"}


class CloudflareClient:
    API_BASE = "https://api.cloudflare.com/client/v4"

    def __init__(
        self,
        api_token: str = "",
        account_id: str = "",
        global_api_key: str = "",
        global_email: str = "",
        timeout_seconds: int = 30,
    ):
        self.api_token = (api_token or "").strip()
        self.account_id = (account_id or "").strip()
        self.global_api_key = (global_api_key or "").strip()
        self.global_email = (global_email or "").strip()
        self.timeout_seconds = timeout_seconds
        self._can_token_auth = bool(self.api_token)
        self._can_global_auth = bool(self.global_api_key and self.global_email)

        if not self._can_token_auth and not self._can_global_auth:
            raise ValueError(
                "Cloudflare auth missing. Set CLOUDFLARE_API_TOKEN or set CLOUDFLARE_GLOBAL_KEY + CLOUDFLARE_EMAIL."
            )

        self.auth_mode = "token" if self._can_token_auth else "global"

        if not self.account_id:
            logger.warning(
                "CLOUDFLARE_ACCOUNT_ID is not set. Zone creation may fail unless the zone already exists."
            )

    def _headers_for_mode(self, mode: str) -> Dict[str, str]:
        if mode == "token":
            return {
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json",
            }
        return {
            "X-Auth-Email": self.global_email,
            "X-Auth-Key": self.global_api_key,
            "Content-Type": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        allow_fallback: bool = True,
    ) -> Dict[str, Any]:
        modes: List[str] = [self.auth_mode]
        if (
            allow_fallback
            and self.auth_mode == "token"
            and self._can_global_auth
            and "global" not in modes
        ):
            modes.append("global")

        last_response: Optional[requests.Response] = None
        last_payload: Dict[str, Any] = {}

        for index, mode in enumerate(modes):
            headers = self._headers_for_mode(mode)
            response = requests.request(
                method=method.upper(),
                url=f"{self.API_BASE}{path}",
                headers=headers,
                params=params,
                json=json_body,
                timeout=self.timeout_seconds,
            )
            try:
                payload: Dict[str, Any] = response.json()
            except Exception:
                payload = {}

            ok = bool(response.ok and payload.get("success", True))
            if ok:
                if index > 0:
                    logger.warning("Cloudflare request succeeded using global-key fallback auth.")
                return payload

            if self._is_auth_error(response, payload) and index < len(modes) - 1:
                logger.warning("Cloudflare token auth failed; retrying request with global key auth.")
                continue

            last_response = response
            last_payload = payload
            break

        message = self._extract_error_message(last_payload) or (
            last_response.text if last_response is not None else "Unknown Cloudflare error"
        )
        if last_response is not None and self._is_auth_error(last_response, last_payload):
            message = (
                f"{message}. Cloudflare authentication failed. "
                "Use a valid CLOUDFLARE_API_TOKEN, or set CLOUDFLARE_GLOBAL_KEY and CLOUDFLARE_EMAIL."
            )
        raise RuntimeError(f"Cloudflare API {method.upper()} {path} failed: {message}")

    def _is_auth_error(self, response: requests.Response, payload: Dict[str, Any]) -> bool:
        if response.status_code in (401, 403):
            return True
        message = self._extract_error_message(payload).lower()
        if "authentication error" in message:
            return True
        if "invalid request headers" in message:
            return True
        if "invalid access token" in message:
            return True
        return False

    def get_or_create_zone(self, domain: str) -> Tuple[str, bool]:
        zone = self.find_zone_by_name(domain)
        if zone:
            return zone["id"], False

        body: Dict[str, Any] = {
            "name": domain,
            "type": "full",
        }
        if self.account_id:
            body["account"] = {"id": self.account_id}

        data = self._request("POST", "/zones", json_body=body)
        zone_id = data.get("result", {}).get("id")
        if not zone_id:
            raise RuntimeError("Cloudflare create zone failed: missing zone id in response")
        return zone_id, True

    def find_zone_by_name(self, domain: str) -> Optional[Dict[str, Any]]:
        data = self._request("GET", "/zones", params={"name": domain})
        zones = data.get("result") or []
        return zones[0] if zones else None

    def get_zone(self, zone_id: str) -> Dict[str, Any]:
        data = self._request("GET", f"/zones/{zone_id}")
        return data.get("result") or {}

    def get_zone_nameservers(self, zone_id: str) -> Tuple[str, str]:
        zone = self.get_zone(zone_id)
        nameservers = zone.get("name_servers") or []
        if len(nameservers) < 2:
            raise RuntimeError("Cloudflare zone does not have 2 assigned nameservers yet")
        return nameservers[0], nameservers[1]

    def is_zone_active(self, zone_id: str) -> bool:
        zone = self.get_zone(zone_id)
        status = str(zone.get("status") or "").lower()
        if status == "active":
            return True
        if status != "pending":
            return False

        try:
            self._request("PUT", f"/zones/{zone_id}/activation_check", allow_fallback=False)
        except Exception:
            # Best-effort only. Cloudflare can rate-limit activation checks on free zones.
            pass

        domain = str(zone.get("name") or "").strip().lower()
        nameservers = [
            str(value or "").strip().lower()
            for value in (zone.get("name_servers") or [])
            if str(value or "").strip()
        ]
        if not domain or len(nameservers) < 2:
            return False

        return self._is_zone_authoritative(domain, nameservers)

    def _is_zone_authoritative(self, domain: str, nameservers: List[str]) -> bool:
        expected = {value.rstrip(".").lower() for value in nameservers if str(value or "").strip()}
        if len(expected) < 2:
            return False

        for nameserver in expected:
            resolver = dns.resolver.Resolver(configure=False)
            resolver.timeout = 3
            resolver.lifetime = 5
            resolver.nameservers = self._nameserver_ips(nameserver)
            if not resolver.nameservers:
                continue

            try:
                soa_answer = resolver.resolve(domain, "SOA")
                ns_answer = resolver.resolve(domain, "NS")
            except Exception:
                continue

            soa_primary = ""
            if soa_answer:
                soa_primary = str(getattr(soa_answer[0], "mname", "")).rstrip(".").lower()
            served = {str(record.target).rstrip(".").lower() for record in ns_answer}

            if soa_primary in expected and expected.issubset(served):
                return True

        return False

    def _nameserver_ips(self, nameserver: str) -> List[str]:
        host = str(nameserver or "").rstrip(".")
        if not host:
            return []

        ips: List[str] = []
        seen = set()
        try:
            for family, _, _, _, sockaddr in socket.getaddrinfo(host, 53, proto=socket.IPPROTO_UDP):
                ip = sockaddr[0]
                if family not in (socket.AF_INET, socket.AF_INET6):
                    continue
                if ip in seen:
                    continue
                seen.add(ip)
                ips.append(ip)
        except Exception:
            return []
        return ips

    def upsert_dns_records(self, zone_id: str, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        created = 0
        skipped = 0
        failed = 0
        deleted = 0
        errors: List[Dict[str, str]] = []

        for record in records:
            rec_type = str(record.get("type") or "").upper().strip()
            name = str(record.get("name") or "").strip()
            content = str(record.get("content") or "").strip()
            if not rec_type or not name or not content:
                skipped += 1
                continue

            body: Dict[str, Any] = {
                "type": rec_type,
                "name": name,
                "content": content,
                "ttl": int(record.get("ttl") or 3600),
            }
            if rec_type == "MX" and record.get("priority") is not None:
                body["priority"] = int(record["priority"])

            try:
                if bool(record.get("replace_existing")):
                    deleted += self._delete_existing_dns_records(zone_id, rec_type, name)
                self._request("POST", f"/zones/{zone_id}/dns_records", json_body=body)
                created += 1
                continue
            except Exception as exc:
                msg = str(exc)

            msg_lower = msg.lower()
            if "already exists" in msg_lower or "a record with those settings already exists" in msg_lower:
                skipped += 1
            else:
                failed += 1
                errors.append(
                    {
                        "type": rec_type,
                        "name": name,
                        "message": msg,
                    }
                )
                logger.warning("Cloudflare DNS record failed for %s %s: %s", rec_type, name, msg)

        return {"created": created, "skipped": skipped, "failed": failed, "deleted": deleted, "errors": errors[:20]}

    def _delete_existing_dns_records(self, zone_id: str, rec_type: str, name: str) -> int:
        deleted = 0
        for record_id in self._find_dns_record_ids(zone_id, rec_type, name):
            self._request("DELETE", f"/zones/{zone_id}/dns_records/{record_id}")
            deleted += 1
        return deleted

    def _find_dns_record_ids(self, zone_id: str, rec_type: str, name: str) -> List[str]:
        ids: List[str] = []
        seen: set[str] = set()
        for query_name in self._dns_name_query_candidates(zone_id, name):
            data = self._request(
                "GET",
                f"/zones/{zone_id}/dns_records",
                params={"type": rec_type, "name": query_name},
            )
            for row in data.get("result") or []:
                record_id = str(row.get("id") or "").strip()
                if not record_id or record_id in seen:
                    continue
                seen.add(record_id)
                ids.append(record_id)
        return ids

    def _dns_name_query_candidates(self, zone_id: str, name: str) -> List[str]:
        clean_name = str(name or "").strip().rstrip(".").lower()
        if not clean_name:
            return []

        zone_name = ""
        try:
            zone_name = str(self.get_zone(zone_id).get("name") or "").strip().rstrip(".").lower()
        except Exception:
            zone_name = ""

        candidates: List[str] = []
        if zone_name:
            if clean_name == "@":
                candidates.append(zone_name)
            elif clean_name == zone_name or clean_name.endswith(f".{zone_name}"):
                candidates.append(clean_name)
            else:
                candidates.append(f"{clean_name}.{zone_name}")
        candidates.append(clean_name)

        unique: List[str] = []
        for candidate in candidates:
            if candidate and candidate not in unique:
                unique.append(candidate)
        return unique

    @staticmethod
    def _extract_error_message(payload: Dict[str, Any]) -> str:
        errors = payload.get("errors") if isinstance(payload, dict) else None
        if isinstance(errors, list) and errors:
            first = errors[0]
            if isinstance(first, dict):
                return str(first.get("message") or "")
            return str(first)
        return ""


class PartnerHubClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://partnerhubapi.netstager.com/api",
        default_plan_id: str = "94c835bb-a675-4249-8fba-e95cdb2ca4ed",
        timeout_seconds: int = 30,
    ):
        self.api_key = (api_key or "").strip()
        self.base_url = base_url.rstrip("/")
        self.default_plan_id = default_plan_id
        self.timeout_seconds = timeout_seconds
        if not self.api_key:
            raise ValueError("PARTNERHUB_API_KEY (or GOOGLE_NETSTAGER_API_KEY) is required")

    @property
    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def create_order(
        self,
        domain: str,
        organization_name: str,
        users: List[Dict[str, Any]],
        plan_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "planId": plan_id or self.default_plan_id,
            "domain": domain,
            "organisationName": organization_name,
            "licenseUsers": users,
        }
        data, status_code, ok = self._request("POST", "/integration/orders", payload=payload)
        success = bool(data.get("success")) if isinstance(data, dict) else False
        if not ok or not success:
            raise RuntimeError(f"PartnerHub create order failed: {data}")
        return data

    def get_order_details(self, domain: str) -> Dict[str, Any]:
        data, status_code, ok = self._request("GET", "/integration/orders", params={"domain": domain})
        if not ok:
            raise RuntimeError(f"PartnerHub get order details failed: {data}")
        return data

    def get_order_by_id(self, order_id: str) -> Dict[str, Any]:
        data, status_code, ok = self._request("GET", f"/integration/orders/{order_id}")
        if not ok:
            raise RuntimeError(f"PartnerHub get order by id failed: {data}")
        return data

    def resolve_order_id_by_domain(self, domain: str) -> Optional[str]:
        clean_domain = str(domain or "").strip().lower()
        if not clean_domain:
            return None

        data, status_code, ok = self._request(
            "GET",
            "/integration/orders",
            params={"domain": clean_domain, "page": 1, "limit": 100},
        )
        if ok:
            order_id = self.extract_order_id(data, clean_domain)
            if order_id:
                return order_id
        elif status_code not in {404, 410}:
            raise RuntimeError(f"PartnerHub order lookup failed for {clean_domain}: status={status_code} response={data}")

        max_pages = max(1, int(os.getenv("PARTNERHUB_RESOLVE_MAX_PAGES", "20")))
        page_size = max(1, min(200, int(os.getenv("PARTNERHUB_RESOLVE_PAGE_SIZE", "100"))))
        for page in range(1, max_pages + 1):
            page_data, page_status, page_ok = self._request(
                "GET",
                "/integration/orders",
                params={"page": page, "limit": page_size},
            )
            if not page_ok:
                raise RuntimeError(
                    f"PartnerHub order list failed while resolving {clean_domain}: "
                    f"page={page} status={page_status} response={page_data}"
                )

            order_id = self.extract_order_id(page_data, clean_domain)
            if order_id:
                return order_id

            rows = self._extract_order_rows(page_data)
            if len(rows) < page_size:
                break

        return None

    def increase_license(
        self,
        order_id: str,
        number_of_licenses: int,
        plan_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "orderId": order_id,
            "numberOfLicenses": int(number_of_licenses),
        }
        if plan_id or self.default_plan_id:
            payload["planId"] = plan_id or self.default_plan_id

        data, status_code, ok = self._request(
            "POST",
            "/integration/orders/increase-license",
            payload=payload,
        )
        if not ok:
            raise RuntimeError(f"PartnerHub increase license failed: status={status_code} response={data}")
        return data

    def add_license_users(
        self,
        order_id: str,
        license_users: List[Dict[str, Any]],
        *,
        domain: Optional[str] = None,
        organization_name: Optional[str] = None,
        plan_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        users = [u for u in license_users if isinstance(u, dict)]
        payload: Dict[str, Any] = {
            "orderId": order_id,
            "licenceUser": users,
            "planId": plan_id or self.default_plan_id,
        }
        if domain:
            payload["domain"] = domain
        if organization_name:
            payload["organisationName"] = organization_name

        endpoints = [
            "/integration/order/amendment-order-licence-users",
            "/integration/orders/amendment-order-licence-users",
        ]
        attempted: List[Dict[str, Any]] = []
        for endpoint in endpoints:
            data, status_code, ok = self._request("POST", endpoint, payload=payload)
            attempted.append({"endpoint": endpoint, "status": status_code, "ok": ok})
            if ok:
                if isinstance(data, dict):
                    data["_endpoint"] = endpoint
                return data
            message = str(data).lower()
            if "same email exists in order licence users" in message or "same email exists in order license users" in message:
                return {
                    "success": True,
                    "already_exists": True,
                    "_endpoint": endpoint,
                    "response": data,
                }
            if endpoint.endswith("/order/amendment-order-licence-users") and (
                status_code in {404, 405} or "cannot post" in message or "not found" in message
            ):
                continue
            raise RuntimeError(
                "PartnerHub add license users failed: "
                f"endpoint={endpoint} status={status_code} response={data}"
            )

        raise RuntimeError(f"PartnerHub add license users failed. Attempts={attempted}")

    def sync_order_users(
        self,
        domain: str,
        organization_name: str,
        users: List[Dict[str, Any]],
        plan_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "planId": plan_id or self.default_plan_id,
            "domain": domain,
            "organisationName": organization_name,
            "licenseUsers": users,
        }

        attempted: List[Dict[str, Any]] = []
        for method in ("PUT", "PATCH", "POST"):
            data, status_code, ok = self._request(method, "/integration/orders", payload=payload)
            success = bool(data.get("success")) if isinstance(data, dict) else False
            attempted.append({"method": method, "status": status_code, "success": success, "response": data})

            if ok and success:
                if method != "POST":
                    logger.info("PartnerHub user sync succeeded via %s /integration/orders", method)
                return {"method": method, "response": data, "attempts": attempted}

            message = str(data).lower()
            maybe_wrong_method = status_code in (404, 405) or "method not allowed" in message
            if maybe_wrong_method and method != "POST":
                continue

            if method == "POST" and "domain already exists" in message:
                # Order exists but API may not support mutation for this tenant.
                break

        raise RuntimeError(f"PartnerHub user sync failed for {domain}. Attempts={attempted}")

    def update_profile_photos(
        self,
        updates: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        endpoint = os.getenv("PARTNERHUB_PROFILE_PHOTO_ENDPOINT", "").strip()
        if not endpoint:
            raise RuntimeError(
                "PARTNERHUB_PROFILE_PHOTO_ENDPOINT is not configured. "
                "Set it to the PartnerHub path that updates Google user profile photos."
            )
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        payload: Dict[str, Any] = {"updates": updates}
        data, status_code, ok = self._request("POST", endpoint, payload=payload)
        success = bool(data.get("success")) if isinstance(data, dict) else False
        if not ok or not success:
            raise RuntimeError(f"PartnerHub profile photo update failed: status={status_code} response={data}")
        return data

    @staticmethod
    def _extract_order_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        rows: List[Dict[str, Any]] = []
        data = payload.get("data") or {}
        if isinstance(data, dict):
            order = data.get("order")
            if isinstance(order, dict):
                rows.append(order)
            listed_rows = data.get("rows")
            if isinstance(listed_rows, list):
                rows.extend([row for row in listed_rows if isinstance(row, dict)])
        if payload.get("id"):
            rows.append(payload)
        return rows

    def extract_order_id(self, payload: Dict[str, Any], domain: Optional[str] = None) -> Optional[str]:
        rows = self._extract_order_rows(payload)
        if not rows:
            return None
        clean_domain = str(domain or "").strip().lower()

        def row_matches(candidate: Dict[str, Any]) -> bool:
            if not clean_domain:
                return True
            candidate_domain = str(candidate.get("domain") or "").strip().lower()
            return bool(candidate_domain and candidate_domain == clean_domain)

        for row in rows:
            if row.get("id") and row_matches(row):
                return str(row.get("id"))
        return None

    def extract_dns_records(self, payload: Dict[str, Any], domain: str) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []

        candidates: List[Dict[str, Any]] = []

        def walk(value: Any) -> None:
            if isinstance(value, dict):
                normalized = self._normalize_dns_record(value, domain)
                if normalized:
                    candidates.append(normalized)
                for child in value.values():
                    walk(child)
            elif isinstance(value, list):
                for item in value:
                    walk(item)

        walk(payload)

        seen = set()
        unique: List[Dict[str, Any]] = []
        for rec in candidates:
            key = (rec.get("type"), rec.get("name"), rec.get("content"), rec.get("priority"))
            if key in seen:
                continue
            seen.add(key)
            unique.append(rec)
        return unique

    @staticmethod
    def _normalize_dns_record(candidate: Dict[str, Any], domain: str) -> Optional[Dict[str, Any]]:
        type_candidates = [
            candidate.get("type"),
            candidate.get("recordType"),
            candidate.get("dnsType"),
        ]
        name_candidates = [
            candidate.get("name"),
            candidate.get("host"),
            candidate.get("recordName"),
            candidate.get("label"),
        ]
        content_candidates = [
            candidate.get("content"),
            candidate.get("value"),
            candidate.get("recordValue"),
            candidate.get("target"),
            candidate.get("data"),
            candidate.get("mailExchange"),
            candidate.get("text"),
        ]
        priority = candidate.get("priority")
        ttl = candidate.get("ttl")

        record_type = next((str(x).upper().strip() for x in type_candidates if x), "")
        name = next((str(x).strip() for x in name_candidates if x), "")
        content = next((str(x).strip() for x in content_candidates if x), "")

        if record_type not in {"A", "AAAA", "CNAME", "TXT", "MX", "SRV"}:
            return None
        if not content:
            return None

        if not name or name == domain:
            name = "@"
        if name.endswith(f".{domain}"):
            name = name[: -(len(domain) + 1)]
            if not name:
                name = "@"

        record: Dict[str, Any] = {
            "type": record_type,
            "name": name,
            "content": content,
            "ttl": int(ttl) if ttl else 3600,
        }
        if record_type == "MX" and priority is not None:
            try:
                record["priority"] = int(priority)
            except Exception:
                record["priority"] = 1
        return record

    @staticmethod
    def _json_or_error(resp: requests.Response) -> Dict[str, Any]:
        try:
            return resp.json()
        except Exception:
            return {"success": False, "error": resp.text}

    def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], int, bool]:
        response = requests.request(
            method=method.upper(),
            url=f"{self.base_url}{path}",
            headers=self._headers,
            params=params,
            json=payload,
            timeout=self.timeout_seconds,
        )
        data = self._json_or_error(response)
        ok = bool(response.ok)
        return data, response.status_code, ok
