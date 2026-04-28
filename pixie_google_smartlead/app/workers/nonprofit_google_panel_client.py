import json
from typing import Any, Dict, List, Optional

import requests


class NonprofitGooglePanelClient:
    def __init__(self, apps_script_url: str, *, timeout_seconds: int = 45):
        self.apps_script_url = str(apps_script_url or "").strip()
        self.timeout_seconds = max(5, int(timeout_seconds))
        if not self.apps_script_url:
            raise ValueError("apps_script_url is required")
        self.session = requests.Session()

    def get_domain_txt(self, domain: str) -> Dict[str, Any]:
        return self._post_json({"action": "domain", "domain": domain, "verify": False})

    def verify_domain_via_api(self, domain: str) -> Dict[str, Any]:
        return self._post_json({"action": "domain", "domain": domain, "verify": True})

    def batch_create_users(self, users: List[Dict[str, Any]], skip_photos: bool = False) -> Dict[str, Any]:
        return self._post_json(
            {
                "action": "batchUsers",
                "users": list(users or []),
                "skipPhotos": bool(skip_photos),
            }
        )

    def update_user(
        self,
        old_email: str,
        new_username: str,
        new_first_name: str,
        new_last_name: str,
        new_password: str,
        new_photo_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "action": "updateUser",
            "oldEmail": old_email,
            "newUsername": new_username,
            "newFirstName": new_first_name,
            "newLastName": new_last_name,
            "newPassword": new_password,
        }
        if str(new_photo_url or "").strip():
            payload["newPhotoUrl"] = str(new_photo_url).strip()
        return self._post_json(payload)

    def delete_user(self, email: str, permanent: bool = True) -> Dict[str, Any]:
        return self._post_json({"action": "deleteUser", "email": email, "permanent": bool(permanent)})

    def delete_domain(self, domain: str) -> Dict[str, Any]:
        clean_domain = str(domain or "").strip().lower()
        if not clean_domain:
            raise ValueError("domain is required")

        result = self._post_json({"action": "deleteDomain", "domain": clean_domain})
        if self._looks_like_unsupported_action(result):
            result = self._post_json({"action": "removeDomain", "domain": clean_domain})
        return result

    def check_status(self, email: str) -> Dict[str, Any]:
        return self._post_json({"action": "checkStatus", "email": email})

    def _post_json(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = self.session.post(
            self.apps_script_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            allow_redirects=False,
            timeout=self.timeout_seconds,
        )

        if response.status_code in {301, 302, 303, 307, 308}:
            redirect_url = str(response.headers.get("Location") or response.headers.get("location") or "").strip()
            if not redirect_url:
                raise RuntimeError(f"Apps Script redirect missing Location header for payload={payload}")
            redirect_response = self.session.get(redirect_url, timeout=self.timeout_seconds)
            return self._parse_json(redirect_response, payload)

        return self._parse_json(response, payload)

    def _parse_json(self, response: requests.Response, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            data = response.json()
        except Exception as exc:
            preview = str(response.text or "")[:500]
            raise RuntimeError(
                f"Apps Script returned non-JSON response ({response.status_code}) for payload={payload}: {preview}"
            ) from exc

        if isinstance(data, dict):
            return data
        raise RuntimeError(f"Apps Script returned unexpected JSON type for payload={payload}: {type(data).__name__}")

    @staticmethod
    def _looks_like_unsupported_action(result: Dict[str, Any]) -> bool:
        if not isinstance(result, dict):
            return False
        if result.get("success") is not False:
            return False
        text = " ".join(
            str(result.get(key) or "")
            for key in ("error", "message", "details", "action")
        ).lower()
        return any(token in text for token in ("unknown action", "unsupported action", "invalid action"))
