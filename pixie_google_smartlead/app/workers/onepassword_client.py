import json
import os
import subprocess
from typing import Any, Dict, List, Optional
from urllib.parse import quote


class OnePasswordCliClient:
    def __init__(
        self,
        service_account_token: str,
        vault: str,
        *,
        timeout_seconds: int = 45,
        title_prefix: str = "",
    ):
        self.service_account_token = (service_account_token or "").strip()
        self.vault = (vault or "").strip()
        self.timeout_seconds = timeout_seconds
        self.title_prefix = (title_prefix or "").strip()
        if not self.service_account_token:
            raise ValueError("OP_SERVICE_ACCOUNT_TOKEN is required")
        if not self.vault:
            raise ValueError("ONEPASSWORD_VAULT is required")

    @classmethod
    def from_env(cls) -> "OnePasswordCliClient":
        return cls(
            service_account_token=os.getenv("OP_SERVICE_ACCOUNT_TOKEN", ""),
            vault=os.getenv("ONEPASSWORD_VAULT", ""),
            timeout_seconds=max(10, int(os.getenv("ONEPASSWORD_OP_TIMEOUT_SECONDS", "45"))),
            title_prefix=os.getenv("GOOGLE_ONEPASSWORD_ITEM_PREFIX", ""),
        )

    def create_or_update_google_login(
        self,
        *,
        email: str,
        password: str,
        otp_secret: str,
        username: Optional[str] = None,
    ) -> Dict[str, Any]:
        clean_email = str(email or "").strip().lower()
        if not clean_email:
            raise ValueError("email is required")
        clean_password = str(password or "").strip()
        if not clean_password:
            raise ValueError(f"password is required for {clean_email}")
        clean_secret = self._normalize_secret(otp_secret)
        if not clean_secret:
            raise ValueError(f"otp_secret is required for {clean_email}")

        item_title = self._item_title(clean_email)
        totp_uri = self._totp_uri(clean_email, clean_secret)
        login_username = str(username or clean_email).strip() or clean_email

        existing = self.find_google_login_item(clean_email)
        if existing:
            item_id = str(existing.get("id") or "")
            if not item_id:
                raise RuntimeError(f"1Password item id missing for existing title {item_title}")
            self._run_op(
                [
                    "item",
                    "edit",
                    item_id,
                    "--vault",
                    self.vault,
                    f"--title={item_title}",
                    f"username={login_username}",
                    f"password={clean_password}",
                    f"totp[otp]={totp_uri}",
                ]
            )
            return {"id": item_id, "title": item_title, "created": False}

        created = self._run_op_json(
            [
                "item",
                "create",
                "--category=Login",
                f"--title={item_title}",
                "--vault",
                self.vault,
                f"username={login_username}",
                f"password={clean_password}",
                f"totp[otp]={totp_uri}",
                "--format",
                "json",
            ]
        )
        created_id = str(created.get("id") or "")
        if not created_id:
            raise RuntimeError(f"Failed to create 1Password item for {clean_email}: missing id")
        return {"id": created_id, "title": item_title, "created": True}

    def get_totp(self, item_id: str) -> str:
        clean_item_id = str(item_id or "").strip()
        if not clean_item_id:
            raise ValueError("item_id is required")
        value = self._run_op(["item", "get", clean_item_id, "--vault", self.vault, "--otp"])
        code = str(value or "").strip()
        if not code:
            raise RuntimeError(f"1Password OTP code empty for item {clean_item_id}")
        return code

    def update_google_login_identity_by_item_id(
        self,
        *,
        item_id: str,
        email: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        previous_name: Optional[str] = None,
        previous_username: Optional[str] = None,
        previous_email: Optional[str] = None,
        current_name: Optional[str] = None,
        current_username: Optional[str] = None,
        current_email: Optional[str] = None,
    ) -> Dict[str, Any]:
        clean_item_id = str(item_id or "").strip()
        clean_email = str(email or "").strip().lower()
        if not clean_item_id:
            raise ValueError("item_id is required")
        if not clean_email:
            raise ValueError("email is required")

        login_username = str(username or clean_email).strip() or clean_email
        item_title = self._item_title(clean_email)

        args: List[str] = [
            "item",
            "edit",
            clean_item_id,
            "--vault",
            self.vault,
            f"--title={item_title}",
            f"username={login_username}",
        ]
        clean_password = str(password or "").strip()
        if clean_password:
            args.append(f"password={clean_password}")

        notes = self._identity_notes(
            previous_name=previous_name,
            previous_username=previous_username,
            previous_email=previous_email,
            current_name=current_name,
            current_username=current_username or login_username,
            current_email=current_email or clean_email,
        )
        if notes:
            args.append(f"notesPlain={notes}")

        self._run_op(args)
        return {"id": clean_item_id, "title": item_title, "username": login_username}

    def find_google_login_item(self, email: str) -> Optional[Dict[str, Any]]:
        clean_email = str(email or "").strip().lower()
        if not clean_email:
            return None
        candidates = [title.lower() for title in self._candidate_item_titles(clean_email)]
        rows = self._run_op_json(
            [
                "item",
                "list",
                "--vault",
                self.vault,
                "--categories",
                "Login",
                "--format",
                "json",
            ]
        )
        if not isinstance(rows, list):
            return None
        for row in rows:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title") or "").strip().lower()
            if title in candidates:
                return row
        return None

    def _find_item_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        rows = self._run_op_json(
            [
                "item",
                "list",
                "--vault",
                self.vault,
                "--categories",
                "Login",
                "--format",
                "json",
            ]
        )
        if not isinstance(rows, list):
            return None
        target = title.strip().lower()
        for row in rows:
            if not isinstance(row, dict):
                continue
            candidate = str(row.get("title") or "").strip().lower()
            if candidate == target:
                return row
        return None

    def _run_op_json(self, args: List[str]) -> Any:
        raw = self._run_op(args)
        try:
            return json.loads(raw)
        except Exception as exc:
            raise RuntimeError(f"Failed to parse 1Password JSON response: {exc}") from exc

    def _run_op(self, args: List[str]) -> str:
        env = dict(os.environ)
        env["OP_SERVICE_ACCOUNT_TOKEN"] = self.service_account_token
        proc = subprocess.run(
            ["op", *args],
            env=env,
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds,
        )
        if proc.returncode != 0:
            stderr = str(proc.stderr or "").strip()
            stdout = str(proc.stdout or "").strip()
            message = stderr or stdout or "unknown op cli error"
            raise RuntimeError(f"1Password CLI call failed: {message}")
        return str(proc.stdout or "").strip()

    def _item_title(self, email: str) -> str:
        clean = str(email or "").strip().lower()
        safe_email = clean.replace("@", "-")
        if self.title_prefix:
            return f"{self.title_prefix}-{safe_email}"
        return safe_email

    def _candidate_item_titles(self, email: str) -> List[str]:
        clean = str(email or "").strip().lower()
        if not clean:
            return []
        current = self._item_title(clean)
        legacy_at = clean.replace("@", "-at-")
        titles = [current, f"google-{legacy_at}", legacy_at]
        if self.title_prefix:
            titles.append(f"{self.title_prefix}-{legacy_at}")
        # preserve order, drop duplicates/empties
        seen = set()
        ordered: List[str] = []
        for t in titles:
            tt = str(t or "").strip()
            if not tt:
                continue
            key = tt.lower()
            if key in seen:
                continue
            seen.add(key)
            ordered.append(tt)
        return ordered

    @staticmethod
    def _identity_notes(
        *,
        previous_name: Optional[str],
        previous_username: Optional[str],
        previous_email: Optional[str],
        current_name: Optional[str],
        current_username: Optional[str],
        current_email: Optional[str],
    ) -> str:
        previous_lines = [
            f"- Previous Name: {value}"
            for value in [str(previous_name or "").strip()]
            if value
        ]
        previous_lines += [
            f"- Previous Username: {value}"
            for value in [str(previous_username or "").strip()]
            if value
        ]
        previous_lines += [
            f"- Previous Email: {value}"
            for value in [str(previous_email or "").strip()]
            if value
        ]

        current_lines = [
            f"- Current Name: {value}"
            for value in [str(current_name or "").strip()]
            if value
        ]
        current_lines += [
            f"- Current Username: {value}"
            for value in [str(current_username or "").strip()]
            if value
        ]
        current_lines += [
            f"- Current Email: {value}"
            for value in [str(current_email or "").strip()]
            if value
        ]

        sections: List[str] = []
        if previous_lines:
            sections.append("Previous identity details:\n\n" + "\n".join(previous_lines))
        if current_lines:
            sections.append("Current identity details:\n\n" + "\n".join(current_lines))
        return "\n\n".join(sections)

    @staticmethod
    def _normalize_secret(secret: str) -> str:
        return str(secret or "").strip().replace(" ", "")

    @staticmethod
    def _totp_uri(email: str, secret: str) -> str:
        issuer = "Google Workspace"
        label = f"{issuer}:{email}"
        return (
            f"otpauth://totp/{quote(label)}"
            f"?secret={quote(secret)}&issuer={quote(issuer)}"
        )
