#!/usr/bin/env python3
import argparse
import os
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import requests

from app.workers.google_admin_playwright import GoogleAdminPlaywrightClient
from app.workers.onepassword_client import OnePasswordCliClient


def out(message: str) -> None:
    print(message, flush=True)


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and value and key not in os.environ:
            os.environ[key] = value


def get_supabase_base() -> Tuple[str, str]:
    url = str(os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    key = str(os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
    return f"{url}/rest/v1", key


def fetch_inbox(email: str) -> Dict[str, str]:
    base, key = get_supabase_base()
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}
    response = requests.get(
        f"{base}/inboxes",
        headers=headers,
        params={
            "select": "email,password,onepassword_item_id,otp_secret,is_admin,profile_pic_url",
            "email": f"eq.{email}",
            "limit": "1",
        },
        timeout=30,
    )
    response.raise_for_status()
    rows = response.json()
    if not rows:
        raise RuntimeError(f"No inbox row found for {email}")
    row = rows[0]
    return {k: str(v or "") for k, v in row.items()}


def patch_profile_pic_url(email: str, profile_pic_url: str) -> None:
    base, key = get_supabase_base()
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    response = requests.patch(
        f"{base}/inboxes",
        headers=headers,
        params={"email": f"eq.{email}"},
        json={"profile_pic_url": profile_pic_url},
        timeout=30,
    )
    response.raise_for_status()


def resolve_onepassword_client() -> Optional[OnePasswordCliClient]:
    try:
        return OnePasswordCliClient.from_env()
    except Exception:
        return None


def do_upload(
    client: GoogleAdminPlaywrightClient,
    target_email: str,
    photo_url: str,
    *,
    update_supabase: bool,
) -> None:
    out(f"STEP::upload_profile_photo::{target_email}")
    result = client.upload_profile_photos([(target_email, photo_url)])
    out(f"UPLOAD_RESULT::{result}")
    uploaded = int(result.get("uploaded") or 0)
    failed = int(result.get("failed") or 0)
    if uploaded > 0 and failed == 0:
        if update_supabase:
            patch_profile_pic_url(target_email, photo_url)
            out("STEP_OK::supabase_profile_pic_url_updated")
        out("STEP_OK::upload_profile_photo")
    else:
        out("IM_NOT_SURE_WHAT_TO_DO::Profile upload did not report full success")


def run_initial_flow(
    client: GoogleAdminPlaywrightClient,
    *,
    admin_email: str,
    admin_password: str,
    target_email: str,
    photo_url: str,
    onepassword: Optional[OnePasswordCliClient],
) -> None:
    out(f"STEP::login_admin::{admin_email}")
    client._login_google_account(admin_email, admin_password, onepassword=onepassword)
    out(f"STEP_OK::login_admin::url={client._safe_url()}")

    out("STEP::open_admin_users_page")
    client._open_users_page()
    out(f"STEP_OK::open_admin_users_page::url={client._safe_url()}")

    do_upload(client, target_email, photo_url, update_supabase=True)


def command_loop(
    client: GoogleAdminPlaywrightClient,
    *,
    default_target_email: str,
    default_photo_url: str,
) -> None:
    out("BROWSER_STAYS_OPEN::commands=help,status,retry,upload,goto,exit")
    while True:
        line = sys.stdin.readline()
        if not line:
            time.sleep(0.2)
            continue
        cmd = line.strip()
        if not cmd:
            continue

        parts = cmd.split(maxsplit=2)
        action = parts[0].lower()

        try:
            if action in {"help", "?"}:
                out("HELP::status | retry | upload <email> <photo_url> | goto <url> | exit")
            elif action == "status":
                out(f"STATUS::url={client._safe_url()}")
            elif action == "goto" and len(parts) >= 2:
                url = parts[1].strip()
                client._goto(url)
                out(f"STEP_OK::goto::{client._safe_url()}")
            elif action == "retry":
                do_upload(
                    client,
                    default_target_email,
                    default_photo_url,
                    update_supabase=True,
                )
            elif action == "upload":
                if len(parts) < 3:
                    out("IM_NOT_SURE_WHAT_TO_DO::Use upload <email> <photo_url>")
                    continue
                email = parts[1].strip().lower()
                url = parts[2].strip()
                do_upload(client, email, url, update_supabase=True)
            elif action in {"exit", "quit"}:
                out("EXITING::closing_browser")
                return
            else:
                out("IM_NOT_SURE_WHAT_TO_DO::Unknown command. Use help")
        except Exception as exc:
            out(f"IM_NOT_SURE_WHAT_TO_DO::{exc}")
            try:
                out(f"DEBUG_CAPTURE::{client._capture_debug_state('profile_photo_debug_loop_error')}")
            except Exception:
                pass


def parse_args() -> argparse.Namespace:
    default_env = Path(__file__).resolve().parents[3] / "AP" / ".env"
    parser = argparse.ArgumentParser(description="Interactive Google profile photo upload runner")
    parser.add_argument("--env-file", default=str(default_env), help="Path to env file (default: AP/.env)")
    parser.add_argument("--admin-email", required=True, help="Admin inbox email used to login")
    parser.add_argument("--target-email", required=True, help="Target inbox email for profile photo")
    parser.add_argument("--photo-url", required=True, help="Image URL (supports Drive share links)")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--slow-mo-ms", type=int, default=120, help="Playwright slow_mo in ms")
    parser.add_argument("--timeout-seconds", type=int, default=45, help="Per-action timeout in seconds")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_env_file(Path(args.env_file))

    admin = fetch_inbox(args.admin_email)
    admin_password = str(admin.get("password") or "").strip()
    if not admin_password:
        raise RuntimeError(f"Missing password for admin inbox {args.admin_email}")

    onepassword = resolve_onepassword_client()

    client = GoogleAdminPlaywrightClient(
        headless=bool(args.headless),
        slow_mo_ms=max(0, int(args.slow_mo_ms or 0)),
        timeout_seconds=max(10, int(args.timeout_seconds or 45)),
    )
    client.__enter__()
    out("BROWSER_OPEN::started")

    try:
        try:
            run_initial_flow(
                client,
                admin_email=args.admin_email,
                admin_password=admin_password,
                target_email=args.target_email,
                photo_url=args.photo_url,
                onepassword=onepassword,
            )
        except Exception as exc:
            out(f"IM_NOT_SURE_WHAT_TO_DO::{exc}")
            try:
                out(f"DEBUG_URL::{client._safe_url()}")
                out(f"DEBUG_CAPTURE::{client._capture_debug_state('profile_photo_initial_error')}")
            except Exception:
                pass

        command_loop(
            client,
            default_target_email=args.target_email,
            default_photo_url=args.photo_url,
        )
    finally:
        client.__exit__(None, None, None)


if __name__ == "__main__":
    main()
