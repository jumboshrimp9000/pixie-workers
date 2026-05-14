#!/usr/bin/env python3
"""
Dry-run/import helper for adding Microsoft admin credentials to SimpleInboxes.

Input format:
  - CSV or TSV with headers: email,password[,provider,status,active,max_usage]
  - Headerless two-column TSV/CSV is also accepted as email,password.

Safety:
  - Defaults to dry-run.
  - Never writes passwords to the dry-run report.
  - Apply mode inserts missing Microsoft admins only unless explicitly asked to
    update existing passwords.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

try:
    import psycopg
    from psycopg.rows import dict_row

    DB_DRIVER = "psycopg"
except Exception:  # pragma: no cover - fallback for hosts with psycopg2 only
    import psycopg2
    import psycopg2.extras

    DB_DRIVER = "psycopg2"


ROOT = Path(__file__).resolve().parent
PROJECTS = ROOT.parents[1]
LOG_DIR = ROOT / "logs"
ENV_FILE = PROJECTS / "AP" / ".env"
EMAIL_RE = re.compile(r"^admin@[A-Za-z0-9.-]+\.onmicrosoft\.com$", re.I)
CONFIRM_TEXT = "IMPORT_MICROSOFT_ADMINS"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def sniff_delimiter(sample: str, path: Path) -> str:
    if path.suffix.lower() == ".tsv":
        return "\t"
    try:
        return csv.Sniffer().sniff(sample, delimiters=",\t;").delimiter
    except csv.Error:
        return "\t" if "\t" in sample else ","


def normalize_bool(value: str, default: bool = True) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return default
    return text in {"1", "true", "t", "yes", "y", "active"}


def read_input(path: Path) -> list[dict[str, object]]:
    sample = path.read_text(encoding="utf-8-sig", errors="ignore")[:4096]
    delimiter = sniff_delimiter(sample, path)
    rows: list[dict[str, object]] = []
    with path.open(newline="", encoding="utf-8-sig", errors="ignore") as handle:
        preview = handle.readline()
        handle.seek(0)
        has_header = bool(re.search(r"\b(email|admin|password|pass)\b", preview, re.I))
        if has_header:
            reader = csv.DictReader(handle, delimiter=delimiter)
            for idx, raw in enumerate(reader, start=2):
                email = str(raw.get("email") or raw.get("admin") or raw.get("AdminEmail") or "").strip().lower()
                password = str(raw.get("password") or raw.get("pass") or raw.get("Admin Password") or "").strip()
                rows.append(
                    {
                        "row_number": idx,
                        "email": email,
                        "password": password,
                        "provider": str(raw.get("provider") or "microsoft").strip().lower(),
                        "status": str(raw.get("status") or "Active").strip() or "Active",
                        "active": normalize_bool(str(raw.get("active") or ""), default=True),
                        "max_usage": str(raw.get("max_usage") or "").strip(),
                    }
                )
        else:
            reader = csv.reader(handle, delimiter=delimiter)
            for idx, raw in enumerate(reader, start=1):
                if not raw or all(not str(cell).strip() for cell in raw):
                    continue
                email = str(raw[0] if len(raw) > 0 else "").strip().lower()
                password = str(raw[1] if len(raw) > 1 else "").strip()
                rows.append(
                    {
                        "row_number": idx,
                        "email": email,
                        "password": password,
                        "provider": "microsoft",
                        "status": "Active",
                        "active": True,
                        "max_usage": "",
                    }
                )
    return rows


def connect():
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        raise SystemExit("SUPABASE_DB_URL is required; set it or keep AP/.env available.")
    if DB_DRIVER == "psycopg":
        return psycopg.connect(db_url, row_factory=dict_row)
    return psycopg2.connect(db_url)


def fetch_existing(conn) -> dict[str, dict[str, object]]:
    cursor_kwargs = {} if DB_DRIVER == "psycopg" else {"cursor_factory": psycopg2.extras.RealDictCursor}
    with conn.cursor(**cursor_kwargs) as cur:
        cur.execute(
            """
            select lower(email) as email, id::text as id, status, active, usage_count
            from admin_credentials
            where provider = 'microsoft'
            order by lower(email)
            """
        )
        return {str(row["email"]).lower(): dict(row) for row in cur.fetchall()}


def classify(rows: list[dict[str, object]], existing: dict[str, dict[str, object]]) -> tuple[list[dict[str, object]], Counter]:
    seen = Counter(str(row["email"]) for row in rows)
    report: list[dict[str, object]] = []
    counts: Counter = Counter()
    for row in rows:
        email = str(row["email"])
        password = str(row["password"])
        provider = str(row["provider"] or "microsoft").lower()
        status = str(row["status"] or "Active")
        problems: list[str] = []
        if not EMAIL_RE.match(email):
            problems.append("invalid_admin_onmicrosoft_email")
        if provider != "microsoft":
            problems.append("provider_must_be_microsoft")
        if not password:
            problems.append("missing_password")
        if status not in {"Active", "threshold exceeded"}:
            problems.append("invalid_status")
        if seen[email] > 1:
            problems.append("duplicate_in_input")

        existing_row = existing.get(email)
        if problems:
            action = "blocked_" + ";".join(problems)
        elif existing_row:
            action = "already_exists_skip"
        else:
            action = "would_insert"
        counts[action] += 1
        report.append(
            {
                "row_number": row["row_number"],
                "email": email,
                "provider": provider,
                "requested_status": status,
                "requested_active": str(bool(row["active"])),
                "password_present": str(bool(password)),
                "exists_in_simpleinboxes": str(bool(existing_row)),
                "existing_status": existing_row.get("status", "") if existing_row else "",
                "existing_active": existing_row.get("active", "") if existing_row else "",
                "action": action,
            }
        )
    return report, counts


def parse_max_usage(value: object):
    text = str(value or "").strip()
    return int(text) if text else None


def apply_import(conn, rows: list[dict[str, object]], update_existing_passwords: bool) -> Counter:
    counts: Counter = Counter()
    cursor_kwargs = {} if DB_DRIVER == "psycopg" else {"cursor_factory": psycopg2.extras.RealDictCursor}
    with conn.cursor(**cursor_kwargs) as cur:
        for row in rows:
            email = str(row["email"]).lower()
            password = str(row["password"])
            provider = str(row["provider"] or "microsoft").lower()
            status = str(row["status"] or "Active")
            if not EMAIL_RE.match(email) or provider != "microsoft" or not password or status not in {"Active", "threshold exceeded"}:
                counts["blocked_invalid_or_missing_password"] += 1
                continue
            cur.execute(
                "select id, status from admin_credentials where provider = 'microsoft' and lower(email) = %s limit 1",
                (email,),
            )
            existing = cur.fetchone()
            if existing:
                if update_existing_passwords:
                    cur.execute(
                        """
                        update admin_credentials
                        set password = %s, status = %s, active = %s, max_usage = %s, updated_at = now()
                        where id = %s
                        """,
                        (password, status, bool(row["active"]), parse_max_usage(row["max_usage"]), existing["id"]),
                    )
                    counts["updated_existing"] += 1
                else:
                    counts["already_exists_skip"] += 1
                continue
            cur.execute(
                """
                insert into admin_credentials (provider, email, password, status, active, max_usage, extra_fields)
                values ('microsoft', %s, %s, %s, %s, %s, '{}'::jsonb)
                """,
                (email, password, status, bool(row["active"]), parse_max_usage(row["max_usage"])),
            )
            counts["inserted"] += 1
    conn.commit()
    return counts


def write_report(report_rows: list[dict[str, object]], counts: Counter, applied: bool) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = LOG_DIR / f"admin-pool-import-{'applied' if applied else 'dry-run'}-{ts}.csv"
    fields = [
        "row_number",
        "email",
        "provider",
        "requested_status",
        "requested_active",
        "password_present",
        "exists_in_simpleinboxes",
        "existing_status",
        "existing_active",
        "action",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(report_rows)
    summary_path = path.with_suffix(".summary.json")
    summary_path.write_text(json.dumps(dict(counts), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Dry-run/import Microsoft admin credentials without logging passwords.")
    parser.add_argument("--input", required=True, help="CSV/TSV containing email,password rows.")
    parser.add_argument("--apply", action="store_true", help="Actually insert missing admins. Dry-run is default.")
    parser.add_argument("--confirm", default="", help=f"Required for --apply: {CONFIRM_TEXT}")
    parser.add_argument("--update-existing-passwords", action="store_true", help="Also update passwords/status for existing admins.")
    args = parser.parse_args()

    load_env_file(ENV_FILE)
    input_path = Path(args.input).expanduser().resolve()
    rows = read_input(input_path)
    with connect() as conn:
        existing = fetch_existing(conn)
        report, counts = classify(rows, existing)
        if args.apply:
            if args.confirm != CONFIRM_TEXT:
                raise SystemExit(f"--apply requires --confirm {CONFIRM_TEXT}")
            applied_counts = apply_import(conn, rows, args.update_existing_passwords)
            existing_after = fetch_existing(conn)
            report, counts = classify(rows, existing_after)
            counts.update({f"apply_{key}": value for key, value in applied_counts.items()})
        report_path = write_report(report, counts, applied=args.apply)

    print(json.dumps({"input_rows": len(rows), "applied": bool(args.apply), "counts": dict(counts), "report": str(report_path)}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
