#!/usr/bin/env python3
"""
Build non-secret reconciliation and migration-readiness artifacts for the
Jack/ProfitPath Microsoft admin pool.

This script never selects or writes admin passwords. It reads the prior
pasted-admin overlap report, cross-checks live SimpleInboxes admin/domain
state, and emits CSV/JSON planning artifacts under ./logs.
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PROJECTS = ROOT.parents[1]
LOG_DIR = ROOT / "logs"
OVERLAP_REPORT = LOG_DIR / "pasted-897-admins-overlap-check.csv"
ENV_FILE = PROJECTS / "AP" / ".env"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def psql_tsv(sql: str) -> list[dict[str, str]]:
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        raise SystemExit("SUPABASE_DB_URL is required; set it or keep AP/.env available.")

    result = subprocess.run(
        ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-P", "footer=off", "-A", "-F", "\t", "-c", sql],
        check=True,
        capture_output=True,
        text=True,
    )
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        return []
    reader = csv.DictReader(lines, delimiter="\t")
    return [dict(row) for row in reader]


def read_overlap_report() -> list[dict[str, str]]:
    with OVERLAP_REPORT.open(newline="", encoding="utf-8-sig") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def boolish(value: object) -> bool:
    return str(value or "").strip().lower() in {"true", "t", "1", "yes", "y"}


def write_csv(path: Path, rows: list[dict[str, object]], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    load_env_file(ENV_FILE)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    pasted_rows = read_overlap_report()
    pasted_admins = [row["admin"].strip().lower() for row in pasted_rows if row.get("admin")]
    pasted_set = set(pasted_admins)
    jack_threshold_set = {
        row["admin"].strip().lower()
        for row in pasted_rows
        if boolish(row.get("in_jack_threshold_227"))
    }

    admin_sql = """
with assigned as (
  select
    daa.admin_cred_id,
    count(*) as domain_assignments,
    count(*) filter (where coalesce(d.status, '') not in ('cancelled', 'deleted')) as active_domain_assignments
  from domain_admin_assignments daa
  join domains d on d.id = daa.domain_id
  group by daa.admin_cred_id
)
select
  ac.id::text as admin_id,
  lower(ac.email) as email,
  ac.provider,
  ac.status,
  ac.active::text as active,
  ac.usage_count::text as usage_count,
  coalesce(ac.max_usage::text, '') as max_usage,
  coalesce(assigned.domain_assignments, 0)::text as domain_assignments,
  coalesce(assigned.active_domain_assignments, 0)::text as active_domain_assignments,
  case
    when ac.lock_type is not null
      or ac.locked_by_action_id is not null
      or (ac.lock_expires_at is not null and ac.lock_expires_at > now())
    then 'locked'
    else 'clear'
  end as lock_state
from admin_credentials ac
left join assigned on assigned.admin_cred_id = ac.id
where ac.provider = 'microsoft'
order by lower(ac.email);
"""
    domain_sql = """
select
  lower(ac.email) as admin,
  d.domain,
  d.id::text as domain_id,
  coalesce(d.status, '') as domain_status,
  coalesce(d.interim_status, '') as interim_status,
  coalesce(d.customer_id::text, '') as customer_id,
  coalesce(d.workspace_id::text, '') as workspace_id,
  count(i.id) filter (where coalesce(i.status, '') = 'active')::text as active_inboxes,
  count(i.id)::text as total_inboxes
from admin_credentials ac
join domain_admin_assignments daa on daa.admin_cred_id = ac.id
join domains d on d.id = daa.domain_id
left join inboxes i on i.domain_id = d.id
where ac.provider = 'microsoft'
group by ac.email, d.domain, d.id, d.status, d.interim_status, d.customer_id, d.workspace_id
order by lower(ac.email), d.domain;
"""

    live_admins = psql_tsv(admin_sql)
    live_domains = psql_tsv(domain_sql)
    live_by_email = {row["email"]: row for row in live_admins}
    domains_by_admin: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in live_domains:
        domains_by_admin[row["admin"]].append(row)

    reconciliation_rows: list[dict[str, object]] = []
    missing_import_rows: list[dict[str, object]] = []
    for overlap in sorted(pasted_rows, key=lambda item: item["admin"].lower()):
        admin = overlap["admin"].strip().lower()
        live = live_by_email.get(admin)
        in_si = live is not None
        status = live.get("status", "") if live else ""
        active = live.get("active", "") if live else ""
        domain_assignments = int(live.get("domain_assignments", "0")) if live else 0
        usage_count = int(live.get("usage_count", "0")) if live else 0
        lock_state = live.get("lock_state", "") if live else ""
        if not in_si:
            bucket = "missing_from_simpleinboxes"
            missing_import_rows.append(
                {
                    "email": admin,
                    "password": "",
                    "provider": "microsoft",
                    "status": "Active",
                    "active": "true",
                    "max_usage": "",
                }
            )
        elif status == "threshold exceeded":
            bucket = "threshold_exceeded_do_not_use"
        elif status == "Active" and active == "true" and lock_state == "clear" and domain_assignments == 0 and usage_count == 0:
            bucket = "active_unused_candidate"
        elif status == "Active":
            bucket = "active_used_or_needs_capacity_check"
        else:
            bucket = "other_status_review"

        reconciliation_rows.append(
            {
                "admin": admin,
                "in_simpleinboxes_live": str(in_si),
                "live_status": status,
                "live_active": active,
                "usage_count": usage_count if live else "",
                "max_usage": live.get("max_usage", "") if live else "",
                "domain_assignments": domain_assignments if live else "",
                "active_domain_assignments": live.get("active_domain_assignments", "") if live else "",
                "lock_state": lock_state,
                "in_jack_threshold_227": str(admin in jack_threshold_set),
                "in_airtable_csv_exact": overlap.get("in_airtable_csv_exact", ""),
                "in_airtable_csv_tenant": overlap.get("in_airtable_csv_tenant", ""),
                "readiness_bucket": bucket,
            }
        )

    candidate_rows: list[dict[str, object]] = []
    for row in reconciliation_rows:
        if row["readiness_bucket"] == "active_unused_candidate":
            candidate_rows.append(
                {
                    "admin": row["admin"],
                    "candidate_type": "si_active_unused",
                    "in_simpleinboxes_live": "True",
                    "status": row["live_status"],
                    "usage_count": row["usage_count"],
                    "domain_assignments": row["domain_assignments"],
                    "lock_state": row["lock_state"],
                    "validation_required": "message_trace_threshold_check; capacity_check; lock_check",
                    "notes": "Preferred pool for first migration dry runs; already in SI and currently unused.",
                }
            )
        elif row["readiness_bucket"] == "missing_from_simpleinboxes":
            candidate_rows.append(
                {
                    "admin": row["admin"],
                    "candidate_type": "missing_requires_import",
                    "in_simpleinboxes_live": "False",
                    "status": "",
                    "usage_count": "",
                    "domain_assignments": "",
                    "lock_state": "",
                    "validation_required": "credential_import; message_trace_threshold_check; capacity_check",
                    "notes": "Only usable after password-bearing import and provider validation.",
                }
            )

    active_unused_candidates = [
        row for row in candidate_rows if row["candidate_type"] == "si_active_unused"
    ]
    active_unused_candidates.sort(key=lambda row: str(row["admin"]))

    threshold_domain_rows: list[dict[str, object]] = []
    threshold_domains = []
    for admin in sorted(jack_threshold_set):
        assigned_domains = domains_by_admin.get(admin, [])
        source_domain_count = len(assigned_domains)
        for domain in sorted(assigned_domains, key=lambda row: row["domain"]):
            threshold_domains.append((admin, source_domain_count, domain))

    threshold_domains.sort(key=lambda item: (item[1], item[0], item[2]["domain"]))
    for idx, (admin, source_domain_count, domain) in enumerate(threshold_domains):
        destination = active_unused_candidates[idx] if idx < len(active_unused_candidates) else None
        priority = "1_one_domain_source_first" if source_domain_count == 1 else "2_multi_domain_source_second"
        threshold_domain_rows.append(
            {
                "priority_group": priority,
                "source_admin": admin,
                "source_admin_domain_count": source_domain_count,
                "domain": domain["domain"],
                "domain_status": domain["domain_status"],
                "interim_status": domain["interim_status"],
                "active_inboxes": domain["active_inboxes"],
                "total_inboxes": domain["total_inboxes"],
                "proposed_destination_admin": destination["admin"] if destination else "",
                "destination_candidate_type": destination["candidate_type"] if destination else "no_candidate_available",
                "preflight_required": "validate destination clean; verify no lock; connect domain; recreate 99 room mailboxes; reset passwords; unblock; UPN/domain check; DKIM enabled; Instantly reupload/settings/tags/counts",
                "post_move_guard": "Do not quarantine/delete old tenant artifacts until provider-side inbox count/settings/tag validation passes.",
            }
        )

    summary = {
        "generated_at_utc": ts,
        "pasted_admins_total": len(pasted_set),
        "live_microsoft_admins_total": len(live_by_email),
        "pasted_already_in_simpleinboxes_live": sum(1 for admin in pasted_set if admin in live_by_email),
        "pasted_missing_from_simpleinboxes_live": sum(1 for admin in pasted_set if admin not in live_by_email),
        "pasted_threshold_exceeded_live": sum(
            1 for admin in pasted_set if live_by_email.get(admin, {}).get("status") == "threshold exceeded"
        ),
        "pasted_active_live": sum(
            1 for admin in pasted_set if live_by_email.get(admin, {}).get("status") == "Active"
        ),
        "active_unused_si_candidates": len(active_unused_candidates),
        "missing_requires_password_import": len(missing_import_rows),
        "jack_threshold_admins": len(jack_threshold_set),
        "jack_threshold_domains_currently_assigned": len(threshold_domain_rows),
        "candidate_shortfall_for_threshold_domains": max(0, len(threshold_domain_rows) - len(active_unused_candidates)),
    }

    reconciliation_path = LOG_DIR / f"admin-pool-reconciliation-{ts}.csv"
    candidate_path = LOG_DIR / f"admin-pool-candidate-destinations-{ts}.csv"
    migration_path = LOG_DIR / f"jack-threshold-domain-migration-plan-{ts}.csv"
    missing_template_path = LOG_DIR / f"admin-pool-missing-import-template-{ts}.tsv"
    summary_path = LOG_DIR / f"admin-pool-summary-{ts}.json"

    write_csv(
        reconciliation_path,
        reconciliation_rows,
        [
            "admin",
            "in_simpleinboxes_live",
            "live_status",
            "live_active",
            "usage_count",
            "max_usage",
            "domain_assignments",
            "active_domain_assignments",
            "lock_state",
            "in_jack_threshold_227",
            "in_airtable_csv_exact",
            "in_airtable_csv_tenant",
            "readiness_bucket",
        ],
    )
    write_csv(
        candidate_path,
        candidate_rows,
        [
            "admin",
            "candidate_type",
            "in_simpleinboxes_live",
            "status",
            "usage_count",
            "domain_assignments",
            "lock_state",
            "validation_required",
            "notes",
        ],
    )
    write_csv(
        migration_path,
        threshold_domain_rows,
        [
            "priority_group",
            "source_admin",
            "source_admin_domain_count",
            "domain",
            "domain_status",
            "interim_status",
            "active_inboxes",
            "total_inboxes",
            "proposed_destination_admin",
            "destination_candidate_type",
            "preflight_required",
            "post_move_guard",
        ],
    )
    with missing_template_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["email", "password", "provider", "status", "active", "max_usage"],
            delimiter="\t",
        )
        writer.writeheader()
        writer.writerows(missing_import_rows)

    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(
        {
            "summary": summary,
            "paths": {
                "reconciliation": str(reconciliation_path),
                "candidates": str(candidate_path),
                "migration_plan": str(migration_path),
                "missing_import_template": str(missing_template_path),
                "summary": str(summary_path),
            },
        },
        indent=2,
        sort_keys=True,
    ))


if __name__ == "__main__":
    main()
