"""
One-off backfill: copy existing Zap Audit rows from Airtable into Supabase's
`zap_runs` table. Run this once, after creating the table with zap_runs.sql
and before switching the Zapier Logger zap over to Supabase.

Required environment variables (do not hardcode these):
  AIRTABLE_ZAP_AUDIT_TOKEN   Airtable PAT with read access to base appq10XQm3AKQYyYr
  SUPABASE_URL               e.g. https://ykayxonkdzgvzrqktcyt.supabase.co
  SUPABASE_SERVICE_ROLE_KEY  the secret/service_role key (bypasses RLS for inserts)

Usage:
  AIRTABLE_ZAP_AUDIT_TOKEN=pat... \
  SUPABASE_URL=https://ykayxonkdzgvzrqktcyt.supabase.co \
  SUPABASE_SERVICE_ROLE_KEY=sb_secret_... \
  python supabase/migrate_zap_runs_from_airtable.py
"""

import os
import sys
import requests

AIRTABLE_BASE_ID = "appq10XQm3AKQYyYr"
AIRTABLE_TABLE_ID = "tbleFE2RpNXq1s3S4"


def fetch_airtable_runs(token: str) -> list:
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"pageSize": 100}
    runs = []
    offset = None
    while True:
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        for rec in data.get("records", []):
            f = rec.get("fields", {})
            status = f.get("Status", "")
            if isinstance(status, dict):
                status = status.get("name", "")
            runs.append({
                "run_id":        f.get("Run ID") or rec["id"],
                "zap_name":      f.get("Zap Name", ""),
                "zap_id":        f.get("Zap ID", ""),
                "status":        str(status).lower(),
                "ts":            f.get("Timestamp"),
                "step":          f.get("Step (if error)", ""),
                "error_message": f.get("Error Message", ""),
                "duration_ms":   f.get("Duration (ms)", 0) or 0,
                "task_count":    f.get("Task Count", 0) or 0,
                "logger_source": f.get("Logger Source", ""),
            })
        offset = data.get("offset")
        if not offset:
            break
    return runs


def insert_into_supabase(supabase_url: str, service_key: str, rows: list) -> None:
    url = f"{supabase_url}/rest/v1/zap_runs"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        resp = requests.post(url, headers=headers, json=batch, timeout=30)
        resp.raise_for_status()
        print(f"Inserted {i + len(batch)}/{len(rows)}")


def main() -> None:
    airtable_token = os.environ.get("AIRTABLE_ZAP_AUDIT_TOKEN")
    supabase_url = os.environ.get("SUPABASE_URL")
    service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    missing = [
        name for name, val in [
            ("AIRTABLE_ZAP_AUDIT_TOKEN", airtable_token),
            ("SUPABASE_URL", supabase_url),
            ("SUPABASE_SERVICE_ROLE_KEY", service_key),
        ] if not val
    ]
    if missing:
        print(f"Missing required env var(s): {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    print("Fetching existing runs from Airtable...")
    runs = fetch_airtable_runs(airtable_token)
    print(f"Fetched {len(runs)} rows from Airtable.")

    if not runs:
        print("Nothing to migrate.")
        return

    print("Inserting into Supabase...")
    insert_into_supabase(supabase_url, service_key, runs)
    print("Done.")


if __name__ == "__main__":
    main()
