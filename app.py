import re
import io
import json
import time
import datetime
import requests
import openpyxl
import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import date, timedelta
from collections import Counter
from openpyxl.styles import Font, PatternFill, Alignment
from task_store import (
    load_tasks, add_task, update_task, delete_task, reset_recurring_tasks
)
import inbox_scanner

# ── Helpers: secrets ──────────────────────────────────────────────────────────
def _secret(key, default=""):
    try:
        return st.secrets[key]
    except Exception:
        return default

# ── Airtable Config ───────────────────────────────────────────────────────────
TOKEN = _secret("AIRTABLE_TOKEN")
AT_HEADERS = {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}
BASE_IDS = ["appbXFzZnhij88tnQ", "appXT2xJZ1zgll4fG"]
TARGET = re.compile(r"^\+1\d{10}$")
PLACEHOLDER_PATTERNS = [
    re.compile(r"\{[^}]+\}"),
    re.compile(r"\[[A-Z][^\]]+\]"),
    re.compile(r"<[A-Z][^>]+>"),
    re.compile(r"\{\{[^}]+\}\}"),
]

# ── Zapier Config ─────────────────────────────────────────────────────────────
ZAPIER_GQL_URL = "https://zapier.com/api/reporting/graphql"
ZAP_FAILURE_RATE_THRESHOLD = 10
ZAP_VOLUME_SPIKE_THRESHOLD  = 3.0
ZAP_VOLUME_DROP_THRESHOLD   = 0.2

# ═════════════════════════════════════════════════════════════════════════════
# AIRTABLE HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def get_base_name(base_id):
    r = requests.get("https://api.airtable.com/v0/meta/bases", headers=AT_HEADERS)
    r.raise_for_status()
    for b in r.json().get("bases", []):
        if b["id"] == base_id:
            return b["name"]
    return base_id

def categorize_phone(value):
    if not value or not str(value).strip():
        return "Empty"
    v = str(value).strip()
    digits = re.sub(r"\D", "", v)
    if TARGET.match(v):
        return "OK"
    if len(digits) == 11 and digits.startswith("1"):
        return "Has digits but wrong format"
    if len(digits) == 10:
        return "Missing country code (+1)"
    if len(digits) > 11:
        return "Too many digits"
    if len(digits) < 10:
        return "Too few digits"
    return "Non-standard format"

def fix_phone_number(value):
    digits = re.sub(r"\D", "", str(value).strip())
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None

def has_placeholder(text):
    return any(p.search(text) for p in PLACEHOLDER_PATTERNS)

def extract_tokens(content):
    found = []
    for p in PLACEHOLDER_PATTERNS:
        found.extend(p.findall(str(content)))
    return ", ".join(sorted(set(found))) if found else ""

def categorize_message(content):
    if not content or not str(content).strip():
        return "Empty"
    if len(str(content).strip()) < 20:
        return "Too short"
    if has_placeholder(str(content)):
        return "Unfilled placeholder"
    return "OK"

def patch_phone_records(base_id, rows):
    url = f"https://api.airtable.com/v0/{base_id}/Contact%20List"
    success, errors = 0, []
    for i in range(0, len(rows), 10):
        batch = rows[i:i + 10]
        payload = {
            "records": [
                {"id": r["record_id"], "fields": {"Contact Cell": r["Fixed Value"]}}
                for r in batch
            ]
        }
        try:
            resp = requests.patch(url, headers=AT_HEADERS, json=payload)
            resp.raise_for_status()
            success += len(batch)
        except Exception:
            errors.extend([r["record_id"] for r in batch])
        time.sleep(0.22)
    return success, errors

def revert_phone_records(base_id, revert_rows):
    url = f"https://api.airtable.com/v0/{base_id}/Contact%20List"
    success, errors = 0, []
    for i in range(0, len(revert_rows), 10):
        batch = revert_rows[i:i + 10]
        payload = {
            "records": [
                {"id": r["record_id"], "fields": {"Contact Cell": r["original_value"]}}
                for r in batch
            ]
        }
        try:
            resp = requests.patch(url, headers=AT_HEADERS, json=payload)
            resp.raise_for_status()
            success += len(batch)
        except Exception:
            errors.extend([r["record_id"] for r in batch])
        time.sleep(0.22)
    return success, errors

def fetch_records(base_id, table, fields, filter_formula=None):
    records, offset = [], None
    url = f"https://api.airtable.com/v0/{base_id}/{requests.utils.quote(table)}"
    while True:
        params = {"pageSize": 100, "fields[]": fields}
        if offset:
            params["offset"] = offset
        if filter_formula:
            params["filterByFormula"] = filter_formula
        r = requests.get(url, headers=AT_HEADERS, params=params)
        r.raise_for_status()
        data = r.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return records

def run_phone_audit(base_id, base_name):
    records = fetch_records(base_id, "Contact List",
                            ["Contact Cell", "Contact Full Name:", "Funeral Home Name"])
    rows = []
    for rec in records:
        f = rec.get("fields", {})
        val = f.get("Contact Cell", "")
        name = f.get("Contact Full Name:", "(unknown)")
        fh_raw = f.get("Funeral Home Name", [])
        fh = fh_raw[0] if isinstance(fh_raw, list) and fh_raw else str(fh_raw) if fh_raw else "(unknown)"
        cat = categorize_phone(val)
        rows.append({
            "Base": base_name,
            "Funeral Home": fh,
            "Contact Full Name": name,
            "Record ID": rec["id"],
            "Current Value": str(val) if val else "(empty)",
            "Issue": cat,
        })
    return pd.DataFrame(rows)

def run_message_audit(base_id, base_name):
    records = fetch_records(
        base_id, "Messages",
        ["Direction", "Message Content", "Message Type",
         "Contact Full Name: (from Contact Cell)",
         "Funeral Home: (from Contact Cell)"],
        filter_formula='FIND("outbound", LOWER({Direction})) > 0'
    )
    rows = []
    for rec in records:
        f = rec.get("fields", {})
        content = f.get("Message Content", "")
        name_raw = f.get("Contact Full Name: (from Contact Cell)", "")
        name = name_raw[0] if isinstance(name_raw, list) and name_raw else str(name_raw) if name_raw else "(unknown)"
        fh_raw = f.get("Funeral Home: (from Contact Cell)", [])
        fh = fh_raw[0] if isinstance(fh_raw, list) and fh_raw else str(fh_raw) if fh_raw else "(unknown)"
        cat = categorize_message(content)
        rows.append({
            "Base": base_name,
            "Funeral Home": fh,
            "Contact Full Name": name,
            "Record ID": rec["id"],
            "Direction": f.get("Direction", ""),
            "Message Type": f.get("Message Type", "(unknown)"),
            "Issue": cat,
            "Content (first 200 chars)": str(content)[:200] if content else "(empty)",
        })
    return pd.DataFrame(rows)

def build_excel(results_dict):
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    HEADER_FILL = PatternFill("solid", start_color="1a2b4a")
    HEADER_FONT = Font(bold=True, color="FFFFFF")
    ISSUE_COLORS = {
        "Has digits but wrong format": "DDEBF7",
        "Missing country code (+1)": "FFF2CC",
        "Too many digits": "FCE4D6",
        "Too few digits": "FCE4D6",
        "Non-standard format": "EAD1DC",
        "Empty": "E2EFDA",
        "Unfilled placeholder": "FFF2CC",
        "Too short": "FCE4D6",
    }
    for sheet_name, df in results_dict.items():
        safe = sheet_name[:31]
        ws = wb.create_sheet(safe)
        for c, col in enumerate(df.columns, 1):
            cell = ws.cell(1, c, col)
            cell.font = HEADER_FONT
            cell.fill = HEADER_FILL
        for r, row in enumerate(df.itertuples(index=False), 2):
            issue = row.Issue if hasattr(row, "Issue") else ""
            for c, val in enumerate(row, 1):
                cell = ws.cell(r, c, val)
                if issue in ISSUE_COLORS:
                    cell.fill = PatternFill("solid", start_color=ISSUE_COLORS[issue])
        for c, col in enumerate(df.columns, 1):
            max_len = max(len(str(col)), df.iloc[:, c-1].astype(str).str.len().max() if len(df) else 0)
            ws.column_dimensions[openpyxl.utils.get_column_letter(c)].width = min(max_len + 4, 50)
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = f"A1:{openpyxl.utils.get_column_letter(len(df.columns))}1"
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf

# ═════════════════════════════════════════════════════════════════════════════
# ZAPIER HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _zap_headers(session_token, csrf_token):
    return {
        "Cookie": f"zapsession={session_token}; csrftoken={csrf_token}",
        "X-CSRFToken": csrf_token,
        "Content-Type": "application/json",
        "Referer": "https://zapier.com/app/history",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

def fetch_zap_runs_for_range(session_token, csrf_token, account_id, start_date_str, end_date_str):
    hdrs = _zap_headers(session_token, csrf_token)
    runs = []
    offset = 0
    limit = 500

    while True:
        payload = {
            "query": """
                query GetRuns($accountId: ID!, $limit: Int!, $offset: Int!) {
                    zapRuns(accountId: $accountId, limit: $limit, offset: $offset) {
                        pageInfo { hasNextPage }
                        edges {
                            id
                            status
                            startTime
                            zap { id title }
                        }
                    }
                }
            """,
            "variables": {
                "accountId": str(account_id),
                "limit": limit,
                "offset": offset,
            },
        }
        r = requests.post(ZAPIER_GQL_URL, headers=hdrs, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        if "errors" in data:
            raise ValueError(f"Zapier API error: {data['errors']}")

        edges = data["data"]["zapRuns"]["edges"]
        has_next = data["data"]["zapRuns"]["pageInfo"]["hasNextPage"]
        past_range = False

        for run in edges:
            run_date = run["startTime"][:10]
            if start_date_str <= run_date <= end_date_str:
                runs.append(run)
            elif run_date < start_date_str:
                past_range = True
                break

        if past_range or not has_next:
            break

        offset += limit
        time.sleep(0.2)

    return runs

def build_zap_summaries(runs):
    zap_map = {}
    for run in runs:
        zap_id = run["zap"]["id"]
        title = run["zap"]["title"]
        if zap_id not in zap_map:
            zap_map[zap_id] = {
                "Zap Title": title,
                "Total Runs": 0,
                "Success": 0,
                "Errors": 0,
                "Halted": 0,
                "Filtered": 0,
                "Throttled": 0,
                "Other": 0,
                "_zap_id": zap_id,
            }
        z = zap_map[zap_id]
        z["Total Runs"] += 1
        status = (run.get("status") or "").lower()
        if status == "success":
            z["Success"] += 1
        elif status == "error":
            z["Errors"] += 1
        elif status == "halted":
            z["Halted"] += 1
        elif status == "filtered":
            z["Filtered"] += 1
        elif status == "throttled":
            z["Throttled"] += 1
        else:
            z["Other"] += 1

    rows = []
    for z in zap_map.values():
        t = z["Total Runs"]
        rows.append({
            **z,
            "Error Rate": f"{round(z['Errors']/t*100, 1)}%" if t else "0%",
            "Success Rate": f"{round(z['Success']/t*100, 1)}%" if t else "0%",
            "_error_rate_num": round(z["Errors"] / t * 100, 1) if t else 0,
            "_success_rate_num": round(z["Success"] / t * 100, 1) if t else 0,
        })
    return rows

def detect_zap_flags(summaries):
    flags = []
    for z in summaries:
        title = z["Zap Title"]
        total = z["Total Runs"]
        errors = z["Errors"]
        halted = z["Halted"]
        filtered = z["Filtered"]
        error_rate = z["_error_rate_num"]
        success_rate = z["_success_rate_num"]

        if errors >= 3:
            flags.append({
                "Severity": "🔴 Critical",
                "Type": "Repeated Failures",
                "Zap": title,
                "Detail": f"{errors} errors today ({error_rate}% error rate).",
            })
        elif error_rate >= ZAP_FAILURE_RATE_THRESHOLD and errors > 0:
            flags.append({
                "Severity": "⚠️ Warning",
                "Type": "High Error Rate",
                "Zap": title,
                "Detail": f"{error_rate}% error rate ({errors}/{total} runs).",
            })

        if total >= 5 and success_rate < 50 and error_rate < ZAP_FAILURE_RATE_THRESHOLD:
            flags.append({
                "Severity": "⚠️ Warning",
                "Type": "Low Success Rate",
                "Zap": title,
                "Detail": f"Only {success_rate}% success rate.",
            })

        if halted > 0:
            flags.append({
                "Severity": "⚠️ Warning",
                "Type": "Halted Runs",
                "Zap": title,
                "Detail": f"{halted} run(s) halted — check filter or path logic.",
            })

        if total > 10 and filtered == total:
            flags.append({
                "Severity": "⚠️ Warning",
                "Type": "All Filtered",
                "Zap": title,
                "Detail": f"All {total} runs were filtered — trigger may be too broad.",
            })

    return flags

# ═════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Aftercare Texting Audit — Parting Pro",
    layout="wide",
    page_icon="https://partingpro.com/wp-content/uploads/2024/07/partingpro-logo.png"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

#MainMenu, footer { visibility: hidden; }

/* Hide Streamlit toolbar (deploy/share/settings) but keep sidebar toggle */
[data-testid="stToolbar"] { visibility: hidden !important; }
[data-testid="stDecoration"] { display: none !important; }
[data-testid="stStatusWidget"] { visibility: hidden !important; }
header[data-testid="stHeader"] {
    background: transparent !important;
    border-bottom: none !important;
    box-shadow: none !important;
}

.stApp { background: #f0f2f7; }

/* ── Hero ── */
.hero {
    background: linear-gradient(135deg, #1a2b4a 0%, #243860 60%, #2e4a7a 100%);
    border-radius: 16px;
    padding: 48px 56px;
    margin-bottom: 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: relative;
    overflow: hidden;
    box-shadow: 0 8px 32px rgba(26,43,74,0.18);
}
.hero::before {
    content: '';
    position: absolute;
    top: -60px; right: -60px;
    width: 280px; height: 280px;
    background: rgba(255,255,255,0.04);
    border-radius: 50%;
}
.hero::after {
    content: '';
    position: absolute;
    bottom: -80px; right: 120px;
    width: 200px; height: 200px;
    background: rgba(255,255,255,0.03);
    border-radius: 50%;
}
.hero-left { z-index: 1; }
.hero-logo { height: 36px; margin-bottom: 20px; filter: brightness(0) invert(1); }
.hero-title {
    font-size: 30px;
    font-weight: 700;
    color: #ffffff;
    margin: 0 0 8px 0;
    line-height: 1.2;
    letter-spacing: -0.5px;
}
.hero-subtitle {
    font-size: 15px;
    color: rgba(255,255,255,0.65);
    margin: 0;
    font-weight: 400;
}
.hero-badge {
    background: rgba(255,255,255,0.1);
    border: 1px solid rgba(255,255,255,0.2);
    border-radius: 8px;
    padding: 8px 16px;
    color: rgba(255,255,255,0.85);
    font-size: 12px;
    font-weight: 500;
    z-index: 1;
    backdrop-filter: blur(8px);
}

/* ── Cards ── */
.card {
    background: white;
    border-radius: 12px;
    padding: 24px;
    border: 1px solid #e4e7ef;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
    margin-bottom: 16px;
}
.card-title {
    font-size: 15px;
    font-weight: 600;
    color: #1a2b4a;
    margin-bottom: 16px;
    display: flex;
    align-items: center;
    gap: 8px;
}

/* ── Metric Cards ── */
.metrics-row { display: flex; gap: 16px; margin-bottom: 20px; flex-wrap: wrap; }
.metric {
    flex: 1;
    min-width: 120px;
    background: white;
    border-radius: 12px;
    padding: 20px 24px;
    border: 1px solid #e4e7ef;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04);
}
.metric .m-label {
    font-size: 11px;
    font-weight: 600;
    color: #4a5568;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 8px;
}
.metric .m-value {
    font-size: 36px;
    font-weight: 700;
    color: #1a2b4a;
    line-height: 1;
}
.metric .m-sub {
    font-size: 12px;
    color: #4a5568;
    margin-top: 4px;
}
.metric.green .m-value { color: #1a9e5c; }
.metric.red .m-value { color: #e05252; }
.metric.blue .m-value { color: #3b7de8; }

/* ── Section Headers ── */
.section-wrap {
    background: white;
    border-radius: 16px;
    padding: 28px 32px;
    border: 1px solid #e4e7ef;
    box-shadow: 0 1px 6px rgba(0,0,0,0.04);
    margin-bottom: 24px;
}
.section-head {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 20px;
    padding-bottom: 16px;
    border-bottom: 1px solid #f0f2f7;
}
.section-icon {
    width: 40px; height: 40px;
    background: #eef2ff;
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 20px;
}
.section-head-text h3 {
    font-size: 17px;
    font-weight: 650;
    color: #1a2b4a;
    margin: 0 0 2px 0;
}
.section-head-text p {
    font-size: 13px;
    color: #4a5568;
    margin: 0;
}

/* ── Base Tag ── */
.base-tag {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: #eef2ff;
    color: #3b5bdb;
    border-radius: 6px;
    padding: 4px 12px;
    font-size: 12px;
    font-weight: 600;
    margin-bottom: 16px;
}

/* ── Issue Badge ── */
.issue-ok { color: #1a9e5c; font-weight: 600; }
.issue-warn { color: #e07b39; font-weight: 600; }
.issue-error { color: #e05252; font-weight: 600; }

/* ══ GLOBAL TEXT VISIBILITY ══════════════════════════════════════════════════ */
section[data-testid="stMain"] p,
section[data-testid="stMain"] span,
section[data-testid="stMain"] li,
section[data-testid="stMain"] strong,
section[data-testid="stMain"] em,
section[data-testid="stMain"] h1,
section[data-testid="stMain"] h2,
section[data-testid="stMain"] h3,
section[data-testid="stMain"] h4,
section[data-testid="stMain"] h5 {
    color: #1a2b4a !important;
}

section[data-testid="stMain"] .stCheckbox label,
section[data-testid="stMain"] .stCheckbox label p,
section[data-testid="stMain"] [data-testid="stCheckbox"] label {
    color: #1a2b4a !important;
    font-weight: 500 !important;
    font-size: 14px !important;
}

section[data-testid="stMain"] .stNumberInput label,
section[data-testid="stMain"] .stNumberInput label p,
section[data-testid="stMain"] [data-testid="stNumberInput"] label {
    color: #1a2b4a !important;
    font-weight: 500 !important;
    font-size: 14px !important;
}
section[data-testid="stMain"] .stNumberInput input {
    color: #1a2b4a !important;
    background: #ffffff !important;
    border: 1px solid #c8cdd8 !important;
}

section[data-testid="stMain"] [data-testid="stSpinner"] p,
section[data-testid="stMain"] [data-testid="stSpinner"] span,
section[data-testid="stMain"] [data-testid="stSpinnerContainer"] p,
section[data-testid="stMain"] .stSpinner p {
    color: #1a2b4a !important;
    font-weight: 500 !important;
}

section[data-testid="stMain"] [data-testid="stAlert"] p,
section[data-testid="stMain"] .stAlert p {
    font-weight: 500 !important;
}

section[data-testid="stMain"] .vega-embed text,
section[data-testid="stMain"] .vega-embed .mark-text text {
    fill: #1a2b4a !important;
}

.hero-title    { color: #ffffff !important; }
.hero-subtitle { color: rgba(255,255,255,0.65) !important; }
.hero-badge    { color: rgba(255,255,255,0.85) !important; }

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #1a2b4a !important;
}
section[data-testid="stSidebar"] * {
    color: rgba(255,255,255,0.85) !important;
}

/* ── Sidebar collapse button (< arrow at right edge when sidebar is open) ── */
[data-testid="stSidebarCollapseButton"] {
    opacity: 1 !important;
    visibility: visible !important;
    background: #1a2b4a !important;
    border-radius: 0 12px 12px 0 !important;
    min-height: 68px !important;
    min-width: 36px !important;
    width: 36px !important;
    box-shadow: 4px 0 20px rgba(26,43,74,0.6) !important;
    border: none !important;
    cursor: pointer !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
}
[data-testid="stSidebarCollapseButton"] svg,
[data-testid="stSidebarCollapseButton"] svg *,
[data-testid="stSidebarCollapseButton"] path,
[data-testid="stSidebarCollapseButton"] polyline,
[data-testid="stSidebarCollapseButton"] line {
    fill: white !important;
    stroke: white !important;
    color: white !important;
}

/* ── Expand button (> shown when sidebar is collapsed) ── */
[data-testid="collapsedControl"] {
    opacity: 1 !important;
    visibility: visible !important;
    background: #1a2b4a !important;
    border-radius: 0 12px 12px 0 !important;
    min-height: 68px !important;
    min-width: 36px !important;
    width: 36px !important;
    box-shadow: 4px 0 20px rgba(26,43,74,0.6) !important;
    border: none !important;
    cursor: pointer !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
}
[data-testid="collapsedControl"] svg,
[data-testid="collapsedControl"] svg *,
[data-testid="collapsedControl"] path,
[data-testid="collapsedControl"] polyline,
[data-testid="collapsedControl"] line {
    fill: white !important;
    stroke: white !important;
    color: white !important;
}
section[data-testid="stSidebar"] .stButton > button,
section[data-testid="stSidebar"] .stFormSubmitButton > button {
    background: rgba(255,255,255,0.1) !important;
    color: white !important;
    border: 1px solid rgba(255,255,255,0.2) !important;
    border-radius: 8px !important;
    font-weight: 500 !important;
    transition: background 0.2s !important;
    width: 100%;
}
section[data-testid="stSidebar"] .stButton > button:hover,
section[data-testid="stSidebar"] .stFormSubmitButton > button:hover {
    background: rgba(255,255,255,0.2) !important;
}
section[data-testid="stSidebar"] hr {
    border-color: rgba(255,255,255,0.1) !important;
}

/* ── Sidebar inputs — light background, dark text ── */
/* Use component-class selectors (higher specificity than the broad * rule) */
section[data-testid="stSidebar"] .stTextInput *,
section[data-testid="stSidebar"] .stTextInput input {
    color: #1a2b4a !important;
    background: #ffffff !important;
}
section[data-testid="stSidebar"] .stTextArea *,
section[data-testid="stSidebar"] .stTextArea textarea {
    color: #1a2b4a !important;
    background: #ffffff !important;
}
section[data-testid="stSidebar"] .stSelectbox *,
section[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div {
    color: #1a2b4a !important;
    background: #ffffff !important;
}
section[data-testid="stSidebar"] .stDateInput *,
section[data-testid="stSidebar"] .stDateInput input {
    color: #1a2b4a !important;
    background: #ffffff !important;
}

/* ── Main content inputs ── */
section[data-testid="stMain"] input,
section[data-testid="stMain"] textarea {
    background: #ffffff !important;
    color: #1a2b4a !important;
    border: 1px solid #c8cdd8 !important;
    border-radius: 8px !important;
}
section[data-testid="stMain"] input::placeholder,
section[data-testid="stMain"] textarea::placeholder {
    color: #9aa5b4 !important;
}
section[data-testid="stMain"] [data-baseweb="select"] > div:first-child {
    background: #ffffff !important;
    border: 1px solid #c8cdd8 !important;
}
section[data-testid="stMain"] [data-baseweb="select"] span,
section[data-testid="stMain"] [data-baseweb="select"] div {
    color: #1a2b4a !important;
}

/* ── Sidebar radio nav ── */
section[data-testid="stSidebar"] .stRadio > div {
    gap: 4px !important;
}
section[data-testid="stSidebar"] .stRadio label {
    background: rgba(255,255,255,0.05) !important;
    border-radius: 8px !important;
    padding: 10px 14px !important;
    font-size: 14px !important;
    font-weight: 500 !important;
    cursor: pointer !important;
    transition: background 0.15s !important;
    display: flex !important;
    align-items: center !important;
}
section[data-testid="stSidebar"] .stRadio label:hover {
    background: rgba(255,255,255,0.12) !important;
}
section[data-testid="stSidebar"] .stRadio [aria-checked="true"] + div label,
section[data-testid="stSidebar"] .stRadio label:has(input:checked) {
    background: rgba(255,255,255,0.18) !important;
    font-weight: 600 !important;
}

/* ── Download Button ── */
div[data-testid="stDownloadButton"] > button {
    background: #1a2b4a !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    padding: 10px 20px !important;
    font-weight: 500 !important;
}
div[data-testid="stDownloadButton"] > button:hover {
    background: #243860 !important;
}

/* ── Dataframe ── */
.stDataFrame { border-radius: 10px; overflow: hidden; }

/* ── Divider ── */
hr { border-color: #e4e7ef !important; margin: 24px 0 !important; }

/* ── Task Tracker — Priority Pills ── */
.pill-p1 { display:inline-block; background:#fde8e8; color:#c0392b; border:1px solid #f5c6c6; border-radius:20px; padding:2px 10px; font-size:11px; font-weight:700; }
.pill-p2 { display:inline-block; background:#fff0e0; color:#c47f00; border:1px solid #f5d9a0; border-radius:20px; padding:2px 10px; font-size:11px; font-weight:700; }
.pill-p3 { display:inline-block; background:#f0f2f7; color:#4a5568; border:1px solid #d0d5e0; border-radius:20px; padding:2px 10px; font-size:11px; font-weight:700; }
.type-badge { display:inline-block; background:#eef2ff; color:#3b5bdb; border-radius:6px; padding:2px 8px; font-size:11px; font-weight:600; text-transform:capitalize; }
.task-title { font-size:14px; font-weight:600; color:#1a2b4a; }
.task-title-done { font-size:14px; font-weight:500; color:#9aa5b4; text-decoration:line-through; }
.task-desc { font-size:12px; color:#6b7a94; margin-top:2px; }
.overdue { color:#e05252 !important; font-weight:600 !important; }
</style>
""", unsafe_allow_html=True)

# ═════════════════════════════════════════════════════════════════════════════
# TASK TRACKER HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _priority_pill(p):
    icons = {"P1": "🔴", "P2": "🟠", "P3": "⚪"}
    cls   = {"P1": "pill-p1", "P2": "pill-p2", "P3": "pill-p3"}
    return f'<span class="{cls.get(p,"pill-p3")}">{icons.get(p,"")} {p}</span>'

def _is_overdue(task):
    if task.get("status") == "done" or task.get("type") != "one-off":
        return False
    due = task.get("due_date")
    if not due:
        return False
    try:
        return date.fromisoformat(str(due)) < date.today()
    except ValueError:
        return False

def _render_task_row(task, kp=""):
    tid     = task["id"]
    is_done = task.get("status") == "done"
    col_chk, col_info, col_type, col_due, col_edit, col_del = st.columns([0.04, 0.52, 0.1, 0.18, 0.08, 0.08])

    with col_chk:
        checked = st.checkbox("", value=is_done, key=f"{kp}chk_{tid}", label_visibility="collapsed")
        if checked != is_done:
            update_task(tid, {"status": "done" if checked else "todo"})
            st.session_state.editing_task_id = st.session_state.deleting_task_id = None
            st.rerun()

    with col_info:
        title_cls = "task-title-done" if is_done else "task-title"
        desc_html = f'<div class="task-desc">{task["description"]}</div>' if task.get("description") else ""
        st.markdown(f'<div class="{title_cls}">{_priority_pill(task.get("priority","P3"))} {task["title"]}</div>{desc_html}', unsafe_allow_html=True)

    with col_type:
        st.markdown(f'<div style="margin-top:6px;"><span class="type-badge">{task.get("type","one-off")}</span></div>', unsafe_allow_html=True)

    with col_due:
        due = task.get("due_date")
        if due:
            cls = "overdue" if _is_overdue(task) else ""
            flag = " ⚠️" if _is_overdue(task) else ""
            st.markdown(f'<div style="margin-top:8px;"><span class="{cls}" style="font-size:12px;">📅 {due}{flag}</span></div>', unsafe_allow_html=True)

    with col_edit:
        editing_this = st.session_state.editing_task_id == tid
        if st.button("✖️" if editing_this else "✏️", key=f"{kp}edit_btn_{tid}", help="Edit"):
            st.session_state.editing_task_id  = None if editing_this else tid
            st.session_state.deleting_task_id = None
            st.rerun()

    with col_del:
        deleting_this = st.session_state.deleting_task_id == tid
        if st.button("✖️" if deleting_this else "🗑️", key=f"{kp}del_btn_{tid}", help="Delete"):
            st.session_state.deleting_task_id = None if deleting_this else tid
            st.session_state.editing_task_id  = None
            st.rerun()

    if st.session_state.editing_task_id == tid:
        with st.form(key=f"{kp}edit_form_{tid}"):
            st.markdown("**Edit Task**")
            e_title = st.text_input("Title", value=task.get("title", ""))
            e_desc  = st.text_area("Description", value=task.get("description", ""), height=70)
            _types  = ["daily", "weekly", "monthly", "one-off"]
            e_type  = st.selectbox("Type", _types, index=_types.index(task.get("type", "one-off")))
            _pris   = ["P1", "P2", "P3"]
            e_pri   = st.selectbox("Priority", _pris, index=_pris.index(task.get("priority", "P2")))
            raw_due = task.get("due_date")
            e_due   = st.date_input("Due Date", value=date.fromisoformat(raw_due) if raw_due else None)
            s_col, c_col = st.columns(2)
            with s_col: save_btn   = st.form_submit_button("💾 Save",  use_container_width=True)
            with c_col: cancel_btn = st.form_submit_button("Cancel", use_container_width=True)
        if save_btn:
            update_task(tid, {"title": e_title.strip(), "description": e_desc.strip(),
                              "type": e_type, "priority": e_pri,
                              "due_date": str(e_due) if e_due else None})
            st.session_state.editing_task_id = None
            st.rerun()
        if cancel_btn:
            st.session_state.editing_task_id = None
            st.rerun()

    if st.session_state.deleting_task_id == tid:
        st.warning(f'Delete **"{task["title"]}"**? This cannot be undone.')
        dc, ac = st.columns(2)
        with dc:
            if st.button("🗑️ Confirm", key=f"{kp}confirm_del_{tid}", use_container_width=True):
                delete_task(tid)
                st.session_state.deleting_task_id = None
                st.rerun()
        with ac:
            if st.button("Cancel", key=f"{kp}abort_del_{tid}", use_container_width=True):
                st.session_state.deleting_task_id = None
                st.rerun()

    st.markdown("<hr style='margin:4px 0; border-color:#f0f2f7;'>", unsafe_allow_html=True)


def _render_task_tab(filter_type, all_tasks):
    filtered = all_tasks if filter_type == "all" else [t for t in all_tasks if t.get("type") == filter_type]
    if not filtered:
        st.markdown("<div style='padding:32px 0; text-align:center; color:#9aa5b4; font-size:14px;'>No tasks yet — add one using the sidebar form.</div>", unsafe_allow_html=True)
        return
    pri_ord    = {"P1": 0, "P2": 1, "P3": 2}
    status_ord = {"todo": 0, "in-progress": 1, "done": 2}
    filtered   = sorted(filtered, key=lambda t: (status_ord.get(t.get("status","todo"), 0), pri_ord.get(t.get("priority","P3"), 2)))
    n_done     = sum(1 for t in filtered if t.get("status") == "done")
    n_overdue  = sum(1 for t in filtered if _is_overdue(t))
    ov_badge   = f' &nbsp;<span style="color:#e05252;font-weight:600;">⚠️ {n_overdue} overdue</span>' if n_overdue else ""
    st.markdown(f'<div style="font-size:13px;color:#4a5568;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #e4e7ef;"><strong style="color:#1a2b4a;">{len(filtered)}</strong> tasks &nbsp;·&nbsp; <span style="color:#1a9e5c;font-weight:600;">✅ {n_done} done</span>{ov_badge}</div>', unsafe_allow_html=True)
    for t in filtered:
        _render_task_row(t)

# ── Task Tracker init ─────────────────────────────────────────────────────────
for _k in ("editing_task_id", "deleting_task_id"):
    if _k not in st.session_state:
        st.session_state[_k] = None
reset_recurring_tasks()

# ── Hero ──────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
    <div class="hero-left">
        <img class="hero-logo" src="https://partingpro.com/wp-content/uploads/2024/07/partingpro-logo_white.png" />
        <div class="hero-title">Aftercare Operations — Audit Dashboard</div>
        <div class="hero-subtitle">Monitor Airtable data quality and Zapier automation health</div>
    </div>
    <div class="hero-badge">🔒 Internal Tool &nbsp;·&nbsp; Parting Pro</div>
</div>
""", unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────
run_phones   = False
run_messages = False

with st.sidebar:
    st.markdown("""
    <div style="text-align:center; padding: 16px 0 8px 0;">
        <img src="https://partingpro.com/wp-content/uploads/2024/07/partingpro-logo_white.png"
             style="height:28px; filter: brightness(0) invert(1);" />
    </div>
    """, unsafe_allow_html=True)
    st.markdown("---")

    st.markdown("<div style='font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; opacity:0.5; margin-bottom:10px;'>Menu</div>", unsafe_allow_html=True)
    page = st.radio(
        "Navigate",
        options=["📋  Airtable Audit", "⚡  Zapier Audit", "✅  Tasks", "📊  History", "🤖  Smart Inbox"],
        label_visibility="collapsed",
        key="nav_page",
    )
    st.markdown("---")

    if page == "📋  Airtable Audit":
        st.markdown("<div style='font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; opacity:0.5; margin-bottom:12px;'>Audit Controls</div>", unsafe_allow_html=True)
        run_phones   = st.button("📞  Run Phone Audit",   use_container_width=True)
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        run_messages = st.button("💬  Run Message Audit", use_container_width=True)
        st.markdown("---")
        st.markdown("<div style='font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; opacity:0.5; margin-bottom:8px;'>Connected Bases</div>", unsafe_allow_html=True)
        for b in BASE_IDS:
            st.markdown(f"<div style='font-size:12px; opacity:0.7; padding: 4px 0;'>• {b}</div>", unsafe_allow_html=True)
        st.markdown("---")

    elif page == "🤖  Smart Inbox":
        st.markdown("<div style='font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; opacity:0.5; margin-bottom:12px;'>Auto-Scan</div>", unsafe_allow_html=True)
        st.selectbox(
            "Scan every",
            options=[5, 10, 15, 30],
            index=1,
            format_func=lambda x: f"{x} minutes",
            key="inbox_refresh_interval",
        )
        st.markdown("---")

    elif page == "✅  Tasks":
        st.markdown("<div style='font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; opacity:0.5; margin-bottom:12px;'>Add Task</div>", unsafe_allow_html=True)
        with st.form("sidebar_add_task", clear_on_submit=True):
            _title = st.text_input("Title *", placeholder="What needs to be done?")
            _desc  = st.text_area("Description", placeholder="Optional…", height=60)
            _type  = st.selectbox("Type", ["daily", "weekly", "monthly", "one-off"])
            _pri   = st.selectbox("Priority", ["P1", "P2", "P3"], index=1)
            _due   = st.date_input("Due Date", value=None) if _type == "one-off" else None
            _sub   = st.form_submit_button("➕ Add Task", use_container_width=True)
        if _sub:
            if _title.strip():
                add_task({"title": _title.strip(), "description": _desc.strip(),
                          "type": _type, "priority": _pri,
                          "due_date": str(_due) if _due else None})
                st.rerun()
            else:
                st.warning("Title required.")
        st.markdown("---")

    st.markdown("<div style='font-size:11px; opacity:0.4; text-align:center;'>Parting Pro Internal · 2025</div>", unsafe_allow_html=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE — AIRTABLE AUDIT
# ═════════════════════════════════════════════════════════════════════════════
if page == "📋  Airtable Audit":
    # ── Phone Audit ───────────────────────────────────────────────────────────
    st.markdown("""
    <div class="section-wrap">
        <div class="section-head">
            <div class="section-icon">📞</div>
            <div class="section-head-text">
                <h3>Step 1 — Phone Number Audit</h3>
                <p>Validates Contact Cell format against E.164 standard (+1XXXXXXXXXX)</p>
            </div>
        </div>
    """, unsafe_allow_html=True)

    if run_phones:
        for base_id in BASE_IDS:
            with st.spinner(f"Fetching records from {base_id}..."):
                base_name = get_base_name(base_id)
                df = run_phone_audit(base_id, base_name)
                st.session_state[f"phone_{base_id}"] = df
                st.session_state[f"phone_name_{base_id}"] = base_name
        st.success("✅ Phone audit complete for both bases!")

    for base_id in BASE_IDS:
        if f"phone_{base_id}" in st.session_state:
            df = st.session_state[f"phone_{base_id}"]
            base_name = st.session_state[f"phone_name_{base_id}"]

            total = len(df)
            ok = len(df[df["Issue"] == "OK"])
            flagged = len(df[df["Issue"] != "OK"])
            pass_rate = round((ok / total * 100), 1) if total else 0

            st.markdown(f'<div class="base-tag">🏢 {base_name}</div>', unsafe_allow_html=True)
            st.markdown(f"""
            <div class="metrics-row">
                <div class="metric blue">
                    <div class="m-label">Total Records</div>
                    <div class="m-value">{total:,}</div>
                    <div class="m-sub">Contact List</div>
                </div>
                <div class="metric green">
                    <div class="m-label">✅ Passing</div>
                    <div class="m-value">{ok:,}</div>
                    <div class="m-sub">{pass_rate}% pass rate</div>
                </div>
                <div class="metric red">
                    <div class="m-label">⚠️ Flagged</div>
                    <div class="m-value">{flagged:,}</div>
                    <div class="m-sub">Need attention</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            issue_counts = df[df["Issue"] != "OK"]["Issue"].value_counts().reset_index()
            issue_counts.columns = ["Issue", "Count"]
            if not issue_counts.empty:
                st.markdown("**Issue Breakdown**")
                st.bar_chart(issue_counts.set_index("Issue"), color="#1a2b4a")

            flagged_df = df[df["Issue"] != "OK"]
            if not flagged_df.empty:
                st.markdown(f"**Flagged Records — {len(flagged_df)} total**")
                st.dataframe(flagged_df, use_container_width=True, hide_index=True)

            FIXABLE_ISSUES = {"Missing country code (+1)", "Has digits but wrong format"}
            fixable_rows = []
            for _, row in df[df["Issue"].isin(FIXABLE_ISSUES)].iterrows():
                fixed = fix_phone_number(row["Current Value"])
                if fixed:
                    fixable_rows.append({
                        "record_id": row["Record ID"],
                        "Contact Full Name": row["Contact Full Name"],
                        "Funeral Home": row["Funeral Home"],
                        "Current Value": row["Current Value"],
                        "Fixed Value": fixed,
                        "Issue": row["Issue"],
                    })

            if fixable_rows:
                fix_df = pd.DataFrame(fixable_rows)
                applied_key = f"fix_applied_{base_id}"
                if applied_key not in st.session_state:
                    st.session_state[applied_key] = set()

                pending = [r for r in fixable_rows
                           if r["record_id"] not in st.session_state[applied_key]]
                n_done = len(fixable_rows) - len(pending)

                st.markdown(f"**🔧 {len(fix_df)} number(s) can be auto-fixed**")
                st.dataframe(
                    fix_df[["Contact Full Name", "Funeral Home", "Current Value", "Fixed Value", "Issue"]],
                    use_container_width=True, hide_index=True
                )

                if n_done:
                    st.success(f"✅ {n_done} of {len(fixable_rows)} record(s) fixed so far this session.")

                if pending:
                    confirmed = st.checkbox(
                        f"I've reviewed the changes above and want to apply them to {base_name}",
                        key=f"confirm_fix_{base_id}"
                    )
                    if confirmed:
                        max_test = min(10, len(pending))
                        test_n = int(st.number_input(
                            f"How many records to patch first? (max 10 for a safe test run)",
                            min_value=1, max_value=max_test, value=min(3, max_test),
                            key=f"test_n_{base_id}"
                        ))

                        if n_done == 0:
                            if st.button(f"🧪 Test fix ({test_n} record(s))", key=f"test_fix_{base_id}"):
                                with st.spinner(f"Patching {test_n} record(s) in Airtable…"):
                                    ok_count, errs = patch_phone_records(base_id, pending[:test_n])
                                for r in pending[:ok_count]:
                                    st.session_state[applied_key].add(r["record_id"])
                                if errs:
                                    st.warning(f"Fixed {ok_count}/{test_n}. ⚠️ {len(errs)} failed — try again.")
                                else:
                                    st.success(f"✅ Test passed — {ok_count} record(s) fixed. "
                                               f"Check Airtable to confirm, then apply the rest below.")
                                st.rerun()
                        else:
                            col1, col2 = st.columns(2)
                            with col1:
                                if st.button(f"🧪 Test another {test_n} record(s)",
                                             key=f"test_fix_{base_id}"):
                                    with st.spinner(f"Patching {test_n} record(s)…"):
                                        ok_count, errs = patch_phone_records(base_id, pending[:test_n])
                                    for r in pending[:ok_count]:
                                        st.session_state[applied_key].add(r["record_id"])
                                    if errs:
                                        st.warning(f"Fixed {ok_count}/{test_n}. ⚠️ {len(errs)} failed.")
                                    else:
                                        st.success(f"✅ Fixed {ok_count} more. "
                                                   f"{len(pending) - ok_count} remaining.")
                                    st.rerun()
                            with col2:
                                if st.button(f"✅ Apply all {len(pending)} remaining",
                                             key=f"apply_all_{base_id}"):
                                    with st.spinner(f"Patching {len(pending)} record(s)…"):
                                        ok_count, errs = patch_phone_records(base_id, pending)
                                    for r in pending[:ok_count]:
                                        st.session_state[applied_key].add(r["record_id"])
                                    if errs:
                                        st.warning(f"Fixed {ok_count}. ⚠️ {len(errs)} failed — re-run audit to retry.")
                                    else:
                                        st.success(f"✅ All done! Fixed {ok_count} records in {base_name}.")
                                    st.rerun()
                else:
                    st.success(f"✅ All {len(fixable_rows)} numbers in {base_name} are fixed!")
                    if st.button("🔄 Re-run audit to confirm", key=f"clear_{base_id}"):
                        del st.session_state[f"phone_{base_id}"]
                        del st.session_state[f"phone_name_{base_id}"]
                        if applied_key in st.session_state:
                            del st.session_state[applied_key]
                        st.rerun()

            elif flagged > 0:
                st.info("ℹ️ No auto-fixable numbers found — all flagged records need manual review in Airtable.")
            st.markdown("---")

    st.markdown("</div>", unsafe_allow_html=True)

    if any(f"phone_{b}" in st.session_state for b in BASE_IDS):
        all_dfs = {
            f"{st.session_state[f'phone_name_{b}']} - Issues": st.session_state[f"phone_{b}"][st.session_state[f"phone_{b}"]["Issue"] != "OK"]
            for b in BASE_IDS if f"phone_{b}" in st.session_state
        }
        excel_buf = build_excel(all_dfs)
        st.download_button("⬇️ Download Phone Audit Report (.xlsx)",
                           excel_buf, "phone_audit_results.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # ── Message Audit ─────────────────────────────────────────────────────────
    st.markdown("""
    <div class="section-wrap">
        <div class="section-head">
            <div class="section-icon">💬</div>
            <div class="section-head-text">
                <h3>Step 2 — Message Content Audit</h3>
                <p>Scans outbound messages for unfilled placeholders, empty content, and short messages</p>
            </div>
        </div>
    """, unsafe_allow_html=True)

    if run_messages:
        for base_id in BASE_IDS:
            with st.spinner(f"Fetching outbound messages from {base_id}..."):
                base_name = get_base_name(base_id)
                df = run_message_audit(base_id, base_name)
                st.session_state[f"msg_{base_id}"] = df
                st.session_state[f"msg_name_{base_id}"] = base_name
        st.success("✅ Message audit complete for both bases!")

    for base_id in BASE_IDS:
        if f"msg_{base_id}" in st.session_state:
            df = st.session_state[f"msg_{base_id}"]
            base_name = st.session_state[f"msg_name_{base_id}"]

            excl_test = st.checkbox(
                "🔕 Exclude messages containing 'test'",
                value=True,
                key=f"excl_test_{base_id}"
            )
            df_view = (
                df[~df["Content (first 200 chars)"].str.contains("test", case=False, na=False)]
                if excl_test else df
            )

            total = len(df_view)
            ok = len(df_view[df_view["Issue"] == "OK"])
            flagged = len(df_view[df_view["Issue"] != "OK"])
            pass_rate = round((ok / total * 100), 1) if total else 0

            st.markdown(f'<div class="base-tag">🏢 {base_name}</div>', unsafe_allow_html=True)
            st.markdown(f"""
            <div class="metrics-row">
                <div class="metric blue">
                    <div class="m-label">Total Outbound</div>
                    <div class="m-value">{total:,}</div>
                    <div class="m-sub">Outbound messages</div>
                </div>
                <div class="metric green">
                    <div class="m-label">✅ Passing</div>
                    <div class="m-value">{ok:,}</div>
                    <div class="m-sub">{pass_rate}% pass rate</div>
                </div>
                <div class="metric red">
                    <div class="m-label">⚠️ Flagged</div>
                    <div class="m-value">{flagged:,}</div>
                    <div class="m-sub">Need attention</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            issue_counts = df_view[df_view["Issue"] != "OK"]["Issue"].value_counts().reset_index()
            issue_counts.columns = ["Issue", "Count"]
            if not issue_counts.empty:
                st.markdown("**Issue Breakdown**")
                st.bar_chart(issue_counts.set_index("Issue"), color="#1a2b4a")

            flagged_df = df_view[df_view["Issue"] != "OK"]
            if not flagged_df.empty:
                st.markdown(f"**Flagged Records — {len(flagged_df)} total**")
                st.dataframe(flagged_df, use_container_width=True, hide_index=True)

            ph_df = df_view[df_view["Issue"] == "Unfilled placeholder"].copy()
            if not ph_df.empty:
                ph_df["Bad Token(s)"] = ph_df["Content (first 200 chars)"].apply(extract_tokens)

                all_tokens = Counter()
                for content in ph_df["Content (first 200 chars)"]:
                    for p in PLACEHOLDER_PATTERNS:
                        for m in p.findall(str(content)):
                            all_tokens[m] += 1

                st.markdown("**📋 Unfilled Placeholder Breakdown**")
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.markdown("**Token frequency**")
                    st.dataframe(
                        pd.DataFrame(all_tokens.most_common(), columns=["Bad Token", "Times Sent"]),
                        use_container_width=True, hide_index=True
                    )
                with col2:
                    st.markdown(f"**{len(ph_df)} affected message(s)**")
                    st.dataframe(
                        ph_df[["Contact Full Name", "Funeral Home",
                               "Bad Token(s)", "Content (first 200 chars)"]],
                        use_container_width=True, hide_index=True
                    )
                st.info(
                    "ℹ️ These messages were already sent with unfilled tokens. "
                    "The contacts above may need a follow-up message. "
                    "Fix the corresponding message templates to prevent future occurrences."
                )
            st.markdown("---")

    st.markdown("</div>", unsafe_allow_html=True)

    if any(f"msg_{b}" in st.session_state for b in BASE_IDS):
        all_dfs = {
            f"{st.session_state[f'msg_name_{b}']} - Issues": st.session_state[f"msg_{b}"][st.session_state[f"msg_{b}"]["Issue"] != "OK"]
            for b in BASE_IDS if f"msg_{b}" in st.session_state
        }
        excel_buf = build_excel(all_dfs)
        st.download_button("⬇️ Download Message Audit Report (.xlsx)",
                           excel_buf, "messages_audit_results.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ═════════════════════════════════════════════════════════════════════════════
# PAGE — ZAPIER AUDIT
# ═════════════════════════════════════════════════════════════════════════════
elif page == "⚡  Zapier Audit":
    st.markdown("""
    <div class="section-wrap">
        <div class="section-head">
            <div class="section-icon">⚡</div>
            <div class="section-head-text">
                <h3>Zapier Automation Audit</h3>
                <p>Detects failing zaps, high error rates, halted runs, and volume anomalies</p>
            </div>
        </div>
    """, unsafe_allow_html=True)

    zap_account_id  = _secret("ZAPIER_ACCOUNT_ID") or "22022304"
    zap_session_sec = _secret("ZAPIER_SESSION")
    zap_csrf_sec    = _secret("ZAPIER_CSRF")
    creds_expired   = st.session_state.get("zap_creds_expired", False)
    no_secrets      = not (zap_session_sec and zap_csrf_sec)

    if creds_expired or no_secrets:
        if creds_expired:
            st.warning(
                "⚠️ Your Zapier session has expired. "
                "Paste fresh cookies below to continue."
            )
        else:
            st.info("Zapier session credentials not found in secrets — enter them below.")

        with st.expander("❓ How to get fresh cookies", expanded=creds_expired):
            st.markdown("""
            1. Log into [zapier.com](https://zapier.com) in Chrome
            2. Open DevTools (`F12`) → **Application** → **Cookies** → `zapier.com`
            3. Copy the value of `zapsession` → paste as **Session Token**
            4. Copy the value of `csrftoken` → paste as **CSRF Token**

            Cookies expire every 1–4 weeks.
            """)
        zap_session = st.text_input("Session Token (zapsession cookie)", type="password", key="zap_session_input")
        zap_csrf    = st.text_input("CSRF Token (csrftoken cookie)",     type="password", key="zap_csrf_input")
    else:
        zap_session = zap_session_sec
        zap_csrf    = zap_csrf_sec
        st.success("✅ Zapier credentials loaded from secrets.")

    col_date, col_btn = st.columns([3, 1])
    with col_date:
        today     = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        date_range = st.date_input(
            "Date Range",
            value=(yesterday, today),
            max_value=today,
            key="zap_audit_date_picker",
            help="Select a start and end date. Single day = click the same date twice."
        )
    with col_btn:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        run_zap_audit = st.button("⚡ Run Zapier Audit", use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

    if run_zap_audit:
        if not zap_account_id:
            st.error("❌ ZAPIER_ACCOUNT_ID not found in Streamlit secrets.")
        elif not (zap_session and zap_csrf):
            st.error("Please enter your Zapier session credentials above.")
        elif not isinstance(date_range, (list, tuple)) or len(date_range) != 2:
            st.error("Please select both a start and end date.")
        else:
            start_str = date_range[0].strftime("%Y-%m-%d")
            end_str   = date_range[1].strftime("%Y-%m-%d")
            label     = start_str if start_str == end_str else f"{start_str} → {end_str}"
            with st.spinner(f"Fetching Zapier run history for {label}…"):
                try:
                    runs = fetch_zap_runs_for_range(
                        zap_session, zap_csrf, zap_account_id, start_str, end_str
                    )
                    summaries = build_zap_summaries(runs)
                    flags     = detect_zap_flags(summaries)
                    st.session_state["zap_runs"]       = runs
                    st.session_state["zap_summaries"]  = summaries
                    st.session_state["zap_flags"]       = flags
                    st.session_state["zap_audit_label"] = label
                    st.session_state["zap_creds_expired"] = False
                    st.success(f"✅ Fetched {len(runs):,} runs across {len(summaries)} zap(s).")
                except requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code in (401, 403):
                        st.session_state["zap_creds_expired"] = True
                        st.rerun()
                    else:
                        st.error(f"❌ HTTP error: {e}")
                except Exception as e:
                    st.error(f"❌ Error: {e}")

    if "zap_summaries" in st.session_state and st.session_state["zap_summaries"]:
        summaries  = st.session_state["zap_summaries"]
        flags      = st.session_state["zap_flags"]
        audit_label = st.session_state.get("zap_audit_label", "")

        total_runs    = sum(z["Total Runs"] for z in summaries)
        total_errors  = sum(z["Errors"]     for z in summaries)
        total_success = sum(z["Success"]    for z in summaries)
        n_critical    = sum(1 for f in flags if "Critical" in f["Severity"])
        n_warnings    = sum(1 for f in flags if "Warning"  in f["Severity"])
        overall_rate  = round(total_success / total_runs * 100, 1) if total_runs else 0

        crit_cls = "red" if n_critical > 0 else "green"
        warn_cls = "red" if n_warnings > 0 else "green"
        rate_cls = "green" if overall_rate >= 90 else "red"

        st.markdown(f"""
        <div class="metrics-row">
            <div class="metric blue">
                <div class="m-label">Zaps Active</div>
                <div class="m-value">{len(summaries)}</div>
                <div class="m-sub">{audit_label}</div>
            </div>
            <div class="metric blue">
                <div class="m-label">Total Runs</div>
                <div class="m-value">{total_runs:,}</div>
                <div class="m-sub">all zaps</div>
            </div>
            <div class="metric {crit_cls}">
                <div class="m-label">🔴 Critical</div>
                <div class="m-value">{n_critical}</div>
                <div class="m-sub">flags</div>
            </div>
            <div class="metric {warn_cls}">
                <div class="m-label">⚠️ Warnings</div>
                <div class="m-value">{n_warnings}</div>
                <div class="m-sub">flags</div>
            </div>
            <div class="metric {rate_cls}">
                <div class="m-label">Success Rate</div>
                <div class="m-value">{overall_rate}%</div>
                <div class="m-sub">overall</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        status_cols = ["Success", "Errors", "Halted", "Filtered", "Throttled", "Other"]
        status_totals = {s: sum(z[s] for z in summaries) for s in status_cols}
        chart_df = pd.DataFrame([
            {"Status": k, "Count": v} for k, v in status_totals.items() if v > 0
        ])
        if not chart_df.empty:
            st.markdown("**Run Status Breakdown**")
            st.bar_chart(chart_df.set_index("Status"), color="#1a2b4a")

        st.markdown("---")
        if flags:
            st.markdown(f"**🚩 {len(flags)} Flag(s) Detected — sorted by severity**")
            flags_df = pd.DataFrame(flags).sort_values(
                "Severity",
                key=lambda s: s.map({"🔴 Critical": 0, "⚠️ Warning": 1}).fillna(2)
            )
            st.dataframe(flags_df, use_container_width=True, hide_index=True)
        else:
            st.success("✅ No flags detected — all zaps are running cleanly!")

        st.markdown("---")
        st.markdown("**Zap-by-Zap Breakdown**")
        display_cols = ["Zap Title", "Total Runs", "Success", "Errors",
                        "Halted", "Filtered", "Throttled", "Error Rate", "Success Rate"]
        summary_df = (
            pd.DataFrame([{k: v for k, v in z.items() if not k.startswith("_")} for z in summaries])
            [display_cols]
            .sort_values("Errors", ascending=False)
        )
        st.dataframe(summary_df, use_container_width=True, hide_index=True)

        zap_export_df = summary_df.copy()
        zap_excel = build_excel({"Zapier Run Summary": zap_export_df})
        if flags:
            flags_export = pd.DataFrame(flags)
            zap_excel = build_excel({
                "Flags": flags_export,
                "Zap Summary": zap_export_df,
            })
        st.download_button(
            "⬇️ Download Zapier Audit Report (.xlsx)",
            zap_excel,
            f"zapier_audit_{audit_label}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ═════════════════════════════════════════════════════════════════════════════
# PAGE — TASK TRACKER
# ═════════════════════════════════════════════════════════════════════════════
elif page == "✅  Tasks":
    _tasks   = load_tasks()
    _today   = date.today()
    _today_s = _today.isoformat()
    _active  = [t for t in _tasks if t.get("status") != "done"]
    _done_td = [t for t in _tasks if t.get("status") == "done" and t.get("completed_at","")[:10] == _today_s]
    _overdue = [t for t in _tasks if _is_overdue(t)]
    _p1_open = [t for t in _tasks if t.get("priority") == "P1" and t.get("status") != "done"]

    st.markdown("""
    <div class="section-head" style="margin-bottom:20px;">
        <div class="section-icon">✅</div>
        <div class="section-head-text">
            <h3>Task Tracker</h3>
            <p>Stored in Airtable — tasks persist across sessions. Use the sidebar to add tasks.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Metrics ───────────────────────────────────────────────────────────────
    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("Total Active", len(_active),  help="All non-done tasks")
    mc2.metric("Done Today",   len(_done_td), help="Completed today")
    mc3.metric("Overdue",      len(_overdue), help="One-off tasks past due date")
    mc4.metric("P1 Items",     len(_p1_open), help="High-priority open tasks")

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # ── Quick-add ─────────────────────────────────────────────────────────────
    _qv = st.text_input(
        "quick_add", placeholder="⚡ Quick-add a task — type and press Enter",
        key="quick_capture", label_visibility="collapsed"
    )
    if _qv and _qv != st.session_state.get("_last_quick", ""):
        st.session_state["_last_quick"] = _qv
        add_task({"title": _qv.strip(), "type": "one-off", "priority": "P2"})
        st.rerun()

    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    # ── Date filter ───────────────────────────────────────────────────────────
    st.markdown("**📅 View tasks for:**")
    _df_c1, _df_c2, _df_c3, _df_c4, _df_c5 = st.columns([1,1,1,1,2])
    _view_date = st.session_state.get("task_view_date", _today)

    with _df_c1:
        if st.button("Today", use_container_width=True, key="btn_today"):
            st.session_state["task_view_date"] = _today
            st.rerun()
    with _df_c2:
        if st.button("Yesterday", use_container_width=True, key="btn_yesterday"):
            st.session_state["task_view_date"] = _today - timedelta(days=1)
            st.rerun()
    with _df_c3:
        if st.button("This Week", use_container_width=True, key="btn_week"):
            st.session_state["task_view_date"] = _today
            st.rerun()
    with _df_c4:
        if st.button("This Month", use_container_width=True, key="btn_month"):
            st.session_state["task_view_date"] = _today
            st.rerun()
    with _df_c5:
        _picked = st.date_input(
            "Pick a date", value=_view_date, key="task_date_picker",
            label_visibility="collapsed"
        )
        if _picked != _view_date:
            st.session_state["task_view_date"] = _picked
            st.rerun()

    _view_date = st.session_state.get("task_view_date", _today)

    # ── Filter label ──────────────────────────────────────────────────────────
    _vd_monday = _view_date - timedelta(days=_view_date.weekday())
    _vd_sunday = _vd_monday + timedelta(days=6)
    _date_label = (
        "Today" if _view_date == _today
        else f"Week of {_vd_monday.strftime('%b %-d')} – {_vd_sunday.strftime('%b %-d')}"
        if st.session_state.get("_week_mode")
        else _view_date.strftime("%A, %B %-d %Y")
    )
    st.markdown(
        f"<div style='font-size:13px;color:#4a5568;margin:8px 0 4px 0;'>"
        f"Showing tasks for <strong style='color:#1a2b4a;'>{_view_date.strftime('%A, %B %-d, %Y')}</strong></div>",
        unsafe_allow_html=True
    )

    # ── Determine which tasks are relevant for _view_date ─────────────────────
    def _task_visible(task, view_date):
        task_type = task.get("type", "one-off")
        # Parse created_at
        raw_created = task.get("created_at", "")
        try:
            created_date = datetime.fromisoformat(raw_created).date() if raw_created else date.min
        except Exception:
            created_date = date.min
        if created_date > view_date:
            return False  # Task didn't exist on this date

        if task_type == "daily":
            return True
        elif task_type == "weekly":
            # Show for any day in the same Mon–Sun week
            view_monday = view_date - timedelta(days=view_date.weekday())
            task_monday = created_date - timedelta(days=created_date.weekday())
            return view_monday >= task_monday
        elif task_type == "monthly":
            # Show for any day in any month since creation
            return (view_date.year, view_date.month) >= (created_date.year, created_date.month)
        elif task_type == "one-off":
            due = task.get("due_date")
            if not due:
                return True  # No due date → always show
            try:
                return date.fromisoformat(str(due)) == view_date
            except Exception:
                return True
        return True

    # ── Type filter tabs ──────────────────────────────────────────────────────
    _type_tabs = st.tabs(["All", "Daily", "Weekly", "Monthly", "One-Off"])
    _type_keys = ["all", "daily", "weekly", "monthly", "one-off"]

    for _tab, _tkey in zip(_type_tabs, _type_keys):
        with _tab:
            if _tkey == "all":
                _visible = [t for t in _tasks if _task_visible(t, _view_date)]
            else:
                _visible = [t for t in _tasks if t.get("type") == _tkey and _task_visible(t, _view_date)]

            if not _visible:
                st.markdown(
                    "<div style='padding:24px 0;text-align:center;color:#9aa5b4;font-size:14px;'>"
                    "No tasks for this date — add one using the sidebar.</div>",
                    unsafe_allow_html=True
                )
            else:
                _pri_ord    = {"P1": 0, "P2": 1, "P3": 2}
                _status_ord = {"todo": 0, "in-progress": 1, "done": 2}
                _visible    = sorted(
                    _visible,
                    key=lambda t: (
                        _status_ord.get(t.get("status", "todo"), 0),
                        _pri_ord.get(t.get("priority", "P3"), 2),
                    )
                )
                _n_done    = sum(1 for t in _visible if t.get("status") == "done")
                _n_overdue = sum(1 for t in _visible if _is_overdue(t))
                _ov_badge  = (
                    f' &nbsp;<span style="color:#e05252;font-weight:600;">⚠️ {_n_overdue} overdue</span>'
                    if _n_overdue else ""
                )
                st.markdown(
                    f'<div style="font-size:13px;color:#4a5568;margin-bottom:12px;padding-bottom:8px;'
                    f'border-bottom:1px solid #e4e7ef;">'
                    f'<strong style="color:#1a2b4a;">{len(_visible)}</strong> tasks &nbsp;·&nbsp; '
                    f'<span style="color:#1a9e5c;font-weight:600;">✅ {_n_done} done</span>{_ov_badge}</div>',
                    unsafe_allow_html=True
                )
                for _t in _visible:
                    _render_task_row(_t, kp=f"{_tkey}_")

# ═════════════════════════════════════════════════════════════════════════════
# PAGE — HISTORY
# ═════════════════════════════════════════════════════════════════════════════
elif page == "📊  History":
    _tasks = load_tasks()
    _completed = [t for t in _tasks if t.get("status") == "done"]

    st.markdown("""
    <div class="section-head" style="margin-bottom:20px;">
        <div class="section-icon">📊</div>
        <div class="section-head-text">
            <h3>Completed Tasks</h3>
            <p>All tasks marked done — recurring tasks reset automatically on their schedule</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Summary metrics ───────────────────────────────────────────────────────
    _c_daily   = [t for t in _completed if t.get("type") == "daily"]
    _c_weekly  = [t for t in _completed if t.get("type") == "weekly"]
    _c_monthly = [t for t in _completed if t.get("type") == "monthly"]
    _c_oneoff  = [t for t in _completed if t.get("type") == "one-off"]

    hc1, hc2, hc3, hc4, hc5 = st.columns(5)
    hc1.metric("Total Done",  len(_completed))
    hc2.metric("Daily",       len(_c_daily))
    hc3.metric("Weekly",      len(_c_weekly))
    hc4.metric("Monthly",     len(_c_monthly))
    hc5.metric("One-Off",     len(_c_oneoff))

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # ── Filter ────────────────────────────────────────────────────────────────
    _hfilter_map = {
        "All Types": "all",
        "Daily": "daily",
        "Weekly": "weekly",
        "Monthly": "monthly",
        "One-Off": "one-off",
    }
    _hfilter_label = st.selectbox(
        "Filter by type",
        list(_hfilter_map.keys()),
        key="history_filter",
        label_visibility="collapsed",
    )
    _hfilter = _hfilter_map[_hfilter_label]
    _view = _completed if _hfilter == "all" else [t for t in _completed if t.get("type") == _hfilter]

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    # ── Task list ─────────────────────────────────────────────────────────────
    st.markdown('<div class="section-wrap">', unsafe_allow_html=True)

    if not _view:
        st.markdown("<div style='padding:32px 0; text-align:center; color:#9aa5b4; font-size:14px;'>No completed tasks in this category yet.</div>", unsafe_allow_html=True)
    else:
        _pri_ord = {"P1": 0, "P2": 1, "P3": 2}
        _view = sorted(_view, key=lambda t: (t.get("completed_at") or ""), reverse=True)

        st.markdown(f'<div style="font-size:13px;color:#4a5568;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #e4e7ef;"><strong style="color:#1a2b4a;">{len(_view)}</strong> completed task{"s" if len(_view) != 1 else ""} — most recent first</div>', unsafe_allow_html=True)

        for t in _view:
            tid = t["id"]
            completed_at = t.get("completed_at", "")
            completed_label = ""
            if completed_at:
                try:
                    completed_label = datetime.fromisoformat(completed_at).strftime("%-d %b %Y, %-I:%M %p")
                except Exception:
                    try:
                        completed_label = datetime.fromisoformat(completed_at).strftime("%d %b %Y")
                    except Exception:
                        completed_label = completed_at[:10]

            h_info, h_meta, h_reopen = st.columns([0.62, 0.28, 0.10])

            with h_info:
                desc_html = f'<div class="task-desc">{t["description"]}</div>' if t.get("description") else ""
                st.markdown(
                    f'<div class="task-title-done">{_priority_pill(t.get("priority","P3"))} {t["title"]}</div>{desc_html}',
                    unsafe_allow_html=True
                )

            with h_meta:
                type_badge = f'<span class="type-badge">{t.get("type","one-off")}</span>'
                date_str = f'<span style="font-size:11px;color:#9aa5b4;margin-left:6px;">✅ {completed_label}</span>' if completed_label else ""
                st.markdown(f'<div style="margin-top:6px;">{type_badge}{date_str}</div>', unsafe_allow_html=True)

            with h_reopen:
                if st.button("↩️", key=f"reopen_{tid}", help="Re-open task"):
                    update_task(tid, {"status": "todo"})
                    st.rerun()

            st.markdown("<hr style='margin:4px 0; border-color:#f0f2f7;'>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # ── Recurrence info ───────────────────────────────────────────────────────
    with st.expander("ℹ️ How recurring tasks work"):
        st.markdown("""
        | Type | Resets when |
        |------|------------|
        | **Daily** | Every new day |
        | **Weekly** | Every Monday |
        | **Monthly** | 1st of each month |
        | **One-Off** | Never — stays done permanently |

        When a recurring task is reset, it moves back to the active Tasks view as **To Do**.
        Completing it again will show it here until the next reset.
        """)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE — SMART INBOX
# ═════════════════════════════════════════════════════════════════════════════
elif page == "🤖  Smart Inbox":
    inbox_scanner.render_inbox_page(
        refresh_interval=st.session_state.get("inbox_refresh_interval", 10)
    )

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center; padding: 32px 0 16px 0;">
    <img src="https://partingpro.com/wp-content/uploads/2024/07/partingpro-logo.png" style="height:22px; opacity:0.4;" />
    <div style="font-size:11px; color:#b0b8c8; margin-top:8px;">Aftercare Operations Audit Tool · Internal Use Only</div>
</div>
""", unsafe_allow_html=True)
