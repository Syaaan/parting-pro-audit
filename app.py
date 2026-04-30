import re
import io
import json
import time
import requests
import openpyxl
import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import date, datetime, timedelta
from collections import Counter
from openpyxl.styles import Font, PatternFill, Alignment
from onboarding_wrapper import OnboardingAutomation, STEPS
from task_store import (
    load_tasks, add_task, update_task, delete_task, reset_recurring_tasks
)

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN = "patm2acj3jyDwBfyD.3fb175e7596542e2a9be3acc07700272cf8cb09028c58cc03a6d8bc5be022542"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
BASE_IDS = ["appbXFzZnhij88tnQ", "appXT2xJZ1zgll4fG"]

N8N_WEBHOOK = "http://localhost:5678/webhook/run-zap-audit"
AUDIT_DIR = Path(__file__).parent.parent / "query"

# ── Zap Audit helpers ─────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def fetch_query_zap_list():
    url = "https://api.airtable.com/v0/appbXFzZnhij88tnQ/Funeral%20Home%20Information"
    params = {
        "filterByFormula": 'AND({Active Status:}="Active",{Parting Pro ID:}>0)',
        "fields[]": ["Funeral Home Name:", "Parting Pro ID:", "Go-Live Date"],
        "pageSize": 100,
    }
    resp = requests.get(url, headers={"Authorization": f"Bearer {TOKEN}"}, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json().get("records", [])

def load_audit_runs() -> list[dict]:
    """Read all audit_run_*.json files from the query/ folder, newest first."""
    files = sorted(AUDIT_DIR.glob("audit_run_*.json"), reverse=True)
    runs = []
    for f in files:
        try:
            runs.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            pass
    return runs

def latest_audit_df(runs: list[dict]) -> pd.DataFrame:
    """Flatten the most recent run into a DataFrame, upgrading No Cases → Dead."""
    if not runs:
        return pd.DataFrame()
    latest = runs[0]
    results = latest.get("results", [])

    # Build a history map: parting_pro_id → list of statuses (newest first)
    history: dict[int, list[str]] = {}
    for run in runs[1:]:
        for r in run.get("results", []):
            ppid = r.get("parting_pro_id")
            if ppid:
                history.setdefault(ppid, []).append(r.get("status", ""))

    rows = []
    for r in results:
        ppid = r.get("parting_pro_id")
        status = r.get("status", "")
        if status == "No Cases":
            # Upgrade to Dead if no Healthy run in the last 14 days
            past = history.get(ppid, [])
            had_healthy_recently = any(s == "Healthy" for s in past[:14])
            if not had_healthy_recently and len(past) >= 2:
                status = "Dead"
        rows.append({
            "Funeral Home": r.get("funeral_home_name", ""),
            "ID": ppid,
            "DB Cases": r.get("db_row_count", 0),
            "Airtable Records": r.get("airtable_record_count", 0),
            "Status": status,
            "Notes": r.get("notes", ""),
            "Go-Live Date": r.get("go_live_date", ""),
        })
    return pd.DataFrame(rows)

STATUS_EMOJI = {
    "Healthy": "🟢",
    "No Cases": "🟡",
    "Missing Data": "🔴",
    "Dead": "⚫",
    "New": "🔵",
}
STATUS_COLOR = {
    "Healthy": "#1a9e5c",
    "No Cases": "#e07b39",
    "Missing Data": "#e05252",
    "Dead": "#4a5568",
    "New": "#3b7de8",
}
TARGET = re.compile(r"^\+1\d{10}$")
PLACEHOLDER_PATTERNS = [
    re.compile(r"\{[^}]+\}"),
    re.compile(r"\[[A-Z][^\]]+\]"),
    re.compile(r"<[A-Z][^>]+>"),
    re.compile(r"\{\{[^}]+\}\}"),
]

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_base_name(base_id):
    r = requests.get("https://api.airtable.com/v0/meta/bases", headers=HEADERS)
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
    """Return reformatted E.164 number, or None if not auto-fixable."""
    digits = re.sub(r"\D", "", str(value).strip())
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None

def has_placeholder(text):
    return any(p.search(text) for p in PLACEHOLDER_PATTERNS)

def extract_tokens(content):
    """Return a comma-separated string of every placeholder token found in content."""
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
    """
    Patch Contact Cell for a list of records in batches of 10 (Airtable limit).
    rows: list of dicts with 'record_id' and 'fixed_value' keys.
    Returns (success_count, error_record_ids).
    """
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
            resp = requests.patch(url, headers=HEADERS, json=payload)
            resp.raise_for_status()
            success += len(batch)
        except Exception:
            errors.extend([r["record_id"] for r in batch])
        time.sleep(0.22)   # ~4.5 req/sec — safely under Airtable's 5 req/sec limit
    return success, errors

def revert_phone_records(base_id, revert_rows):
    """
    Restore Contact Cell to original values.
    revert_rows: list of dicts with 'record_id' and 'original_value' keys.
    Returns (success_count, error_record_ids).
    """
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
            resp = requests.patch(url, headers=HEADERS, json=payload)
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
        r = requests.get(url, headers=HEADERS, params=params)
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

# ── Page Config ───────────────────────────────────────────────────────────────
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

/* Hide default streamlit header */
#MainMenu, footer, header { visibility: hidden; }

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
.metrics-row { display: flex; gap: 16px; margin-bottom: 20px; }
.metric {
    flex: 1;
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

/* ══ GLOBAL TEXT VISIBILITY — main content area only ══════════════════ */

/* Every p, span, label, div text in the main block */
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

/* Checkbox label text */
section[data-testid="stMain"] .stCheckbox label,
section[data-testid="stMain"] .stCheckbox label p,
section[data-testid="stMain"] [data-testid="stCheckbox"] label {
    color: #1a2b4a !important;
    font-weight: 500 !important;
    font-size: 14px !important;
}

/* Number input label + field */
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

/* Spinner / loading text */
section[data-testid="stMain"] [data-testid="stSpinner"] p,
section[data-testid="stMain"] [data-testid="stSpinner"] span,
section[data-testid="stMain"] [data-testid="stSpinnerContainer"] p,
section[data-testid="stMain"] .stSpinner p {
    color: #1a2b4a !important;
    font-weight: 500 !important;
}

/* Alert / banner body text */
section[data-testid="stMain"] [data-testid="stAlert"] p,
section[data-testid="stMain"] .stAlert p {
    font-weight: 500 !important;
}

/* Bar chart axis labels */
section[data-testid="stMain"] .vega-embed text,
section[data-testid="stMain"] .vega-embed .mark-text text {
    fill: #1a2b4a !important;
}

/* Keep hero / sidebar text untouched (white) */
.hero, .hero * { color: inherit; }
section[data-testid="stSidebar"] * { color: rgba(255,255,255,0.85) !important; }

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #1a2b4a !important;
}
section[data-testid="stSidebar"] * {
    color: rgba(255,255,255,0.85) !important;
}
section[data-testid="stSidebar"] .stButton > button {
    background: rgba(255,255,255,0.1) !important;
    color: white !important;
    border: 1px solid rgba(255,255,255,0.2) !important;
    border-radius: 8px !important;
    font-weight: 500 !important;
    transition: background 0.2s !important;
    width: 100%;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(255,255,255,0.2) !important;
}
section[data-testid="stSidebar"] hr {
    border-color: rgba(255,255,255,0.1) !important;
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
.pill-p1 {
    display: inline-block;
    background: #fde8e8; color: #c0392b;
    border: 1px solid #f5c6c6;
    border-radius: 20px; padding: 2px 10px;
    font-size: 11px; font-weight: 700; letter-spacing: 0.04em;
}
.pill-p2 {
    display: inline-block;
    background: #fff0e0; color: #c47f00;
    border: 1px solid #f5d9a0;
    border-radius: 20px; padding: 2px 10px;
    font-size: 11px; font-weight: 700; letter-spacing: 0.04em;
}
.pill-p3 {
    display: inline-block;
    background: #f0f2f7; color: #4a5568;
    border: 1px solid #d0d5e0;
    border-radius: 20px; padding: 2px 10px;
    font-size: 11px; font-weight: 700; letter-spacing: 0.04em;
}
.type-badge {
    display: inline-block;
    background: #eef2ff; color: #3b5bdb;
    border-radius: 6px; padding: 2px 8px;
    font-size: 11px; font-weight: 600;
    text-transform: capitalize;
}
.task-title { font-size: 14px; font-weight: 600; color: #1a2b4a; }
.task-title-done { font-size: 14px; font-weight: 500; color: #9aa5b4; text-decoration: line-through; }
.task-desc { font-size: 12px; color: #6b7a94; margin-top: 2px; }
.overdue { color: #e05252 !important; font-weight: 600 !important; }
.due-ok { color: #4a5568; }
</style>
""", unsafe_allow_html=True)

# ── Task Tracker — session state & recurrence reset ──────────────────────────
for _k in ("editing_task_id", "deleting_task_id"):
    if _k not in st.session_state:
        st.session_state[_k] = None
reset_recurring_tasks()

# ── Hero Section ──────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
    <div class="hero-left">
        <img class="hero-logo" src="https://partingpro.com/wp-content/uploads/2024/07/partingpro-logo_white.png" />
        <div class="hero-title">Aftercare Texting — Audit Dashboard</div>
        <div class="hero-subtitle">Monitor phone number formats and outbound message quality across all bases</div>
    </div>
    <div class="hero-badge">🔒 Internal Tool &nbsp;·&nbsp; Airtable Connected</div>
</div>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="text-align:center; padding: 16px 0 8px 0;">
        <img src="https://partingpro.com/wp-content/uploads/2024/07/partingpro-logo_white.png"
             style="height:28px; filter: brightness(0) invert(1);" />
    </div>
    """, unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("<div style='font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; opacity:0.5; margin-bottom:12px;'>Audit Controls</div>", unsafe_allow_html=True)
    run_phones = st.button("📞  Run Phone Audit", use_container_width=True)
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    run_messages = st.button("💬  Run Message Audit", use_container_width=True)
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    run_zap_audit = st.button("🔍  Run Zap Audit Now", use_container_width=True)
    st.markdown("---")
    st.markdown("<div style='font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; opacity:0.5; margin-bottom:8px;'>Connected Bases</div>", unsafe_allow_html=True)
    for b in BASE_IDS:
        st.markdown(f"<div style='font-size:12px; opacity:0.7; padding: 4px 0;'>• {b}</div>", unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("<div style='font-size:11px; opacity:0.4; text-align:center;'>Parting Pro Internal · 2025</div>", unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("<div style='font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; opacity:0.5; margin-bottom:12px;'>Add Task</div>", unsafe_allow_html=True)
    with st.form("sidebar_add_task", clear_on_submit=True):
        _title = st.text_input("Title *", placeholder="What needs to be done?")
        _desc  = st.text_area("Description", placeholder="Optional…", height=60)
        _type  = st.selectbox("Type", ["daily", "weekly", "monthly", "one-off"])
        _pri   = st.selectbox("Priority", ["P1", "P2", "P3"], index=1)
        _due   = st.date_input("Due Date (optional)", value=None) if _type == "one-off" else None
        _sub   = st.form_submit_button("➕ Add Task", use_container_width=True)
    if _sub:
        if _title.strip():
            add_task({"title": _title.strip(), "description": _desc.strip(),
                      "type": _type, "priority": _pri,
                      "due_date": str(_due) if _due else None})
            st.rerun()
        else:
            st.warning("Title required.")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_texting, tab_zap, tab_onboarding, tab_tasks = st.tabs(["📞  Texting Audit", "🔍  Zap Audit", "🚀  Onboarding", "✅  Tasks"])

tab_texting.__enter__()

# ── Phone Audit ───────────────────────────────────────────────────────────────
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

        # ── Auto-Fix Section ──────────────────────────────────────────────
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

            # Track which record IDs have already been patched this session
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
                        # No test run yet — only offer the test button
                        if st.button(f"🧪 Test fix ({test_n} record(s))", key=f"test_fix_{base_id}"):
                            with st.spinner(f"Patching {test_n} record(s) in Airtable…"):
                                ok, errs = patch_phone_records(base_id, pending[:test_n])
                            for r in pending[:ok]:
                                st.session_state[applied_key].add(r["record_id"])
                            if errs:
                                st.warning(f"Fixed {ok}/{test_n}. ⚠️ {len(errs)} failed — try again.")
                            else:
                                st.success(f"✅ Test passed — {ok} record(s) fixed. "
                                           f"Check Airtable to confirm, then apply the rest below.")
                            st.rerun()
                    else:
                        # Test already ran — offer both another test batch and apply-all
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button(f"🧪 Test another {test_n} record(s)",
                                         key=f"test_fix_{base_id}"):
                                with st.spinner(f"Patching {test_n} record(s)…"):
                                    ok, errs = patch_phone_records(base_id, pending[:test_n])
                                for r in pending[:ok]:
                                    st.session_state[applied_key].add(r["record_id"])
                                if errs:
                                    st.warning(f"Fixed {ok}/{test_n}. ⚠️ {len(errs)} failed.")
                                else:
                                    st.success(f"✅ Fixed {ok} more. "
                                               f"{len(pending) - ok} remaining.")
                                st.rerun()
                        with col2:
                            if st.button(f"✅ Apply all {len(pending)} remaining",
                                         key=f"apply_all_{base_id}"):
                                with st.spinner(f"Patching {len(pending)} record(s)…"):
                                    ok, errs = patch_phone_records(base_id, pending)
                                for r in pending[:ok]:
                                    st.session_state[applied_key].add(r["record_id"])
                                if errs:
                                    st.warning(f"Fixed {ok}. ⚠️ {len(errs)} failed — re-run audit to retry.")
                                else:
                                    st.success(f"✅ All done! Fixed {ok} records in {base_name}.")
                                st.rerun()
            else:
                # Every fixable record has been patched
                st.success(f"✅ All {len(fixable_rows)} numbers in {base_name} are fixed!")
                if st.button("🔄 Re-run audit to confirm", key=f"clear_{base_id}"):
                    del st.session_state[f"phone_{base_id}"]
                    del st.session_state[f"phone_name_{base_id}"]
                    if applied_key in st.session_state:
                        del st.session_state[applied_key]
                    st.rerun()

        elif flagged > 0:
            st.info("ℹ️ No auto-fixable numbers found — all flagged records need manual review in Airtable.")
        # ─────────────────────────────────────────────────────────────────
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

# ── Message Audit ─────────────────────────────────────────────────────────────
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

        # ── Test filter ───────────────────────────────────────────────
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

        # ── Placeholder Breakdown ─────────────────────────────────────
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
        # ─────────────────────────────────────────────────────────────
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

st.markdown("""
<div style="text-align:center; padding: 32px 0 16px 0;">
    <img src="https://partingpro.com/wp-content/uploads/2024/07/partingpro-logo.png" style="height:22px; opacity:0.4;" />
    <div style="font-size:11px; color:#b0b8c8; margin-top:8px;">Aftercare Texting Audit Tool · Internal Use Only</div>
</div>
""", unsafe_allow_html=True)

tab_texting.__exit__(None, None, None)

# ════════════════════════════════════════════════════════════════════════════
# TAB 2 — Zap Audit
# ════════════════════════════════════════════════════════════════════════════
with tab_zap:
    # ── Trigger n8n run ───────────────────────────────────────────────────
    if run_zap_audit:
        with st.spinner("Triggering n8n audit workflow…"):
            try:
                resp = requests.post(N8N_WEBHOOK, json={}, timeout=300)
                if resp.ok:
                    st.success("✅ Audit run complete — results saved.")
                    st.rerun()
                else:
                    st.error(f"n8n returned {resp.status_code}: {resp.text[:200]}")
            except requests.exceptions.ConnectionError:
                st.error("Could not reach n8n at localhost:5678. Make sure n8n is running and the workflow is active.")
            except Exception as e:
                st.error(f"Error: {e}")

    # ── Load data ─────────────────────────────────────────────────────────
    runs = load_audit_runs()

    # ── Query Zaps List (always visible) ──────────────────────────────────
    st.markdown('<div class="section-wrap">', unsafe_allow_html=True)
    st.markdown('<div class="section-head"><div class="section-icon">🔎</div><div class="section-head-text"><h3>Query Zaps</h3><p>Active "Query Data to upload in Airtable" zaps and their latest audit results</p></div></div>', unsafe_allow_html=True)

    # Build lookup from latest audit run if available
    audit_lookup = {}
    if runs:
        for r in runs[0].get("results", []):
            audit_lookup[int(r.get("parting_pro_id", 0))] = r

    try:
        fh_records = fetch_query_zap_list()
        qz_rows = []
        for rec in fh_records:
            f = rec.get("fields", {})
            ppid = int(f.get("Parting Pro ID:", 0) or 0)
            if not ppid:
                continue
            fh_name = (f.get("Funeral Home Name:", "") or "").strip()
            go_live  = f.get("Go-Live Date", "") or "—"
            zap_title = f"{fh_name} - Query Data to upload in airtable - funeral_home_id = {ppid}"

            # Overlay audit result if available
            audit = audit_lookup.get(ppid)
            if audit:
                status   = audit.get("status", "")
                db_count = audit.get("db_row_count", 0)
                at_count = audit.get("airtable_record_count", 0)
                notes    = audit.get("notes", "") or "—"
                if status == "New":
                    result = "⏭️ Skipped (New FH)"
                elif db_count == 0:
                    result = "🟡 Filtered — no cases in window"
                elif db_count > 0 and at_count > 0:
                    result = "✅ Success"
                elif db_count > 0 and at_count == 0:
                    result = "🛑 Halted — cases not pushed"
                else:
                    result = "❌ Error"
                last_checked = runs[0].get("run_date", "—")
            else:
                status, db_count, at_count, notes = "—", "—", "—", "—"
                result = "⏳ Not yet audited"
                last_checked = "—"

            qz_rows.append({
                "Zap Title": zap_title,
                "Go-Live": go_live,
                "Query Result": result,
                "DB Cases (7–14d)": db_count,
                "Airtable Records (7d)": at_count,
                "Status": f"{STATUS_EMOJI.get(status, '')} {status}".strip(),
                "Notes": notes,
                "Last Checked": last_checked,
            })

        qz_df = pd.DataFrame(qz_rows)

        qz_c1, qz_c2 = st.columns([3, 2])
        with qz_c1:
            qz_search = st.text_input("Search", key="qz_always_search", placeholder="Search funeral home…")
        with qz_c2:
            qz_filter = st.multiselect(
                "Filter by result",
                options=["✅ Success", "🛑 Halted — cases not pushed", "🟡 Filtered — no cases in window", "⏳ Not yet audited", "⏭️ Skipped (New FH)", "❌ Error"],
                default=[],
                key="qz_always_filter"
            )

        qz_view = qz_df.copy()
        if qz_search:
            qz_view = qz_view[qz_view["Zap Title"].str.contains(qz_search, case=False, na=False)]
        if qz_filter:
            qz_view = qz_view[qz_view["Query Result"].isin(qz_filter)]

        st.dataframe(qz_view, use_container_width=True, hide_index=True)
        audited = sum(1 for r in qz_rows if r["Last Checked"] != "—")
        st.caption(f"{len(qz_view)} of {len(qz_df)} query zaps shown  ·  {audited} audited  ·  {len(qz_df) - audited} pending first run")

    except Exception as e:
        import traceback
        st.error(f"❌ Query Zaps error: {e}")
        st.code(traceback.format_exc())

    st.markdown('</div>', unsafe_allow_html=True)

    if not runs:
        st.info("No audit runs yet — click **Run Zap Audit Now** in the sidebar to populate results above.")
    else:
        latest = runs[0]
        summary = latest.get("summary", {})
        run_date = latest.get("run_date", "—")
        run_at = latest.get("run_at", "")
        df = latest_audit_df(runs)

        # ── Summary cards ─────────────────────────────────────────────────
        st.markdown(f"""
        <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:20px;">
            <div style="font-size:18px; font-weight:700; color:#1a2b4a;">Zap Health Report</div>
            <div style="font-size:12px; color:#4a5568; background:#f0f2f7; padding:6px 14px; border-radius:20px;">
                Last run: {run_date} &nbsp;·&nbsp; {summary.get('total', 0)} funeral homes
            </div>
        </div>
        <div class="metrics-row">
            <div class="metric green">
                <div class="m-label">🟢 Healthy</div>
                <div class="m-value">{summary.get('healthy', 0)}</div>
                <div class="m-sub">Zap working</div>
            </div>
            <div class="metric" style="border-left:3px solid #e07b39;">
                <div class="m-label">🟡 No Cases</div>
                <div class="m-value" style="color:#e07b39;">{summary.get('no_cases', 0)}</div>
                <div class="m-sub">Quiet window</div>
            </div>
            <div class="metric red">
                <div class="m-label">🔴 Missing Data</div>
                <div class="m-value">{summary.get('missing_data', 0)}</div>
                <div class="m-sub">Action needed</div>
            </div>
            <div class="metric" style="border-left:3px solid #4a5568;">
                <div class="m-label">⚫ Dead</div>
                <div class="m-value" style="color:#4a5568;">{summary.get('dead', 0)}</div>
                <div class="m-sub">Investigate</div>
            </div>
            <div class="metric blue">
                <div class="m-label">🔵 New</div>
                <div class="m-value">{summary.get('new_fh', 0)}</div>
                <div class="m-sub">Recently onboarded</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # ── Action required banner ────────────────────────────────────────
        flagged = df[df["Status"].isin(["Missing Data", "Dead"])]
        if not flagged.empty:
            items_html = "".join(
                f"<div style='padding:6px 0; border-bottom:1px solid rgba(255,255,255,0.1);'>"
                f"{STATUS_EMOJI.get(row['Status'], '')} <strong>{row['Funeral Home']}</strong>"
                f" &nbsp;<span style='opacity:0.7; font-size:12px;'>(ID: {row['ID']})</span>"
                f"{'  — ' + row['Notes'] if row['Notes'] else ''}</div>"
                for _, row in flagged.iterrows()
            )
            st.markdown(f"""
            <div style="background:#e05252; border-radius:12px; padding:20px 24px; margin-bottom:20px; color:white;">
                <div style="font-weight:700; font-size:15px; margin-bottom:12px;">⚠️ {len(flagged)} funeral home(s) need attention</div>
                {items_html}
            </div>
            """, unsafe_allow_html=True)

        # ── Monitored Zaps ────────────────────────────────────────────────
        st.markdown('<div class="section-wrap">', unsafe_allow_html=True)
        st.markdown('<div class="section-head"><div class="section-icon">⚡</div><div class="section-head-text"><h3>Monitored Zaps</h3><p>Zapier automations being tracked by this audit</p></div></div>', unsafe_allow_html=True)

        zap_rows = []
        for r in latest.get("results", []):
            status = r.get("status", "")
            db_count = r.get("db_row_count", 0)
            at_count = r.get("airtable_record_count", 0)

            # Derive query run result label
            if status == "New":
                query_result = "⏭️ Skipped (New FH)"
            elif db_count == 0 and at_count == 0:
                query_result = "🟡 No data found"
            elif db_count > 0 and at_count > 0:
                query_result = "✅ Success"
            elif db_count > 0 and at_count == 0:
                query_result = "❌ Failed — DB has cases, Airtable empty"
            elif db_count == 0 and at_count > 0:
                query_result = "✅ Success (Airtable only)"
            else:
                query_result = "⚠️ Unknown"

            zap_rows.append({
                "Funeral Home": r.get("funeral_home_name", ""),
                "Parting Pro ID": r.get("parting_pro_id", ""),
                "Go-Live Date": r.get("go_live_date", "") or "—",
                "Query Result": query_result,
                "DB Cases (7–14d)": db_count,
                "Airtable Records (7d)": at_count,
                "Status": f"{STATUS_EMOJI.get(status, '')} {status}",
                "Notes": r.get("notes", "") or "—",
                "Last Checked": run_date,
            })
        zap_df = pd.DataFrame(zap_rows)

        zap_col1, zap_col2 = st.columns([3, 2])
        with zap_col1:
            zap_search = st.text_input("Search", key="monitored_zap_search", placeholder="Search funeral home…")
        with zap_col2:
            success_count = sum(1 for r in zap_rows if "✅" in r["Query Result"])
            fail_count = sum(1 for r in zap_rows if "❌" in r["Query Result"])
            skip_count = sum(1 for r in zap_rows if "⏭️" in r["Query Result"])
            st.markdown(f"""
            <div style="margin-top:28px; font-size:13px; color:#4a5568;">
                <strong>{len(zap_df)}</strong> monitored &nbsp;·&nbsp;
                ✅ {success_count} success &nbsp;·&nbsp;
                ❌ {fail_count} failed &nbsp;·&nbsp;
                ⏭️ {skip_count} skipped
            </div>
            """, unsafe_allow_html=True)

        if zap_search:
            zap_df = zap_df[zap_df["Funeral Home"].str.contains(zap_search, case=False, na=False)]

        st.dataframe(zap_df, use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── Query Zaps — Zap-by-Zap Style ────────────────────────────────
        st.markdown('<div class="section-wrap">', unsafe_allow_html=True)
        st.markdown('<div class="section-head"><div class="section-icon">⚡</div><div class="section-head-text"><h3>Query Zaps — Zap-by-Zap Breakdown</h3><p>Only "Query Data to upload in Airtable" zaps · stats across all audit runs</p></div></div>', unsafe_allow_html=True)

        # Aggregate stats across ALL historical runs per FH
        qzb_stats = {}  # ppid → {title, total, success, halted, filtered, errors}
        for run in runs:
            for r in run.get("results", []):
                ppid     = r.get("parting_pro_id", 0)
                fh_name  = r.get("funeral_home_name", "")
                db_count = r.get("db_row_count", 0)
                at_count = r.get("airtable_record_count", 0)
                status   = r.get("status", "")

                if ppid not in qzb_stats:
                    qzb_stats[ppid] = {
                        "Zap Title": f"{fh_name} - Query Data to upload in airtable - funeral_home_id = {ppid}",
                        "Total Runs": 0, "Success": 0, "Errors": 0,
                        "Halted": 0, "Filtered": 0, "Throttled": 0,
                    }

                s = qzb_stats[ppid]
                s["Total Runs"] += 1
                if status == "New":
                    pass  # don't count skipped runs
                elif db_count == 0:
                    s["Filtered"] += 1
                elif db_count > 0 and at_count > 0:
                    s["Success"] += 1
                elif db_count > 0 and at_count == 0:
                    s["Halted"] += 1
                else:
                    s["Errors"] += 1

        qzb_rows = []
        for ppid, s in sorted(qzb_stats.items(), key=lambda x: x[1]["Zap Title"]):
            total = s["Total Runs"]
            succ  = s["Success"]
            err   = s["Errors"]
            error_rate   = f"{(err / total * 100):.1f}%" if total > 0 else "0.0%"
            success_rate = f"{(succ / total * 100):.1f}%" if total > 0 else "0.0%"
            qzb_rows.append({
                "Zap Title":    s["Zap Title"],
                "Total Runs":   total,
                "Success":      succ,
                "Errors":       err,
                "Halted":       s["Halted"],
                "Filtered":     s["Filtered"],
                "Throttled":    s["Throttled"],
                "Error Rate":   error_rate,
                "Success Rate": success_rate,
            })

        qzb_df = pd.DataFrame(qzb_rows)

        qzb_search = st.text_input("Search zap", key="qzb_search", placeholder="Search funeral home…")
        if qzb_search:
            qzb_df = qzb_df[qzb_df["Zap Title"].str.contains(qzb_search, case=False, na=False)]

        st.dataframe(qzb_df, use_container_width=True, hide_index=True)
        st.caption(
            f"{len(qzb_df)} query zaps  ·  "
            f"✅ {sum(s['Success'] for s in qzb_stats.values())} success  ·  "
            f"🛑 {sum(s['Halted'] for s in qzb_stats.values())} halted  ·  "
            f"🟡 {sum(s['Filtered'] for s in qzb_stats.values())} filtered  ·  "
            f"❌ {sum(s['Errors'] for s in qzb_stats.values())} errors"
        )
        st.markdown('</div>', unsafe_allow_html=True)

        # ── Full results table ────────────────────────────────────────────
        st.markdown('<div class="section-wrap">', unsafe_allow_html=True)
        st.markdown('<div class="section-head"><div class="section-icon">📋</div><div class="section-head-text"><h3>All Funeral Homes</h3><p>Filter by status to focus on what needs attention</p></div></div>', unsafe_allow_html=True)

        col_filter, col_search = st.columns([2, 3])
        with col_filter:
            status_filter = st.multiselect(
                "Filter by status",
                options=["Healthy", "No Cases", "Missing Data", "Dead", "New"],
                default=["Missing Data", "Dead"],
                key="zap_status_filter"
            )
        with col_search:
            search = st.text_input("Search funeral home", key="zap_search", placeholder="Type to search…")

        filtered = df.copy()
        if status_filter:
            filtered = filtered[filtered["Status"].isin(status_filter)]
        if search:
            filtered = filtered[filtered["Funeral Home"].str.contains(search, case=False, na=False)]

        def _color_status(val):
            color = STATUS_COLOR.get(val, "#4a5568")
            return f"color: {color}; font-weight: 600;"

        if not filtered.empty:
            st.dataframe(
                filtered.style.applymap(_color_status, subset=["Status"]),
                use_container_width=True,
                hide_index=True
            )
            st.caption(f"{len(filtered)} of {len(df)} funeral homes shown")
        else:
            st.info("No records match the current filter.")

        st.markdown('</div>', unsafe_allow_html=True)

        # ── History (if multiple runs) ────────────────────────────────────
        if len(runs) > 1:
            st.markdown('<div class="section-wrap">', unsafe_allow_html=True)
            st.markdown('<div class="section-head"><div class="section-icon">📈</div><div class="section-head-text"><h3>Audit History</h3><p>Status counts over time</p></div></div>', unsafe_allow_html=True)

            history_rows = []
            for run in runs[:30]:
                s = run.get("summary", {})
                history_rows.append({
                    "Date": run.get("run_date", ""),
                    "Healthy": s.get("healthy", 0),
                    "No Cases": s.get("no_cases", 0),
                    "Missing Data": s.get("missing_data", 0),
                    "Dead": s.get("dead", 0),
                    "New": s.get("new_fh", 0),
                })
            hist_df = pd.DataFrame(history_rows).set_index("Date")
            st.line_chart(hist_df[["Healthy", "Missing Data", "Dead"]])
            st.markdown('</div>', unsafe_allow_html=True)

        # ── Download ─────────────────────────────────────────────────────
        csv = df.to_csv(index=False)
        st.download_button(
            "⬇️ Download Audit Report (.csv)",
            csv, f"zap_audit_{run_date}.csv", mime="text/csv"
        )

# ── Onboarding Tab ────────────────────────────────────────────────────────────
tab_onboarding.__enter__()

st.markdown("""
<div class="section-wrap">
    <div class="section-head">
        <div class="section-icon">🚀</div>
        <div class="section-head-text">
            <h3>Funeral Home Onboarding Automation</h3>
            <p>Run automated onboarding workflows in the cloud</p>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# Initialize session state for onboarding
if "onboarding" not in st.session_state:
    st.session_state.onboarding = None
if "onboarding_step" not in st.session_state:
    st.session_state.onboarding_step = None
if "onboarding_output" not in st.session_state:
    st.session_state.onboarding_output = []
if "onboarding_input" not in st.session_state:
    st.session_state.onboarding_input = ""

col_step, col_action = st.columns([3, 1])

with col_step:
    selected_step = st.selectbox(
        "Select Onboarding Step",
        options=[f"{s['emoji']} Step {s['key']} – {s['title']}" for s in STEPS],
        help="Choose which onboarding step to run"
    )
    step_key = selected_step.split(" ")[1]

with col_action:
    st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
    if st.button("▶️ Start Step", use_container_width=True):
        st.session_state.onboarding = OnboardingAutomation()
        st.session_state.onboarding_step = step_key
        st.session_state.onboarding_output = []
        try:
            with st.spinner(f"Starting Step {step_key}..."):
                st.session_state.onboarding.start_step(step_key)
            st.success(f"✅ Step {step_key} started! Follow the prompts below.")
            st.rerun()
        except Exception as e:
            st.error(f"❌ Failed to start onboarding: {str(e)}")
            st.session_state.onboarding = None

st.markdown("---")

# Display onboarding process
if st.session_state.onboarding and st.session_state.onboarding.is_running():
    st.markdown("""
    <div class="section-wrap">
        <div class="section-head">
            <div class="section-icon">💬</div>
            <div class="section-head-text">
                <h3>Step Activity</h3>
                <p>Real-time output from the onboarding process</p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    output_container = st.container()
    input_container = st.container()

    # Poll for new output
    while True:
        msg = st.session_state.onboarding.get_output()
        if msg is None:
            break

        if msg.get("t") == "log":
            st.session_state.onboarding_output.append(("log", msg.get("m", "")))
        elif msg.get("t") == "ask":
            st.session_state.onboarding_output.append(("ask", msg.get("q", "")))
        elif msg.get("t") == "done":
            st.session_state.onboarding_output.append(("done", "✅ Step completed!"))
            st.session_state.onboarding = None
        elif msg.get("t") == "error":
            st.session_state.onboarding_output.append(("error", msg.get("m", "Unknown error")))
            st.session_state.onboarding = None

    # Display all output
    with output_container:
        for msg_type, content in st.session_state.onboarding_output:
            if msg_type == "log":
                st.markdown(f"```\n{content}\n```")
            elif msg_type == "ask":
                st.info(f"❓ {content}")
            elif msg_type == "done":
                st.success(content)
            elif msg_type == "error":
                st.error(f"❌ {content}")

    # Input field for answers
    with input_container:
        if st.session_state.onboarding and st.session_state.onboarding.is_running():
            # Check if we're waiting for input
            last_msg = st.session_state.onboarding_output[-1] if st.session_state.onboarding_output else None
            if last_msg and last_msg[0] == "ask":
                user_input = st.text_input("Your response:", key="onboarding_response")
                if st.button("Send"):
                    if user_input.strip():
                        try:
                            st.session_state.onboarding.send_answer(user_input)
                            st.session_state.onboarding_input = ""
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to send answer: {str(e)}")
elif st.session_state.onboarding_output:
    st.markdown("""
    <div class="section-wrap">
        <div class="section-head">
            <div class="section-icon">✅</div>
            <div class="section-head-text">
                <h3>Step Complete</h3>
                <p>Process finished successfully</p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.info("🎉 The onboarding step has been completed. You can start another step above or review the output below.")

    st.markdown("**Process Output:**")
    for msg_type, content in st.session_state.onboarding_output:
        if msg_type == "log":
            st.markdown(f"```\n{content}\n```")
        elif msg_type == "ask":
            st.info(f"❓ {content}")
        elif msg_type == "done":
            st.success(content)
        elif msg_type == "error":
            st.error(f"❌ {content}")

    if st.button("Clear Output", use_container_width=True):
        st.session_state.onboarding_output = []
        st.rerun()
else:
    st.info("👋 Select a step above and click 'Start Step' to begin onboarding. Each step will guide you through the process with interactive prompts.")

    st.markdown("---")
    st.markdown("**Available Steps:**")
    for step in STEPS:
        st.markdown(f"**{step['emoji']} Step {step['key']}: {step['title']}**")
        st.caption(step['description'])

tab_onboarding.__exit__(None, None, None)

# ════════════════════════════════════════════════════════════════════════════
# TAB 4 — Task Tracker
# ════════════════════════════════════════════════════════════════════════════

# ── Task Tracker helpers ──────────────────────────────────────────────────────

def _priority_pill(p: str) -> str:
    icons = {"P1": "🔴", "P2": "🟠", "P3": "⚪"}
    cls   = {"P1": "pill-p1", "P2": "pill-p2", "P3": "pill-p3"}
    return f'<span class="{cls.get(p,"pill-p3")}">{icons.get(p,"")} {p}</span>'


def _is_overdue(task: dict) -> bool:
    if task.get("status") == "done":
        return False
    if task.get("type") != "one-off":
        return False
    due = task.get("due_date")
    if not due:
        return False
    try:
        return date.fromisoformat(str(due)) < date.today()
    except ValueError:
        return False


def _due_label(task: dict) -> str:
    due = task.get("due_date")
    if not due:
        return ""
    cls  = "overdue" if _is_overdue(task) else "due-ok"
    flag = " ⚠️" if _is_overdue(task) else ""
    return f'<span class="{cls}" style="font-size:12px;">📅 {due}{flag}</span>'


def _render_task_row(task: dict):
    tid     = task["id"]
    is_done = task.get("status") == "done"

    col_chk, col_info, col_type, col_due, col_edit, col_del = st.columns(
        [0.04, 0.52, 0.1, 0.18, 0.08, 0.08]
    )
    with col_chk:
        checked = st.checkbox("", value=is_done, key=f"chk_{tid}", label_visibility="collapsed")
        if checked != is_done:
            update_task(tid, {"status": "done" if checked else "todo"})
            st.session_state.editing_task_id  = None
            st.session_state.deleting_task_id = None
            st.rerun()

    with col_info:
        title_cls = "task-title-done" if is_done else "task-title"
        desc_html = (f'<div class="task-desc">{task["description"]}</div>'
                     if task.get("description") else "")
        st.markdown(
            f'<div class="{title_cls}">'
            f'{_priority_pill(task.get("priority","P3"))} {task["title"]}'
            f'</div>{desc_html}',
            unsafe_allow_html=True,
        )

    with col_type:
        st.markdown(
            f'<div style="margin-top:6px;"><span class="type-badge">{task.get("type","one-off")}</span></div>',
            unsafe_allow_html=True,
        )

    with col_due:
        lbl = _due_label(task)
        if lbl:
            st.markdown(f'<div style="margin-top:8px;">{lbl}</div>', unsafe_allow_html=True)

    with col_edit:
        editing_this = st.session_state.editing_task_id == tid
        if st.button("✖️" if editing_this else "✏️", key=f"edit_btn_{tid}", help="Edit"):
            st.session_state.editing_task_id  = None if editing_this else tid
            st.session_state.deleting_task_id = None
            st.rerun()

    with col_del:
        deleting_this = st.session_state.deleting_task_id == tid
        if st.button("✖️" if deleting_this else "🗑️", key=f"del_btn_{tid}", help="Delete"):
            st.session_state.deleting_task_id = None if deleting_this else tid
            st.session_state.editing_task_id  = None
            st.rerun()

    # Inline edit form
    if st.session_state.editing_task_id == tid:
        with st.form(key=f"edit_form_{tid}"):
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

    # Inline delete confirmation
    if st.session_state.deleting_task_id == tid:
        st.warning(f'Delete **"{task["title"]}"**? This cannot be undone.')
        dc, ac = st.columns(2)
        with dc:
            if st.button("🗑️ Confirm", key=f"confirm_del_{tid}", use_container_width=True):
                delete_task(tid)
                st.session_state.deleting_task_id = None
                st.rerun()
        with ac:
            if st.button("Cancel", key=f"abort_del_{tid}", use_container_width=True):
                st.session_state.deleting_task_id = None
                st.rerun()

    st.markdown("<hr style='margin:4px 0; border-color:#f0f2f7;'>", unsafe_allow_html=True)


def _render_task_tab(filter_type: str, all_tasks: list):
    filtered = all_tasks if filter_type == "all" else [
        t for t in all_tasks if t.get("type") == filter_type
    ]
    if not filtered:
        st.markdown(
            "<div style='padding:32px 0; text-align:center; color:#9aa5b4; font-size:14px;'>"
            "No tasks yet — add one using the sidebar form.</div>",
            unsafe_allow_html=True,
        )
        return

    pri_ord    = {"P1": 0, "P2": 1, "P3": 2}
    status_ord = {"todo": 0, "in-progress": 1, "done": 2}
    filtered   = sorted(
        filtered,
        key=lambda t: (status_ord.get(t.get("status","todo"), 0),
                       pri_ord.get(t.get("priority","P3"), 2)),
    )

    n_done    = sum(1 for t in filtered if t.get("status") == "done")
    n_overdue = sum(1 for t in filtered if _is_overdue(t))
    ov_badge  = (f' &nbsp;<span style="color:#e05252;font-weight:600;">⚠️ {n_overdue} overdue</span>'
                 if n_overdue else "")
    st.markdown(
        f'<div style="font-size:13px;color:#4a5568;margin-bottom:12px;padding-bottom:8px;'
        f'border-bottom:1px solid #e4e7ef;">'
        f'<strong style="color:#1a2b4a;">{len(filtered)}</strong> tasks &nbsp;·&nbsp; '
        f'<span style="color:#1a9e5c;font-weight:600;">✅ {n_done} done</span>{ov_badge}'
        f'</div>',
        unsafe_allow_html=True,
    )
    for t in filtered:
        _render_task_row(t)


with tab_tasks:
    tasks = load_tasks()
    today_str = date.today().isoformat()

    # ── Summary metrics ───────────────────────────────────────────────────
    active_tasks  = [t for t in tasks if t.get("status") != "done"]
    done_today    = [t for t in tasks if t.get("status") == "done"
                     and t.get("completed_at","")[:10] == today_str]
    overdue_tasks = [t for t in tasks if _is_overdue(t)]
    p1_open       = [t for t in tasks if t.get("priority") == "P1" and t.get("status") != "done"]

    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("Total Active", len(active_tasks),  help="All non-done tasks")
    mc2.metric("Done Today",   len(done_today),     help="Completed today")
    mc3.metric("Overdue",      len(overdue_tasks),  help="One-off tasks past due date")
    mc4.metric("P1 Items",     len(p1_open),        help="High-priority open tasks")

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    # ── Quick Capture ─────────────────────────────────────────────────────
    _qv = st.text_input("⚡ Quick add a task…", placeholder="Type and press Enter",
                        key="quick_capture", label_visibility="collapsed")
    if _qv and _qv != st.session_state.get("_last_quick", ""):
        st.session_state["_last_quick"] = _qv
        add_task({"title": _qv.strip(), "type": "one-off", "priority": "P2"})
        st.rerun()

    # ── Task Board tabs ───────────────────────────────────────────────────
    tb_all, tb_daily, tb_weekly, tb_monthly, tb_oneoff = st.tabs(
        ["All", "Daily", "Weekly", "Monthly", "One-Off"]
    )
    with tb_all:
        st.markdown('<div class="section-wrap">', unsafe_allow_html=True)
        _render_task_tab("all", tasks)
        st.markdown("</div>", unsafe_allow_html=True)
    with tb_daily:
        st.markdown('<div class="section-wrap">', unsafe_allow_html=True)
        _render_task_tab("daily", tasks)
        st.markdown("</div>", unsafe_allow_html=True)
    with tb_weekly:
        st.markdown('<div class="section-wrap">', unsafe_allow_html=True)
        _render_task_tab("weekly", tasks)
        st.markdown("</div>", unsafe_allow_html=True)
    with tb_monthly:
        st.markdown('<div class="section-wrap">', unsafe_allow_html=True)
        _render_task_tab("monthly", tasks)
        st.markdown("</div>", unsafe_allow_html=True)
    with tb_oneoff:
        st.markdown('<div class="section-wrap">', unsafe_allow_html=True)
        _render_task_tab("one-off", tasks)
        st.markdown("</div>", unsafe_allow_html=True)