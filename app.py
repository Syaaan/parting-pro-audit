import re
import io
import json
import time
import requests
import openpyxl
import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import date, datetime, timedelta, timezone
from collections import Counter
from openpyxl.styles import Font, PatternFill, Alignment
from onboarding_wrapper import OnboardingAutomation, STEPS
from task_store import (
    load_tasks, add_task, update_task, delete_task, reset_recurring_tasks,
    load_members, add_member, update_member, delete_member,
)

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN = "patm2acj3jyDwBfyD.3fb175e7596542e2a9be3acc07700272cf8cb09028c58cc03a6d8bc5be022542"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
BASE_IDS = ["appbXFzZnhij88tnQ", "appXT2xJZ1zgll4fG"]

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

def fetch_records(base_id, table, fields, filter_formula=None, cell_format=None):
    records, offset = [], None
    url = f"https://api.airtable.com/v0/{base_id}/{requests.utils.quote(table)}"
    while True:
        params = {"pageSize": 100, "fields[]": fields}
        if offset:
            params["offset"] = offset
        if filter_formula:
            params["filterByFormula"] = filter_formula
        if cell_format:
            params["cellFormat"] = cell_format
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
        filter_formula='FIND("outbound", LOWER({Direction})) > 0',
        cell_format="string"
    )
    rows = []
    for rec in records:
        f = rec.get("fields", {})
        content = f.get("Message Content", "")
        name_raw = f.get("Contact Full Name: (from Contact Cell)", "")
        name = name_raw[0] if isinstance(name_raw, list) and name_raw else str(name_raw) if name_raw else "(unknown)"
        fh_raw = f.get("Funeral Home: (from Contact Cell)", "")
        fh = fh_raw if isinstance(fh_raw, str) and fh_raw else (fh_raw[0] if isinstance(fh_raw, list) and fh_raw else "(unknown)")
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

# ── Dark-mode session state (read at top so CSS picks it up on every rerun) ──
if "dark_mode" not in st.session_state:
    st.session_state["dark_mode"] = False
_DARK = bool(st.session_state["dark_mode"])

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

/* Hero text must stay white — earlier `color: inherit` was leaking dark body color through */
.hero, .hero h1, .hero h2, .hero h3, .hero p, .hero div, .hero span { color: #ffffff !important; }
.hero .hero-subtitle { color: rgba(255,255,255,0.75) !important; }
.hero .hero-badge { color: rgba(255,255,255,0.9) !important; }
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

# ── Dark-mode CSS overrides ───────────────────────────────────────────────────
# Injected only when the sidebar toggle is on. We override the light theme
# rather than rewriting it so the light theme stays the default and unchanged.
if _DARK:
    st.markdown("""
    <style>
    /* Page background + global text */
    .stApp { background: #0d1117 !important; }
    section[data-testid="stMain"] { background: #0d1117 !important; }

    /* Cards, sections, panels — dark surface */
    .card, .section-wrap, .metric, .empty {
        background: #161b22 !important;
        border-color: #2a3142 !important;
        box-shadow: 0 1px 6px rgba(0,0,0,0.4) !important;
    }
    .section-head { border-bottom-color: #2a3142 !important; }
    hr { border-color: #2a3142 !important; }

    /* Primary text — flip dark navy to soft light gray */
    section[data-testid="stMain"] p,
    section[data-testid="stMain"] span,
    section[data-testid="stMain"] li,
    section[data-testid="stMain"] strong,
    section[data-testid="stMain"] em,
    section[data-testid="stMain"] h1,
    section[data-testid="stMain"] h2,
    section[data-testid="stMain"] h3,
    section[data-testid="stMain"] h4,
    section[data-testid="stMain"] h5,
    .card-title, .section-head-text h3, .metric .m-value,
    .task-title {
        color: #e4e7ef !important;
    }
    .section-head-text p, .metric .m-label, .metric .m-sub,
    .task-desc, .due-ok {
        color: #9aa5b4 !important;
    }

    /* Hero still has its own background gradient — keep text crisp white */
    .hero, .hero * { color: #ffffff !important; }
    .hero .hero-subtitle { color: rgba(255,255,255,0.75) !important; }

    /* Inputs / selects / textareas */
    section[data-testid="stMain"] input,
    section[data-testid="stMain"] textarea,
    section[data-testid="stMain"] select,
    section[data-testid="stMain"] .stNumberInput input,
    section[data-testid="stMain"] .stDateInput input,
    section[data-testid="stMain"] [data-baseweb="input"] input,
    section[data-testid="stMain"] [data-baseweb="select"] > div {
        background: #161b22 !important;
        color: #e4e7ef !important;
        border-color: #2a3142 !important;
    }

    /* Tab bar */
    section[data-testid="stMain"] [data-baseweb="tab-list"] {
        background: #0d1117 !important;
        border-bottom-color: #2a3142 !important;
    }
    section[data-testid="stMain"] [data-baseweb="tab"] { color: #9aa5b4 !important; }
    section[data-testid="stMain"] [data-baseweb="tab"][aria-selected="true"] {
        color: #e4e7ef !important;
    }

    /* Expanders */
    section[data-testid="stMain"] [data-testid="stExpander"] {
        background: #161b22 !important;
        border: 1px solid #2a3142 !important;
        border-radius: 8px !important;
    }
    section[data-testid="stMain"] [data-testid="stExpander"] summary { color: #e4e7ef !important; }

    /* Code / pre blocks */
    section[data-testid="stMain"] code,
    section[data-testid="stMain"] pre {
        background: #0d1117 !important;
        color: #e4e7ef !important;
        border: 1px solid #2a3142 !important;
    }

    /* Dataframes */
    section[data-testid="stMain"] .stDataFrame { background: #161b22 !important; }
    section[data-testid="stMain"] [data-testid="stDataFrame"] * { color: #e4e7ef !important; }

    /* Alerts — keep their semantic background but darken slightly */
    section[data-testid="stMain"] [data-testid="stAlert"] {
        background: #161b22 !important;
        border-left-width: 3px !important;
    }

    /* Sidebar already dark — just deepen it a touch */
    section[data-testid="stSidebar"] { background: #0a0e14 !important; }

    /* Buttons in the main area */
    section[data-testid="stMain"] .stButton > button {
        background: #1f2937 !important;
        color: #e4e7ef !important;
        border-color: #2a3142 !important;
    }
    section[data-testid="stMain"] .stButton > button:hover {
        background: #2a3441 !important;
        border-color: #3b5bdb !important;
    }

    /* Pills + badges — invert the light pastel backgrounds to fit dark mode */
    .pill-p3 { background: #1f2937 !important; color: #c8cdd8 !important; border-color: #2a3142 !important; }
    .type-badge { background: #1c2a4a !important; color: #93b3ff !important; }
    .base-tag { background: #1c2a4a !important; color: #93b3ff !important; }
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
    # Dark-mode toggle — bound to session_state["dark_mode"] via key; flip triggers rerun
    st.toggle("🌙  Dark mode", key="dark_mode", help="Switch the main content to a dark theme")
    st.markdown("---")
    st.markdown("<div style='font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; opacity:0.5; margin-bottom:8px;'>Connected Bases</div>", unsafe_allow_html=True)
    for b in BASE_IDS:
        st.markdown(f"<div style='font-size:12px; opacity:0.7; padding: 4px 0;'>• {b}</div>", unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("<div style='font-size:11px; opacity:0.4; text-align:center;'>Parting Pro Internal · 2025</div>", unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("<div style='font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; opacity:0.5; margin-bottom:12px;'>Add Task</div>", unsafe_allow_html=True)
    _members_for_form = [m for m in load_members() if m.get("active")]
    _name_to_id = {m["name"]: m["id"] for m in _members_for_form}
    with st.form("sidebar_add_task", clear_on_submit=True):
        _title = st.text_input("Title *", placeholder="What needs to be done?")
        _desc  = st.text_area("Description", placeholder="Optional…", height=60)
        _type  = st.selectbox("Type", ["daily", "weekly", "monthly", "one-off"])
        _pri   = st.selectbox("Priority", ["P1", "P2", "P3"], index=1)
        _due   = st.date_input("Due Date (optional)", value=None) if _type == "one-off" else None
        _assignees = st.multiselect("Assign to", options=list(_name_to_id.keys()),
                                    help="Pick from active team members. Manage the roster from the Tasks tab.")
        _sub   = st.form_submit_button("➕ Add Task", use_container_width=True)
    if _sub:
        if _title.strip():
            add_task({"title": _title.strip(), "description": _desc.strip(),
                      "type": _type, "priority": _pri,
                      "due_date": str(_due) if _due else None,
                      "assignee_ids": [_name_to_id[n] for n in _assignees]})
            st.rerun()
        else:
            st.warning("Title required.")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_texting, tab_zap, tab_onboarding, tab_tasks = st.tabs(["📞  Texting Audit", "🔍  Zap Audit", "🚀  Onboarding", "✅  Tasks"])

tab_texting.__enter__()

# ── Audit Controls (Texting tab) ──────────────────────────────────────────────
_phone_col, _msg_col = st.columns(2)
with _phone_col:
    run_phones = st.button("📞  Run Phone Audit", use_container_width=True, key="run_phones_btn")
with _msg_col:
    run_messages = st.button("💬  Run Message Audit", use_container_width=True, key="run_messages_btn")
st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

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
# TAB 2 — Zap Audit  (live dashboard — reads Airtable Zap Run Log)
# ════════════════════════════════════════════════════════════════════════════
# Architecture:
#   Each monitored zap → POST to a Webhooks-by-Zapier catch hook → master
#   Logger zap → Create Record in this Airtable base. We poll Airtable every
#   10s and render a live status board.
# ────────────────────────────────────────────────────────────────────────────

# Zap Audit base. Token must be set in Streamlit Cloud secrets (Settings → Secrets):
#     zap_audit_token = "pat..."
# Locally, you can put the same line in .streamlit/secrets.toml (gitignored).
ZAP_AUDIT_BASE_ID  = "appq10XQm3AKQYyYr"
ZAP_AUDIT_TABLE_ID = "tbleFE2RpNXq1s3S4"
try:
    ZAP_AUDIT_TOKEN = st.secrets["zap_audit_token"]
except Exception:
    ZAP_AUDIT_TOKEN = ""  # missing-secret message rendered inside the tab

ZAP_STATUS_META = {
    "success":   ("✅", "#1a9e5c"),
    "error":     ("❌", "#e05252"),
    "halted":    ("🛑", "#e07b39"),
    "held":      ("⏸",  "#e0b939"),
    "filtered":  ("🚫", "#6b7a94"),
    "delayed":   ("⏱",  "#3b7de8"),
    "throttled": ("🐢", "#9b59b6"),
    "pending":   ("⏳", "#6b7a94"),
    "stopped":   ("💤", "#8b0000"),
}

ZAP_WINDOW_OPTIONS = {
    "Last 1 hour":   timedelta(hours=1),
    "Last 6 hours":  timedelta(hours=6),
    "Last 24 hours": timedelta(hours=24),
    "Last 7 days":   timedelta(days=7),
}

def _zap_parse_ts(s):
    """Parse Airtable ISO timestamp ('2026-05-22T15:30:00.000Z') to aware datetime."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None

@st.cache_data(ttl=5, show_spinner=False)
def fetch_zap_runs(limit: int = 500):
    """Fetch the most recent N runs from Airtable, newest first."""
    url = f"https://api.airtable.com/v0/{ZAP_AUDIT_BASE_ID}/{ZAP_AUDIT_TABLE_ID}"
    headers = {"Authorization": f"Bearer {ZAP_AUDIT_TOKEN}"}
    base_params = {
        "pageSize": 100,
        "sort[0][field]": "Timestamp",
        "sort[0][direction]": "desc",
    }
    runs = []
    offset = None
    safety = 0
    while len(runs) < limit and safety < 20:
        safety += 1
        params = dict(base_params)
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        for rec in data.get("records", []):
            f = rec.get("fields", {})
            status = f.get("Status", "")
            if isinstance(status, dict):  # singleSelect returned as object
                status = status.get("name", "")
            runs.append({
                "id":          rec.get("id"),
                "run_id":      f.get("Run ID", ""),
                "zap_name":    f.get("Zap Name", "") or "(unnamed zap)",
                "zap_id":      f.get("Zap ID", ""),
                "status":      str(status).lower(),
                "timestamp":   f.get("Timestamp"),
                "step":        f.get("Step (if error)", ""),
                "error":       f.get("Error Message", ""),
                "duration_ms": f.get("Duration (ms)", 0) or 0,
                "task_count":  f.get("Task Count", 0) or 0,
                "source":      f.get("Logger Source", ""),
            })
        offset = data.get("offset")
        if not offset:
            break
    return runs


with tab_zap:
    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown('''
    <div class="section-wrap">
      <div class="section-head">
        <div class="section-icon">⚡</div>
        <div class="section-head-text">
          <h3>Zap Audit — Live</h3>
          <p>Every zap run logs to Airtable; this dashboard reads it back in real time. No cookies, no polling Zapier.</p>
        </div>
      </div>
    </div>
    ''', unsafe_allow_html=True)

    # ── Controls ─────────────────────────────────────────────────────────────
    _ctrl_l, _ctrl_r = st.columns([3, 1])
    _zap_window_label = _ctrl_l.selectbox(
        "Time window",
        list(ZAP_WINDOW_OPTIONS.keys()),
        index=2,  # default: Last 24 hours
        key="zap_window_select",
    )
    if _ctrl_r.button("🔄 Refresh now", use_container_width=True, key="zap_refresh_btn"):
        fetch_zap_runs.clear()
        st.rerun()

    # ── Live dashboard — rendered inline (use Refresh button to update) ──────
    def _render_zap_dashboard():
        if not ZAP_AUDIT_TOKEN:
            st.warning(
                "**Zap Audit token not configured.**  \n"
                "Add this line to your Streamlit Cloud secrets (Settings → Secrets):  \n\n"
                "`zap_audit_token = \"pat...\"`  \n\n"
                "Locally, add the same line to `.streamlit/secrets.toml` (already gitignored)."
            )
            return
        try:
            runs = fetch_zap_runs(limit=500)
        except requests.HTTPError as ex:
            code = ex.response.status_code if ex.response is not None else "?"
            st.error(f"Couldn't read Zap Run Log (HTTP {code}). Check that the PAT has access to base {ZAP_AUDIT_BASE_ID}.")
            return
        except Exception as ex:
            st.error(f"Couldn't read Zap Run Log: {ex}")
            return

        if not runs:
            st.info(
                "No zap runs logged yet. Once a monitored zap fires its webhook step, "
                "rows will appear here within a few seconds."
            )
            return

        # Filter to selected window
        now = datetime.now(timezone.utc)
        cutoff = now - ZAP_WINDOW_OPTIONS[_zap_window_label]
        in_window = []
        for r in runs:
            t = _zap_parse_ts(r["timestamp"])
            if t and t >= cutoff:
                rr = dict(r)
                rr["_t"] = t
                in_window.append(rr)

        # ── Live header ────────────────────────────────────────────────────
        st.markdown(
            f"<div style='font-size:12px; color:#6b7a94; margin: 4px 0 12px;'>"
            f"● <strong style='color:#1a9e5c;'>LIVE</strong> · "
            f"{len(in_window)} run(s) in window · "
            f"refreshed {datetime.now().strftime('%H:%M:%S')}"
            f"</div>",
            unsafe_allow_html=True,
        )

        if not in_window:
            st.caption(f"No zap runs in {_zap_window_label.lower()}. (Total in Airtable: {len(runs)})")
            return

        # ── Status summary cards ──────────────────────────────────────────
        counts = {}
        for r in in_window:
            counts[r["status"]] = counts.get(r["status"], 0) + 1

        # First row: Total + the 4 most common statuses
        primary_statuses = ["success", "error", "halted", "held"]
        cols = st.columns(5)
        cols[0].metric("Total runs", len(in_window))
        for i, status in enumerate(primary_statuses, 1):
            icon = ZAP_STATUS_META.get(status, ("·",))[0]
            cols[i].metric(f"{icon} {status.title()}", counts.get(status, 0))

        # Second row: other statuses, only if present
        secondary = [s for s in ("filtered", "delayed", "throttled", "pending", "stopped") if counts.get(s, 0) > 0]
        if secondary:
            cols2 = st.columns(len(secondary))
            for i, status in enumerate(secondary):
                icon = ZAP_STATUS_META.get(status, ("·",))[0]
                cols2[i].metric(f"{icon} {status.title()}", counts.get(status, 0))

        # ── Flagged section ──────────────────────────────────────────────
        flagged = {}
        for r in in_window:
            if r["status"] == "error":
                flagged.setdefault(r["zap_name"], []).append(r)
        if flagged:
            st.markdown("#### 🚩 Needs attention")
            for zap_name, errors in sorted(flagged.items(), key=lambda x: -len(x[1])):
                recent = errors[0]
                step = recent["step"] or "unknown step"
                msg = (recent["error"] or "")[:150]
                detail = f"_Last error_: **{step}** — {msg}" if msg else f"_Last error_ at {step}"
                st.error(f"**{zap_name}** — {len(errors)} error(s) in window\n\n{detail}")

        # ── Activity stream (last 30 events) ────────────────────────────
        st.markdown("#### Recent activity")
        stream_rows = []
        for r in in_window[:30]:
            icon = ZAP_STATUS_META.get(r["status"], ("·",))[0]
            time_str = r["_t"].astimezone().strftime("%m-%d %H:%M:%S")
            stream_rows.append({
                "Time":   time_str,
                "Status": f"{icon} {r['status']}",
                "Zap":    r["zap_name"],
                "Detail": (r["error"] or r["step"] or "")[:80],
            })
        st.dataframe(stream_rows, use_container_width=True, hide_index=True)

        # ── Per-zap aggregate table ─────────────────────────────────────
        st.markdown("#### Zap summary")
        by_zap = {}
        for r in in_window:
            zap = r["zap_name"]
            if zap not in by_zap:
                by_zap[zap] = {"name": zap, "total": 0, "last_run": None}
                for s in ZAP_STATUS_META:
                    by_zap[zap][s] = 0
            by_zap[zap]["total"] += 1
            by_zap[zap][r["status"]] = by_zap[zap].get(r["status"], 0) + 1
            if by_zap[zap]["last_run"] is None or r["_t"] > by_zap[zap]["last_run"]:
                by_zap[zap]["last_run"] = r["_t"]

        agg_rows = []
        for z in sorted(by_zap.values(), key=lambda x: (-x.get("error", 0), -x["total"])):
            err_rate = (z.get("error", 0) / z["total"] * 100) if z["total"] else 0
            agg_rows.append({
                "Zap":      z["name"],
                "Total":    z["total"],
                "✅":       z.get("success", 0),
                "❌":       z.get("error", 0),
                "🛑":       z.get("halted", 0),
                "⏸":       z.get("held", 0),
                "Other":    z["total"] - sum(z.get(s, 0) for s in ("success", "error", "halted", "held")),
                "Err %":    f"{err_rate:.1f}%" if err_rate else "—",
                "Last run": z["last_run"].astimezone().strftime("%m-%d %H:%M:%S") if z["last_run"] else "—",
            })
        st.dataframe(agg_rows, use_container_width=True, hide_index=True)

    _render_zap_dashboard()

# ── Onboarding log renderer ───────────────────────────────────────────────────
def _render_log_line(content: str):
    """Render a single log line with smart styling based on content."""
    import html as _html
    c = content.strip()
    safe = _html.escape(c)

    # Section header: ══════ or ────────
    if c.startswith("═") or c.startswith("╔") or c.startswith("╚"):
        return  # skip pure border lines — they're visual noise
    if c.startswith("╠") or c.startswith("║"):
        # Banner lines — show as bold header
        text = c.lstrip("║╠╔╚═ ").rstrip("║═ ")
        if text:
            st.markdown(
                f"<div style='background:#1e3a5f;color:#93c5fd;font-weight:700;"
                f"font-size:13px;padding:6px 12px;border-radius:4px;margin:6px 0'>"
                f"{_html.escape(text)}</div>",
                unsafe_allow_html=True
            )
        return

    # Box borders (┌ ├ └ │)
    if c.startswith("┌") or c.startswith("├") or c.startswith("└"):
        return  # skip box-drawing borders
    if c.startswith("│"):
        row = c.lstrip("│ ").rstrip("│ ")
        if row:
            st.markdown(
                f"<div style='font-size:13px;padding:2px 12px;color:#374151;"
                f"border-left:2px solid #e5e7eb;margin:1px 0'>{_html.escape(row)}</div>",
                unsafe_allow_html=True
            )
        return

    # Divider: ── or —
    if set(c.replace(" ", "")) <= {"─", "—", "-"} and len(c) > 6:
        st.markdown("<hr style='border:none;border-top:1px solid #e5e7eb;margin:8px 0'>",
                    unsafe_allow_html=True)
        return

    # List item: [1] Name — City, State (...)
    import re as _re
    list_match = _re.match(r"^\[(\d+)\]\s+(.+)$", c)
    if list_match:
        num = list_match.group(1)
        text = list_match.group(2)
        st.markdown(
            f"<div style='display:flex;gap:10px;align-items:baseline;padding:6px 10px;"
            f"background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;margin:3px 0'>"
            f"<span style='background:#3b82f6;color:white;font-size:11px;font-weight:700;"
            f"padding:2px 7px;border-radius:10px;min-width:24px;text-align:center'>{num}</span>"
            f"<span style='font-size:13px;color:#1e293b'>{_html.escape(text)}</span></div>",
            unsafe_allow_html=True
        )
        return

    # Step header: STEP X: Title
    if _re.match(r"^(STEP\s+\d|Step\s+\d)", c):
        st.markdown(
            f"<div style='background:#dbeafe;color:#1e40af;font-weight:700;"
            f"font-size:14px;padding:8px 14px;border-radius:6px;margin:8px 0'>"
            f"🔷 {safe}</div>",
            unsafe_allow_html=True
        )
        return

    # Instruction: 👉
    if c.startswith("👉") or "👉" in c[:6]:
        st.markdown(
            f"<div style='background:#fefce8;border-left:4px solid #eab308;"
            f"padding:8px 14px;border-radius:4px;margin:6px 0;font-size:13px;color:#713f12'>"
            f"{safe}</div>",
            unsafe_allow_html=True
        )
        return

    # Success: ✅
    if c.startswith("✅"):
        st.markdown(
            f"<div style='background:#f0fdf4;color:#166534;font-size:13px;"
            f"padding:5px 12px;border-radius:4px;margin:3px 0'>{safe}</div>",
            unsafe_allow_html=True
        )
        return

    # Warning: ⚠️ or ⏭️
    if c.startswith("⚠️") or c.startswith("⏭️"):
        st.markdown(
            f"<div style='background:#fff7ed;color:#9a3412;font-size:13px;"
            f"padding:5px 12px;border-radius:4px;margin:3px 0'>{safe}</div>",
            unsafe_allow_html=True
        )
        return

    # Info: ℹ️
    if c.startswith("ℹ️"):
        st.markdown(
            f"<div style='color:#475569;font-size:13px;padding:3px 12px;margin:2px 0'>"
            f"{safe}</div>",
            unsafe_allow_html=True
        )
        return

    # Link: 🔗
    if c.startswith("🔗"):
        st.markdown(
            f"<div style='background:#f0f9ff;border-left:3px solid #38bdf8;"
            f"padding:6px 12px;font-size:13px;color:#0369a1;margin:4px 0'>{safe}</div>",
            unsafe_allow_html=True
        )
        return

    # Blank / pure whitespace — skip
    if not c:
        return

    # Default: plain text
    st.markdown(
        f"<div style='font-size:13px;color:#374151;padding:2px 10px;margin:1px 0'>"
        f"{safe}</div>",
        unsafe_allow_html=True
    )


# ── Onboarding Tab ────────────────────────────────────────────────────────────
with tab_onboarding:
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

    # ── How to Use ────────────────────────────────────────────────────────────────
    st.info(
        "**How to use:**  Select a step from the dropdown and click **▶️ Start Step**. "
        "Steps must be run **in order (1 → 6)** for each new funeral home. "
        "The automation will ask you questions — type your answer and press **Send**, "
        "or use the **Yes / No** buttons for confirmation prompts. "
        "Do not close or navigate away while a step is running."
    )

    st.warning(
        "⚠️ **Make sure you're working on the correct funeral home** before starting. "
        "Each step makes live changes in Airtable, Twilio, and Zapier."
    )

    # Build display list: skip Step 6 (QA/inactive), renumber Step 7 → display 6
    _display_steps = []
    _disp_num = 1
    for s in STEPS:
        if s["key"] == "6":
            continue
        _display_steps.append({**s, "display_num": str(_disp_num)})
        _disp_num += 1
    # Map display label → actual Node.js step key
    _step_option_map = {
        f"{s['emoji']} Step {s['display_num']} – {s['title']}": s["key"]
        for s in _display_steps
    }

    col_step, col_action = st.columns([3, 1])

    with col_step:
        selected_step = st.selectbox(
            "Select Onboarding Step",
            options=list(_step_option_map.keys()),
            help="Choose which onboarding step to run for the current funeral home"
        )
        step_key = _step_option_map[selected_step]  # actual key sent to Node.js

        # Show description for the selected step
        step_meta = next((s for s in STEPS if s["key"] == step_key), None)
        if step_meta:
            st.caption(f"📋 {step_meta['description']}")

    with col_action:
        st.markdown("<div style='margin-top: 30px;'></div>", unsafe_allow_html=True)
        can_start = st.session_state.onboarding is None or not st.session_state.onboarding.is_running()
        _start_help = "A step is already running — finish or cancel it first" if not can_start else None
        if st.button("▶️ Start Step", use_container_width=True, disabled=not can_start, help=_start_help):
            ob = OnboardingAutomation()
            st.session_state.onboarding = ob
            st.session_state.onboarding_step = step_key
            st.session_state.onboarding_output = []
            try:
                with st.spinner(f"Running Step {step_key}…"):
                    ob.start_step(step_key)
                    # Wait up to 15 s for the first interactive prompt
                    deadline = time.time() + 15
                    while time.time() < deadline:
                        msg = ob.get_output()
                        if msg is None:
                            time.sleep(0.15)
                            continue
                        t = msg.get("t")
                        if t == "log":
                            st.session_state.onboarding_output.append(("log", msg.get("m", "")))
                        elif t == "ask":
                            st.session_state.onboarding_output.append(("ask", msg.get("q", "")))
                            break
                        elif t == "done":
                            st.session_state.onboarding_output.append(("done", "✅ Step completed!"))
                            st.session_state.onboarding = None
                            break
                        elif t == "error":
                            st.session_state.onboarding_output.append(("error", msg.get("m", "Unknown error")))
                            st.session_state.onboarding = None
                            break
                st.rerun()
            except Exception as e:
                st.error(f"❌ Failed to start onboarding: {str(e)}")
                st.session_state.onboarding = None

    st.markdown("---")

    # Display onboarding process
    if st.session_state.onboarding and st.session_state.onboarding.is_running():
        active_step = st.session_state.get("onboarding_step", "?")
        active_meta = next((s for s in STEPS if s["key"] == active_step), None)
        active_title = active_meta["title"] if active_meta else f"Step {active_step}"
        st.markdown(
            f"<div style='background:#1e3a5f;border-left:4px solid #4a9eff;padding:10px 16px;"
            f"border-radius:4px;margin-bottom:12px;'>"
            f"<strong style='color:#4a9eff'>⚙️ Running:</strong> "
            f"<span style='color:#e0e0e0'>Step {active_step} – {active_title}</span>"
            f"</div>",
            unsafe_allow_html=True
        )

        output_container = st.container()
        input_container = st.container()

        # Display all accumulated output
        with output_container:
            for msg_type, content in st.session_state.onboarding_output:
                if msg_type == "log":
                    _render_log_line(content)
                elif msg_type == "ask":
                    st.markdown(
                        f"<div style='background:#fffbeb;border-left:4px solid #f59e0b;"
                        f"padding:10px 14px;border-radius:4px;margin:8px 0;"
                        f"font-size:14px;color:#92400e'>"
                        f"<strong>❓ Input needed:</strong> {content}</div>",
                        unsafe_allow_html=True
                    )
                elif msg_type == "done":
                    st.success(content)
                elif msg_type == "error":
                    st.error(f"❌ {content}")

        # Input field for answers
        with input_container:
            last_msg = st.session_state.onboarding_output[-1] if st.session_state.onboarding_output else None
            if last_msg and last_msg[0] == "ask":
                st.caption("💡 Type a number to select from a list, or type **y** / **n** for yes/no questions. Use the buttons below as shortcuts.")
                prefill = st.session_state.pop("_prefill_answer", "")
                user_input = st.text_input("Your response:", value=prefill, key="onboarding_response",
                                           placeholder="Type your answer here…")
                col_send, col_yn, col_cancel = st.columns([2, 1, 1])
                with col_send:
                    if st.button("Send ➤", use_container_width=True):
                        if user_input.strip():
                            try:
                                ob = st.session_state.onboarding
                                ob.send_answer(user_input)
                                with st.spinner("Waiting for next prompt…"):
                                    deadline = time.time() + 15
                                    while time.time() < deadline:
                                        msg = ob.get_output()
                                        if msg is None:
                                            time.sleep(0.15)
                                            continue
                                        t = msg.get("t")
                                        if t == "log":
                                            st.session_state.onboarding_output.append(("log", msg.get("m", "")))
                                        elif t == "ask":
                                            st.session_state.onboarding_output.append(("ask", msg.get("q", "")))
                                            break
                                        elif t == "done":
                                            st.session_state.onboarding_output.append(("done", "✅ Step completed!"))
                                            st.session_state.onboarding = None
                                            break
                                        elif t == "error":
                                            st.session_state.onboarding_output.append(("error", msg.get("m", "Unknown error")))
                                            st.session_state.onboarding = None
                                            break
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed to send answer: {str(e)}")
                with col_yn:
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("✅ Yes", use_container_width=True, key="btn_yes",
                                     help="Sends 'y' — use for yes/no confirmation prompts"):
                            st.session_state["_prefill_answer"] = "y"
                            st.rerun()
                    with c2:
                        if st.button("❌ No", use_container_width=True, key="btn_no",
                                     help="Sends 'n' — use for yes/no confirmation prompts"):
                            st.session_state["_prefill_answer"] = "n"
                            st.rerun()
                with col_cancel:
                    if st.button("🛑 Cancel", use_container_width=True, key="btn_cancel",
                                 help="Stop the current step and discard progress"):
                        ob = st.session_state.onboarding
                        if ob:
                            ob.stop()
                        st.session_state.onboarding = None
                        st.session_state.onboarding_output = []
                        st.warning("Step cancelled.")
                        st.rerun()
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
                _render_log_line(content)
            elif msg_type == "ask":
                st.markdown(
                    f"<div style='background:#fffbeb;border-left:4px solid #f59e0b;"
                    f"padding:10px 14px;border-radius:4px;margin:8px 0;"
                    f"font-size:14px;color:#92400e'>"
                    f"<strong>❓</strong> {content}</div>",
                    unsafe_allow_html=True
                )
            elif msg_type == "done":
                st.success(content)
            elif msg_type == "error":
                st.error(f"❌ {content}")

        if st.button("Clear Output", use_container_width=True):
            st.session_state.onboarding_output = []
            st.rerun()
    else:
        st.markdown("#### 🗂️ Step Overview")
        st.caption("Run steps in order for each new funeral home. Click a step number in the dropdown above to select it, then press **▶️ Start Step**.")

        for step in _display_steps:
            st.markdown(
                f"<div style='display:flex;align-items:center;gap:14px;padding:10px 16px;"
                f"border-left:4px solid #3b82f6;background:#eff6ff;"
                f"border-radius:6px;margin-bottom:8px;'>"
                f"<span style='font-size:22px;min-width:28px'>{step['emoji']}</span>"
                f"<div>"
                f"<strong style='color:#1e40af;font-size:14px'>Step {step['display_num']}: {step['title']}</strong><br>"
                f"<span style='font-size:12px;color:#3b82f6'>{step['description']}</span>"
                f"</div></div>",
                unsafe_allow_html=True
            )

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


def _assignee_pills_html(task: dict) -> str:
    ids = task.get("assignee_ids") or []
    if not ids:
        return ""
    members = {m["id"]: m for m in load_members()}
    pills = []
    for mid in ids:
        m = members.get(mid)
        name = m["name"] if m else "(removed)"
        pills.append(
            f'<span style="background:#eef2ff;color:#4338ca;border-radius:10px;'
            f'padding:1px 8px;font-size:11px;margin-right:4px;">@{name}</span>'
        )
    return '<div style="margin-top:4px;">' + "".join(pills) + '</div>'


def _render_task_row(task: dict, tab_id: str = "all"):
    tid     = task["id"]
    k       = f"{tab_id}_{tid}"
    is_done = task.get("status") == "done"

    col_chk, col_info, col_type, col_due, col_edit, col_del = st.columns(
        [0.04, 0.52, 0.1, 0.18, 0.08, 0.08]
    )
    with col_chk:
        checked = st.checkbox(" ", value=is_done, key=f"chk_{k}", label_visibility="collapsed")
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
            f'</div>{desc_html}{_assignee_pills_html(task)}',
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
        if st.button("✖️" if editing_this else "✏️", key=f"edit_btn_{k}", help="Edit"):
            st.session_state.editing_task_id  = None if editing_this else tid
            st.session_state.deleting_task_id = None
            st.rerun()

    with col_del:
        deleting_this = st.session_state.deleting_task_id == tid
        if st.button("✖️" if deleting_this else "🗑️", key=f"del_btn_{k}", help="Delete"):
            st.session_state.deleting_task_id = None if deleting_this else tid
            st.session_state.editing_task_id  = None
            st.rerun()

    # Inline edit form
    if st.session_state.editing_task_id == tid:
        _e_members = [m for m in load_members() if m.get("active")]
        _e_name_to_id = {m["name"]: m["id"] for m in _e_members}
        _e_id_to_name = {m["id"]: m["name"] for m in _e_members}
        _current_assignees = [_e_id_to_name.get(i) for i in (task.get("assignee_ids") or [])
                              if i in _e_id_to_name]
        with st.form(key=f"edit_form_{k}"):
            st.markdown("**Edit Task**")
            e_title = st.text_input("Title", value=task.get("title", ""))
            e_desc  = st.text_area("Description", value=task.get("description", ""), height=70)
            _types  = ["daily", "weekly", "monthly", "one-off"]
            e_type  = st.selectbox("Type", _types, index=_types.index(task.get("type", "one-off")))
            _pris   = ["P1", "P2", "P3"]
            e_pri   = st.selectbox("Priority", _pris, index=_pris.index(task.get("priority", "P2")))
            raw_due = task.get("due_date")
            e_due   = st.date_input("Due Date", value=date.fromisoformat(raw_due) if raw_due else None)
            e_assignees = st.multiselect("Assigned to", options=list(_e_name_to_id.keys()),
                                         default=_current_assignees)
            s_col, c_col = st.columns(2)
            with s_col: save_btn   = st.form_submit_button("💾 Save",  use_container_width=True)
            with c_col: cancel_btn = st.form_submit_button("Cancel", use_container_width=True)
        if save_btn:
            update_task(tid, {"title": e_title.strip(), "description": e_desc.strip(),
                              "type": e_type, "priority": e_pri,
                              "due_date": str(e_due) if e_due else None,
                              "assignee_ids": [_e_name_to_id[n] for n in e_assignees]})
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
            if st.button("🗑️ Confirm", key=f"confirm_del_{k}", use_container_width=True):
                delete_task(tid)
                st.session_state.deleting_task_id = None
                st.rerun()
        with ac:
            if st.button("Cancel", key=f"abort_del_{k}", use_container_width=True):
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
        _render_task_row(t, tab_id=filter_type)


with tab_tasks:
    tasks = load_tasks()
    today_str = date.today().isoformat()

    # ── Summary metrics ───────────────────────────────────────────────────
    active_tasks  = [t for t in tasks if t.get("status") != "done"]
    done_today    = [t for t in tasks if t.get("status") == "done"
                     and (t.get("completed_at") or "")[:10] == today_str]
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

    # ── Manage Team ───────────────────────────────────────────────────────
    with st.expander("👥 Manage Team", expanded=False):
        _all_members = load_members()
        st.caption("People available for assignment. Inactive members won't appear in the dropdowns but stay on past tasks.")

        with st.form("add_member_form", clear_on_submit=True):
            mc_a, mc_b, mc_c, mc_d = st.columns([2, 2, 2, 1])
            with mc_a: _mn = st.text_input("Name *", placeholder="Full name")
            with mc_b: _me = st.text_input("Email", placeholder="name@example.com")
            with mc_c: _mr = st.text_input("Role", placeholder="Designer, PM…")
            with mc_d:
                st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
                _madd = st.form_submit_button("➕ Add", use_container_width=True)
        if _madd:
            if _mn.strip():
                add_member({"name": _mn.strip(), "email": _me.strip(), "role": _mr.strip()})
                st.rerun()
            else:
                st.warning("Name is required.")

        if _all_members:
            for _m in _all_members:
                m_a, m_b, m_c, m_d, m_e = st.columns([2, 2, 2, 1, 1])
                m_a.markdown(f"**{_m['name']}**" + ("" if _m["active"] else " *(inactive)*"))
                m_b.markdown(f"<span style='color:#666;font-size:12px;'>{_m.get('email','—') or '—'}</span>", unsafe_allow_html=True)
                m_c.markdown(f"<span style='color:#666;font-size:12px;'>{_m.get('role','—') or '—'}</span>", unsafe_allow_html=True)
                with m_d:
                    if st.button(("Deactivate" if _m["active"] else "Activate"), key=f"toggle_{_m['id']}", use_container_width=True):
                        update_member(_m["id"], {"active": not _m["active"]})
                        st.rerun()
                with m_e:
                    if st.button("🗑️", key=f"del_member_{_m['id']}", use_container_width=True):
                        delete_member(_m["id"])
                        st.rerun()
        else:
            st.info("No team members yet. Add one above.")

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