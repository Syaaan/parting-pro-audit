import re
import io
import time
import requests
import openpyxl
import streamlit as st
import pandas as pd
from collections import Counter
from openpyxl.styles import Font, PatternFill, Alignment

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN = st.secrets["AIRTABLE_TOKEN"]
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
</style>
""", unsafe_allow_html=True)

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
    st.markdown("---")
    st.markdown("<div style='font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; opacity:0.5; margin-bottom:8px;'>Connected Bases</div>", unsafe_allow_html=True)
    for b in BASE_IDS:
        st.markdown(f"<div style='font-size:12px; opacity:0.7; padding: 4px 0;'>• {b}</div>", unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("<div style='font-size:11px; opacity:0.4; text-align:center;'>Parting Pro Internal · 2025</div>", unsafe_allow_html=True)

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

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center; padding: 32px 0 16px 0;">
    <img src="https://partingpro.com/wp-content/uploads/2024/07/partingpro-logo.png" style="height:22px; opacity:0.4;" />
    <div style="font-size:11px; color:#b0b8c8; margin-top:8px;">Aftercare Texting Audit Tool · Internal Use Only</div>
</div>
""", unsafe_allow_html=True)