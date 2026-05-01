"""
inbox_scanner.py — Smart Inbox scanner for the Parting Pro audit app.

Reads Gmail (IMAP) and Slack (REST API), classifies messages with Google
Gemini Flash (free tier), creates tasks in task_store, and sends email
notifications. All external I/O functions return [] / {} on failure and
never raise — the Streamlit page handles displaying errors.
"""

import imaplib
import email
import email.header
import smtplib
import json
import re
import datetime
import requests
import streamlit as st
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from streamlit_autorefresh import st_autorefresh
from task_store import add_task

# ── Paths ─────────────────────────────────────────────────────────────────────
_DIR = Path(__file__).parent
_PROCESSED_IDS_FILE = _DIR / "processed_ids.json"

# ── Gemini endpoint ───────────────────────────────────────────────────────────
_GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-1.5-flash:generateContent"
)

# ═════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _secret(key: str, default: str = "") -> str:
    try:
        return st.secrets[key]
    except Exception:
        return default


def load_processed_ids() -> set:
    if not _PROCESSED_IDS_FILE.exists():
        return set()
    try:
        data = json.loads(_PROCESSED_IDS_FILE.read_text(encoding="utf-8"))
        return set(data) if isinstance(data, list) else set()
    except Exception:
        return set()


def save_processed_ids(ids: set) -> None:
    sorted_ids = sorted(ids)
    if len(sorted_ids) > 10_000:
        sorted_ids = sorted_ids[-5_000:]
    _PROCESSED_IDS_FILE.write_text(
        json.dumps(sorted_ids, indent=2), encoding="utf-8"
    )


def mark_processed(new_ids: list) -> None:
    existing = load_processed_ids()
    existing.update(new_ids)
    save_processed_ids(existing)


def _decode_header_value(raw) -> str:
    if raw is None:
        return ""
    parts = email.header.decode_header(raw)
    decoded = []
    for part, enc in parts:
        if isinstance(part, bytes):
            decoded.append(part.decode(enc or "utf-8", errors="replace"))
        else:
            decoded.append(str(part))
    return " ".join(decoded)


def _extract_plain_text(msg) -> str:
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition", ""))
            if ct == "text/plain" and "attachment" not in cd:
                try:
                    body = part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8", errors="replace"
                    )
                    break
                except Exception:
                    continue
        if not body:
            for part in msg.walk():
                ct = part.get_content_type()
                if ct == "text/html":
                    try:
                        html = part.get_payload(decode=True).decode(
                            part.get_content_charset() or "utf-8", errors="replace"
                        )
                        body = re.sub(r"<[^>]+>", " ", html)
                        body = re.sub(r"\s+", " ", body).strip()
                        break
                    except Exception:
                        continue
    else:
        try:
            body = msg.get_payload(decode=True).decode(
                msg.get_content_charset() or "utf-8", errors="replace"
            )
        except Exception:
            body = str(msg.get_payload())
    return body.strip()[:1_500]


# ═════════════════════════════════════════════════════════════════════════════
# GMAIL IMAP READER
# ═════════════════════════════════════════════════════════════════════════════

def fetch_gmail_messages(max_messages: int = 30) -> tuple:
    """Returns (messages: list[dict], error: str | None)."""
    address = _secret("GMAIL_ADDRESS")
    password = _secret("GMAIL_APP_PASSWORD")

    if not address or not password:
        return [], "Gmail credentials not configured (GMAIL_ADDRESS / GMAIL_APP_PASSWORD missing from secrets)."

    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
        mail.login(address, password)
        mail.select("INBOX")

        _, data = mail.uid("search", None, "UNSEEN")
        uids = data[0].split() if data and data[0] else []
        uids = uids[-max_messages:]  # most recent N

        messages = []
        for uid in reversed(uids):
            try:
                _, msg_data = mail.uid("fetch", uid, "(RFC822)")
                if not msg_data or not msg_data[0]:
                    continue
                raw = msg_data[0][1]
                msg = email.message_from_bytes(raw)
                uid_str = uid.decode() if isinstance(uid, bytes) else str(uid)
                messages.append({
                    "id": f"gmail_{uid_str}",
                    "source": "gmail",
                    "subject": _decode_header_value(msg.get("Subject", "(no subject)")),
                    "from": _decode_header_value(msg.get("From", "")),
                    "date": _decode_header_value(msg.get("Date", "")),
                    "body": _extract_plain_text(msg),
                })
            except Exception:
                continue

        mail.logout()
        return messages, None

    except imaplib.IMAP4.error as e:
        return [], f"Gmail login failed — check GMAIL_ADDRESS and GMAIL_APP_PASSWORD. ({e})"
    except (OSError, ConnectionRefusedError, TimeoutError) as e:
        return [], f"Gmail connection error — {e}"
    except Exception as e:
        return [], f"Gmail unexpected error — {e}"


# ═════════════════════════════════════════════════════════════════════════════
# SLACK READER
# ═════════════════════════════════════════════════════════════════════════════

def fetch_slack_messages(max_per_channel: int = 20) -> tuple:
    """Returns (messages: list[dict], errors: list[str])."""
    token = _secret("SLACK_BOT_TOKEN")
    channels_raw = _secret("SLACK_CHANNELS")

    if not token:
        return [], ["Slack bot token not configured (SLACK_BOT_TOKEN missing from secrets)."]
    if not channels_raw:
        return [], ["No Slack channels configured (SLACK_CHANNELS missing from secrets)."]

    channel_ids = [c.strip() for c in channels_raw.split(",") if c.strip()]
    headers = {"Authorization": f"Bearer {token}"}
    messages = []
    errors = []

    for channel_id in channel_ids:
        try:
            resp = requests.get(
                "https://slack.com/api/conversations.history",
                headers=headers,
                params={"channel": channel_id, "limit": max_per_channel},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            if not data.get("ok"):
                err = data.get("error", "unknown")
                if err == "invalid_auth":
                    errors.append("Slack: Invalid bot token — check SLACK_BOT_TOKEN.")
                    break
                elif err == "channel_not_found":
                    errors.append(f"Slack: Channel '{channel_id}' not found — check SLACK_CHANNELS.")
                elif err == "not_in_channel":
                    errors.append(f"Slack: Bot not in channel '{channel_id}' — run /invite @bot-name.")
                elif err == "ratelimited":
                    errors.append(f"Slack: Rate limited on '{channel_id}' — will retry next scan.")
                else:
                    errors.append(f"Slack: Error on '{channel_id}' — {err}")
                continue

            for m in data.get("messages", []):
                if m.get("subtype") == "bot_message" or m.get("bot_id"):
                    continue
                text = m.get("text", "").strip()
                if not text:
                    continue
                ts = m.get("ts", "")
                messages.append({
                    "id": f"slack_{channel_id}_{ts}",
                    "source": "slack",
                    "channel": channel_id,
                    "text": text[:1_500],
                    "ts": ts,
                    "user": m.get("user", ""),
                })

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                errors.append(f"Slack: Rate limited on '{channel_id}' — will retry next scan.")
            else:
                errors.append(f"Slack: HTTP error on '{channel_id}' — {e}")
        except requests.exceptions.RequestException as e:
            errors.append(f"Slack: Network error — {e}")

    return messages, errors


# ═════════════════════════════════════════════════════════════════════════════
# GEMINI CLASSIFIER
# ═════════════════════════════════════════════════════════════════════════════

_DEFAULT_CLASSIFICATION = {
    "is_task": False,
    "title": "",
    "description": "",
    "priority": "",
    "type_suggestion": "",
    "urgency_reason": "",
}


def _build_prompt(messages: list) -> str:
    msg_data = []
    for m in messages:
        if m["source"] == "gmail":
            preview = f"Subject: {m.get('subject','')}\nFrom: {m.get('from','')}\n\n{m.get('body','')}"
        else:
            preview = m.get("text", "")
        msg_data.append({
            "id": m["id"],
            "source": m["source"],
            "preview": preview[:800],
        })

    return f"""You are a task classifier for an operations team at Parting Pro, a SaaS platform for funeral homes.

Analyze each message and determine if it contains an actionable task — something a person must do (fix a bug, follow up with someone, send a report, investigate an issue, update a system, onboard a client, etc.).

NOT tasks: newsletters, marketing emails, automated system notifications requiring no action, FYI updates, confirmations that something is already done.

Return a JSON array. Each element corresponds to an input message IN ORDER with exactly these fields:
- "message_id": the id from the input
- "is_task": true or false
- "title": short action-oriented title starting with a verb, max 60 chars (empty string if not a task)
- "description": 1-2 sentence context for why this is a task (empty string if not a task)
- "priority": "P1" (urgent/blocking), "P2" (important/soon), "P3" (low/nice-to-have) — empty string if not a task
- "type_suggestion": "one-off", "daily", "weekly", or "monthly" — use one-off unless the message clearly implies a recurring action (empty string if not a task)
- "urgency_reason": one sentence explaining urgency level (empty string if not a task)

Messages:
{json.dumps(msg_data, indent=2)}

Return ONLY a valid JSON array. No markdown, no explanation, no code fences."""


def classify_messages_with_gemini(messages: list) -> tuple:
    """Returns (classifications: list[dict], error: str | None)."""
    if not messages:
        return [], None

    api_key = _secret("GEMINI_API_KEY")
    if not api_key:
        return [dict(_DEFAULT_CLASSIFICATION, message_id=m["id"]) for m in messages], \
               "GEMINI_API_KEY not configured in secrets."

    payload = {
        "contents": [{"parts": [{"text": _build_prompt(messages)}]}],
        "generationConfig": {
            "temperature": 0.1,
            "responseMimeType": "application/json",
        },
    }

    try:
        resp = requests.post(
            _GEMINI_URL,
            params={"key": api_key},
            json=payload,
            timeout=30,
        )

        if resp.status_code == 429:
            return [dict(_DEFAULT_CLASSIFICATION, message_id=m["id"]) for m in messages], \
                   "Gemini API rate limited (free tier: 15 req/min) — tasks will be classified on the next scan cycle."

        if resp.status_code == 400:
            return [dict(_DEFAULT_CLASSIFICATION, message_id=m["id"]) for m in messages], \
                   "Gemini API key invalid — check GEMINI_API_KEY in secrets."

        resp.raise_for_status()
        raw_text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        results = json.loads(raw_text)

        if not isinstance(results, list):
            raise ValueError("Gemini response was not a JSON array")

        # Align results to input messages by message_id; pad if Gemini returns fewer
        id_to_result = {r.get("message_id", ""): r for r in results if isinstance(r, dict)}
        aligned = []
        for m in messages:
            r = id_to_result.get(m["id"], {})
            aligned.append({
                "message_id": m["id"],
                "is_task": bool(r.get("is_task", False)),
                "title": str(r.get("title", "")).strip(),
                "description": str(r.get("description", "")).strip(),
                "priority": r.get("priority", "P2") or "P2",
                "type_suggestion": r.get("type_suggestion", "one-off") or "one-off",
                "urgency_reason": str(r.get("urgency_reason", "")).strip(),
            })
        return aligned, None

    except json.JSONDecodeError as e:
        st.session_state["inbox_gemini_last_error"] = resp.text[:500] if "resp" in dir() else str(e)
        return [dict(_DEFAULT_CLASSIFICATION, message_id=m["id"]) for m in messages], \
               f"Gemini response parse error — {e}. Raw response logged in session state."
    except requests.exceptions.RequestException as e:
        return [dict(_DEFAULT_CLASSIFICATION, message_id=m["id"]) for m in messages], \
               f"Gemini network error — {e}"
    except Exception as e:
        return [dict(_DEFAULT_CLASSIFICATION, message_id=m["id"]) for m in messages], \
               f"Gemini unexpected error — {e}"


# ═════════════════════════════════════════════════════════════════════════════
# EMAIL NOTIFIER
# ═════════════════════════════════════════════════════════════════════════════

def send_task_notification(new_tasks: list) -> None:
    if not new_tasks:
        return

    address = _secret("GMAIL_ADDRESS")
    password = _secret("GMAIL_APP_PASSWORD")
    notify_to = _secret("NOTIFY_EMAIL")

    if not (address and password and notify_to):
        return

    today = datetime.date.today().strftime("%-d %b %Y")
    subject = f"[Smart Inbox] {len(new_tasks)} new task{'s' if len(new_tasks) != 1 else ''} detected — {today}"

    lines = [
        f"Smart Inbox detected {len(new_tasks)} new task{'s' if len(new_tasks) != 1 else ''} from your Gmail and Slack.\n",
        "=" * 60,
    ]
    for i, t in enumerate(new_tasks, 1):
        lines.append(f"\n{i}. {t['title']}")
        lines.append(f"   Priority : {t.get('priority', 'P2')}")
        lines.append(f"   Type     : {t.get('type', 'one-off')}")
        lines.append(f"   Source   : {t.get('source', '')}")
        if t.get("description"):
            lines.append(f"   Details  : {t['description']}")
        if t.get("urgency_reason"):
            lines.append(f"   Urgency  : {t['urgency_reason']}")
    lines.append(f"\n{'='*60}")
    lines.append("Open the app to view and manage these tasks.")

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = address
        msg["To"] = notify_to
        msg.attach(MIMEText("\n".join(lines), "plain"))

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(address, password)
            server.sendmail(address, notify_to, msg.as_string())
    except Exception:
        pass  # notification failure must not block task creation


# ═════════════════════════════════════════════════════════════════════════════
# SCAN ORCHESTRATOR
# ═════════════════════════════════════════════════════════════════════════════

def run_inbox_scan(max_gmail: int = 30, max_slack_per_channel: int = 20) -> dict:
    errors = []
    new_tasks_created = []

    processed_ids = load_processed_ids()

    gmail_msgs, gmail_err = fetch_gmail_messages(max_gmail)
    if gmail_err:
        errors.append(gmail_err)

    slack_msgs, slack_errs = fetch_slack_messages(max_slack_per_channel)
    errors.extend(slack_errs)

    all_msgs = gmail_msgs + slack_msgs
    new_msgs = [m for m in all_msgs if m["id"] not in processed_ids]

    if not new_msgs:
        return {
            "new_tasks": [],
            "skipped": 0,
            "total_scanned": len(all_msgs),
            "gmail_count": len(gmail_msgs),
            "slack_count": len(slack_msgs),
            "errors": errors,
            "scanned_at": datetime.datetime.now().isoformat(),
        }

    classifications, gemini_err = classify_messages_with_gemini(new_msgs)
    if gemini_err:
        errors.append(gemini_err)

    msg_source_map = {m["id"]: m["source"] for m in new_msgs}

    for clf in classifications:
        if not clf.get("is_task"):
            continue
        title = clf.get("title", "").strip()
        if not title:
            continue
        task_data = {
            "title": title,
            "description": clf.get("description", ""),
            "type": clf.get("type_suggestion", "one-off"),
            "priority": clf.get("priority", "P2"),
            "source": msg_source_map.get(clf.get("message_id", ""), "inbox"),
            "urgency_reason": clf.get("urgency_reason", ""),
        }
        created = add_task(task_data)
        created["urgency_reason"] = clf.get("urgency_reason", "")
        new_tasks_created.append(created)

    mark_processed([m["id"] for m in new_msgs])
    send_task_notification(new_tasks_created)

    return {
        "new_tasks": new_tasks_created,
        "skipped": len(new_msgs) - len(new_tasks_created),
        "total_scanned": len(all_msgs),
        "gmail_count": len(gmail_msgs),
        "slack_count": len(slack_msgs),
        "errors": errors,
        "scanned_at": datetime.datetime.now().isoformat(),
    }


# ═════════════════════════════════════════════════════════════════════════════
# PAGE RENDERER
# ═════════════════════════════════════════════════════════════════════════════

def render_inbox_page(refresh_interval: int = 10) -> None:
    # Auto-refresh — fires a page rerun every N minutes while tab is open
    refresh_count = st_autorefresh(
        interval=refresh_interval * 60 * 1000,
        key="inbox_autorefresh",
    )

    # ── Session state init ─────────────────────────────────────────────────
    for k, v in [
        ("inbox_scan_result", None),
        ("inbox_scan_log", []),
        ("inbox_max_gmail", 30),
        ("inbox_max_slack", 20),
    ]:
        if k not in st.session_state:
            st.session_state[k] = v

    # ── Header ─────────────────────────────────────────────────────────────
    st.markdown("""
    <div class="section-head" style="margin-bottom:20px;">
        <div class="section-icon">🤖</div>
        <div class="section-head-text">
            <h3>Smart Inbox</h3>
            <p>Auto-scans Gmail and Slack for actionable tasks using Google Gemini AI</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Status bar ─────────────────────────────────────────────────────────
    last_result = st.session_state["inbox_scan_result"]
    last_scan_str = "Never"
    if last_result:
        try:
            ts = datetime.datetime.fromisoformat(last_result["scanned_at"])
            last_scan_str = ts.strftime("%-d %b %Y, %-I:%M %p")
        except Exception:
            last_scan_str = last_result.get("scanned_at", "")[:16]

    processed_count = len(load_processed_ids())
    st.markdown(
        f'<div style="font-size:12px;color:#4a5568;margin-bottom:16px;">'
        f'Last scan: <strong style="color:#1a2b4a;">{last_scan_str}</strong>'
        f'&nbsp;·&nbsp; Auto-refresh: every <strong style="color:#1a2b4a;">{refresh_interval} min</strong>'
        f'&nbsp;·&nbsp; Messages tracked: <strong style="color:#1a2b4a;">{processed_count:,}</strong>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Settings expander ──────────────────────────────────────────────────
    with st.expander("⚙️ Scan Settings", expanded=False):
        col_a, col_b = st.columns(2)
        with col_a:
            st.session_state["inbox_max_gmail"] = st.number_input(
                "Max Gmail messages", min_value=5, max_value=100,
                value=st.session_state["inbox_max_gmail"], step=5,
                key="gmail_max_input",
            )
        with col_b:
            st.session_state["inbox_max_slack"] = st.number_input(
                "Max Slack messages per channel", min_value=5, max_value=50,
                value=st.session_state["inbox_max_slack"], step=5,
                key="slack_max_input",
            )

        st.markdown("**Credentials status:**")
        secrets_needed = {
            "GMAIL_ADDRESS": "Gmail address",
            "GMAIL_APP_PASSWORD": "Gmail app password",
            "GEMINI_API_KEY": "Gemini API key",
            "SLACK_BOT_TOKEN": "Slack bot token",
            "SLACK_CHANNELS": "Slack channel IDs",
            "NOTIFY_EMAIL": "Notification email",
        }
        for key, label in secrets_needed.items():
            val = _secret(key)
            icon = "✅" if val else "❌"
            note = "" if val else " — add to Streamlit secrets"
            st.markdown(f"- {icon} **{label}**{note}")

        st.markdown(
            "<div style='font-size:12px;color:#9aa5b4;margin-top:8px;'>"
            "⚠️ Tracked message IDs reset on redeploy — brief duplicates possible after a new deploy."
            "</div>",
            unsafe_allow_html=True,
        )

        if st.button("🔍 Scan Now", key="scan_now_settings", use_container_width=True):
            _do_scan()

    # ── Auto-scan on refresh tick ──────────────────────────────────────────
    if refresh_count > 0:
        _do_scan()

    # ── Results area ───────────────────────────────────────────────────────
    result = st.session_state["inbox_scan_result"]

    if result is None:
        st.markdown('<div class="section-wrap">', unsafe_allow_html=True)
        st.markdown(
            "<div style='padding:40px 0; text-align:center;'>"
            "<div style='font-size:40px;margin-bottom:12px;'>🤖</div>"
            "<div style='font-size:15px;font-weight:600;color:#1a2b4a;margin-bottom:6px;'>Waiting for first scan</div>"
            "<div style='font-size:13px;color:#9aa5b4;margin-bottom:20px;'>Auto-scan is active. Results appear here after the first cycle.</div>"
            "</div>",
            unsafe_allow_html=True,
        )
        if st.button("🔍 Scan Now", key="scan_now_first", use_container_width=False):
            _do_scan()
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        _render_results(result)

    # ── Scan log ───────────────────────────────────────────────────────────
    log = st.session_state["inbox_scan_log"]
    if log:
        with st.expander(f"📜 Scan Log (last {len(log)} scans)", expanded=False):
            for entry in log:
                try:
                    ts = datetime.datetime.fromisoformat(entry["scanned_at"])
                    ts_str = ts.strftime("%-d %b, %-I:%M %p")
                except Exception:
                    ts_str = entry.get("scanned_at", "")[:16]
                tasks_n = len(entry.get("new_tasks", []))
                scanned_n = entry.get("total_scanned", 0)
                errs = entry.get("errors", [])
                err_str = f" · ⚠️ {len(errs)} error(s)" if errs else ""
                color = "#1a9e5c" if tasks_n > 0 else "#4a5568"
                st.markdown(
                    f'<div style="font-size:12px;padding:4px 0;border-bottom:1px solid #f0f2f7;">'
                    f'<strong style="color:#1a2b4a;">{ts_str}</strong>'
                    f' &nbsp;·&nbsp; <span style="color:{color};">{tasks_n} task(s) found</span>'
                    f' &nbsp;·&nbsp; {scanned_n} scanned{err_str}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    # ── Setup guide ────────────────────────────────────────────────────────
    missing = [k for k in ("GMAIL_ADDRESS", "GMAIL_APP_PASSWORD", "GEMINI_API_KEY") if not _secret(k)]
    if missing:
        with st.expander("📖 Setup Guide — How to configure credentials", expanded=True):
            st.markdown("""
**1. Gmail App Password** (required for reading email + sending notifications)
1. Go to [myaccount.google.com](https://myaccount.google.com) → Security → 2-Step Verification (must be ON)
2. Search for "App passwords" → create one named "Parting Pro Inbox"
3. Copy the 16-character password (format: `xxxx xxxx xxxx xxxx`)
4. Also enable IMAP: Gmail → Settings → See All Settings → Forwarding and POP/IMAP → Enable IMAP

**2. Gemini API Key** (free — no credit card)
1. Go to [aistudio.google.com](https://aistudio.google.com)
2. Click **Get API key** → Create API key in new project
3. Copy the key starting with `AIzaSy...`

**3. Slack Bot Token** (optional)
1. Go to [api.slack.com/apps](https://api.slack.com/apps) → Create New App → From Scratch
2. OAuth & Permissions → Bot Token Scopes: add `channels:history`, `channels:read`, `groups:history`
3. Install App to Workspace → copy the **Bot User OAuth Token** (`xoxb-...`)
4. In each Slack channel: `/invite @your-bot-name`
5. Get channel IDs: right-click channel → View channel details → copy ID at the bottom

**4. Add secrets to Streamlit Cloud**
Go to your app dashboard → Settings → Secrets and add:
```toml
GMAIL_ADDRESS      = "your@gmail.com"
GMAIL_APP_PASSWORD = "xxxx xxxx xxxx xxxx"
GEMINI_API_KEY     = "AIzaSy..."
SLACK_BOT_TOKEN    = "xoxb-..."
SLACK_CHANNELS     = "C012AB3CD,C98765XYZ"
NOTIFY_EMAIL       = "your@gmail.com"
```
            """)


def _do_scan():
    with st.spinner("🔍 Scanning Gmail and Slack…"):
        result = run_inbox_scan(
            max_gmail=st.session_state.get("inbox_max_gmail", 30),
            max_slack_per_channel=st.session_state.get("inbox_max_slack", 20),
        )
    st.session_state["inbox_scan_result"] = result

    log = st.session_state.get("inbox_scan_log", [])
    log.insert(0, result)
    st.session_state["inbox_scan_log"] = log[:10]


def _render_results(result: dict):
    errors = result.get("errors", [])
    for err in errors:
        if "invalid_auth" in err.lower() or "key invalid" in err.lower() or "login failed" in err.lower():
            st.error(f"❌ {err}")
        else:
            st.warning(f"⚠️ {err}")

    new_tasks = result.get("new_tasks", [])
    total = result.get("total_scanned", 0)
    skipped = result.get("skipped", 0)
    gmail_n = result.get("gmail_count", 0)
    slack_n = result.get("slack_count", 0)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("New Tasks Added", len(new_tasks))
    c2.metric("Messages Scanned", total)
    c3.metric("Not Tasks", skipped)
    c4.metric("📧 Gmail / 💬 Slack", f"{gmail_n} / {slack_n}")

    st.markdown('<div class="section-wrap" style="margin-top:16px;">', unsafe_allow_html=True)

    if not new_tasks:
        st.markdown(
            "<div style='padding:24px 0; text-align:center; color:#1a9e5c; font-size:15px; font-weight:600;'>"
            "✅ All clear — no new tasks found in this scan."
            "</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f'<div style="font-size:13px;color:#4a5568;margin-bottom:12px;padding-bottom:8px;'
            f'border-bottom:1px solid #e4e7ef;">'
            f'<strong style="color:#1a2b4a;">{len(new_tasks)}</strong> task(s) added to your Task Tracker</div>',
            unsafe_allow_html=True,
        )
        for t in new_tasks:
            source_icon = "📧" if t.get("source") == "gmail" else "💬"
            pri = t.get("priority", "P2")
            pri_colors = {"P1": ("#fde8e8", "#c0392b"), "P2": ("#fff0e0", "#c47f00"), "P3": ("#f0f2f7", "#4a5568")}
            bg, fg = pri_colors.get(pri, pri_colors["P2"])
            urgency = t.get("urgency_reason", "")
            urgency_html = f'<div style="font-size:11px;color:#6b7a94;margin-top:2px;">{urgency}</div>' if urgency else ""
            st.markdown(
                f'<div style="padding:10px 0;">'
                f'{source_icon} &nbsp;'
                f'<strong style="color:#1a2b4a;">{t["title"]}</strong>'
                f'&nbsp; <span style="background:{bg};color:{fg};border-radius:20px;padding:2px 8px;font-size:11px;font-weight:700;">{pri}</span>'
                f'&nbsp; <span style="background:#eef2ff;color:#3b5bdb;border-radius:6px;padding:2px 6px;font-size:11px;">{t.get("type","one-off")}</span>'
                f'{urgency_html}'
                f'</div>',
                unsafe_allow_html=True,
            )
            st.markdown("<hr style='margin:2px 0;border-color:#f0f2f7;'>", unsafe_allow_html=True)

        st.markdown(
            "<div style='font-size:12px;color:#9aa5b4;margin-top:8px;'>"
            "These tasks have been added to your Task Tracker — switch to ✅ Tasks to manage them."
            "</div>",
            unsafe_allow_html=True,
        )

    st.markdown("</div>", unsafe_allow_html=True)
