"""
SMS Health dashboard for the Parting Pro aftercare texting pipeline.

Reads from the "PartingPro - Opt-Out Detector" Supabase project, which holds two
distinct signals in one table, told apart by `detection_source`:

  airtable-inbound : a contact replied STOP (or something close to it) and the message
                     landed in the Airtable Messages table. Caught BEFORE a failed send.
  twilio-api       : Twilio rejected an outbound send, usually 21610 "unsubscribed".
                     Caught AFTER the fact - these are contacts Twilio is blocking whose
                     opt-out may never have produced an Airtable row at all.

The interesting population is the numbers that appear in the second group but not the
first: opted out at the carrier, invisible in Airtable, quietly burning a failed send
on every scheduled campaign.

Wire into app.py:
    from sms_health import render_sms_health
    ...
    PAGE_RENDERERS = {..., "SMS Health": None}
    ...
    elif selected_page == "SMS Health":
        render_sms_health()

Required Streamlit secrets:
    OPTOUT_SUPABASE_URL = "https://lzpdkykxmunljwharcln.supabase.co"
    OPTOUT_SUPABASE_KEY = "<publishable key for that project>"
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import streamlit as st

# --------------------------------------------------------------------------------------
# Connection
# --------------------------------------------------------------------------------------

OPTOUT_URL = st.secrets.get("OPTOUT_SUPABASE_URL", "").rstrip("/")
OPTOUT_KEY = st.secrets.get("OPTOUT_SUPABASE_KEY", "")

# Twilio error codes we actually see on this account, in plain language.
ERROR_LABELS = {
    21610: "Recipient unsubscribed (replied STOP)",
    21211: "Invalid 'To' number",
    21612: "Not reachable via SMS",
    21614: "Not a valid mobile number (landline)",
    21408: "Region not enabled on the account",
    30003: "Unreachable handset",
    30004: "Message blocked",
    30005: "Unknown handset",
    30006: "Landline or unreachable carrier",
    30007: "Carrier filtered as spam",
    30008: "Unknown delivery error",
    30032: "Toll-free number not verified",
}

# Which of those mean "stop texting this person, permanently".
PERMANENT_CODES = {21610, 21211, 21614}


def _configured() -> bool:
    return bool(OPTOUT_URL and OPTOUT_KEY)


@st.cache_data(ttl=300, show_spinner=False)
def _fetch(table: str, params: dict) -> pd.DataFrame:
    """GET one table via PostgREST. Returns an empty frame rather than raising, so a
    single bad panel never takes the whole page down."""
    try:
        res = requests.get(
            f"{OPTOUT_URL}/rest/v1/{table}",
            headers={
                "apikey": OPTOUT_KEY,
                "Authorization": f"Bearer {OPTOUT_KEY}",
                "Accept": "application/json",
            },
            params={**params, "limit": params.get("limit", 5000)},
            timeout=30,
        )
        res.raise_for_status()
        return pd.DataFrame(res.json())
    except Exception as exc:  # noqa: BLE001 - surfaced in the UI, not swallowed
        st.error(f"Could not read `{table}`: {exc}")
        return pd.DataFrame()


def _csv_download(df: pd.DataFrame, stem: str, key: str) -> None:
    """Download button for any table on the page. Filenames carry the date so
    successive exports don't overwrite each other in the Downloads folder."""
    if df.empty:
        st.caption("Nothing to export for the current filters.")
        return
    stamp = datetime.now().strftime("%Y-%m-%d")
    st.download_button(
        label=f"Download CSV ({len(df):,} rows)",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"{stem}_{stamp}.csv",
        mime="text/csv",
        key=key,
    )


def _pretty_code(code) -> str:
    if pd.isna(code):
        return "-"
    code = int(code)
    return f"{code} - {ERROR_LABELS.get(code, 'Unrecognised code')}"


# --------------------------------------------------------------------------------------
# Page
# --------------------------------------------------------------------------------------

def render_sms_health() -> None:
    st.title("SMS Health")

    if not _configured():
        st.warning(
            "Not connected. Add `OPTOUT_SUPABASE_URL` and `OPTOUT_SUPABASE_KEY` to the "
            "app's secrets, pointing at the Opt-Out Detector project."
        )
        return

    # ---- Filters -----------------------------------------------------------------
    with st.container(border=True):
        c1, c2, c3 = st.columns([1.2, 1, 1])

        with c1:
            preset = st.selectbox(
                "Date range",
                ["Last 7 days", "Last 14 days", "Last 30 days", "Last 60 days",
                 "Last 90 days", "Custom number of days", "All time"],
                index=2,
            )

        with c2:
            if preset == "Custom number of days":
                days = st.number_input(
                    "How many days back?", min_value=1, max_value=1095, value=30, step=1
                )
            elif preset == "All time":
                days = None
                st.caption("No date limit applied.")
            else:
                days = int(preset.split()[1])
                st.caption(f"Showing the last {days} days.")

        with c3:
            if st.button("Refresh data", use_container_width=True):
                st.cache_data.clear()
                st.rerun()

        since_iso = None
        if days is not None:
            since_iso = (datetime.now(timezone.utc) - timedelta(days=int(days))).isoformat()
            st.caption(f"Since {since_iso[:10]} UTC")

    ts_filter = {"message_timestamp": f"gte.{since_iso}"} if since_iso else {}

    # ---- Load --------------------------------------------------------------------
    messages = _fetch("sms_messages", {"select": "*", "order": "message_timestamp.desc", **ts_filter})
    queue = _fetch(
        "opt_out_review_queue",
        {"select": "*", "order": "message_timestamp.desc",
         **({"message_timestamp": f"gte.{since_iso}"} if since_iso else {})},
    )

    if not messages.empty and "detection_source" in messages:
        inbound = messages[messages["detection_source"] == "airtable-inbound"]
        twilio = messages[messages["detection_source"] == "twilio-api"]
    else:
        inbound = twilio = pd.DataFrame()

    # ---- Headline numbers --------------------------------------------------------
    confirmed = queue[queue["classification"] == "Opt-Out Confirmed"] if not queue.empty else pd.DataFrame()
    needs_review = queue[queue["classification"] == "Opt-Out Not Sure"] if not queue.empty else pd.DataFrame()
    unapplied = confirmed[~confirmed.get("opt_out_applied", pd.Series(dtype=bool)).fillna(False)] \
        if not confirmed.empty else pd.DataFrame()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Confirmed opt-outs", len(confirmed))
    m2.metric("Awaiting human review", len(needs_review))
    m3.metric("Confirmed, not yet applied", len(unapplied),
              help="Still textable in Airtable. Every campaign run reaches these people.")
    m4.metric("Twilio send failures", len(twilio))

    tab_queue, tab_errors, tab_gap, tab_health = st.tabs(
        ["Opt-out queue", "Twilio errors", "Invisible opt-outs", "Pipeline health"]
    )

    # ---- Opt-out review queue ----------------------------------------------------
    with tab_queue:
        if queue.empty:
            st.info("No opt-out signals in this window.")
        else:
            f1, f2 = st.columns(2)
            with f1:
                cls = st.multiselect(
                    "Classification",
                    sorted(queue["classification"].dropna().unique()),
                    default=sorted(queue["classification"].dropna().unique()),
                )
            with f2:
                bases = sorted(queue["source_base"].dropna().unique())
                base_sel = st.multiselect("Source base", bases, default=bases)

            view = queue[queue["classification"].isin(cls) & queue["source_base"].isin(base_sel)]

            cols = [c for c in ["message_timestamp", "classification", "status", "contact_name",
                                "contact_cell", "funeral_home", "source_base", "message_content",
                                "reasoning", "opt_out_applied"] if c in view.columns]
            st.dataframe(view[cols], use_container_width=True, hide_index=True)
            _csv_download(view[cols], "opt_out_queue", "dl_queue")

    # ---- Twilio errors -----------------------------------------------------------
    with tab_errors:
        if twilio.empty:
            st.info(
                "No Twilio failures recorded yet. If `twilio-error-sync` has not run "
                "successfully, check that TWILIO_API_KEY_SID and TWILIO_API_KEY_SECRET "
                "are set in the Edge Function secrets."
            )
        else:
            by_code = (
                twilio.groupby("error_code", dropna=False)
                .agg(occurrences=("twilio_sid", "count"),
                     distinct_numbers=("contact_cell", "nunique"))
                .reset_index()
                .sort_values("occurrences", ascending=False)
            )
            by_code["error"] = by_code["error_code"].apply(_pretty_code)

            st.subheader("By error code")
            st.dataframe(
                by_code[["error", "occurrences", "distinct_numbers"]],
                use_container_width=True, hide_index=True,
            )
            _csv_download(by_code, "twilio_errors_by_code", "dl_by_code")

            st.subheader("Worst offenders")
            st.caption("Numbers wasting the most sends. Each row is a contact still in a send view.")
            worst = (
                twilio.groupby("contact_cell")
                .agg(failed_sends=("twilio_sid", "count"),
                     last_attempt=("message_timestamp", "max"),
                     codes=("error_code", lambda s: ", ".join(sorted({str(int(x)) for x in s.dropna()}))))
                .reset_index()
                .sort_values("failed_sends", ascending=False)
            )
            st.dataframe(worst, use_container_width=True, hide_index=True)
            _csv_download(worst, "twilio_worst_numbers", "dl_worst")

            with st.expander("Every failed send"):
                cols = [c for c in ["message_timestamp", "contact_cell", "error_code",
                                    "error_message", "send_status", "twilio_sid",
                                    "message_content"] if c in twilio.columns]
                st.dataframe(twilio[cols], use_container_width=True, hide_index=True)
                _csv_download(twilio[cols], "twilio_failed_sends", "dl_all_errors")

    # ---- The gap between the two signals -----------------------------------------
    with tab_gap:
        st.caption(
            "Numbers Twilio is permanently rejecting that have **no** inbound opt-out "
            "message in Airtable. Nothing in the Airtable pipeline can see these - they "
            "are the reason sends keep failing with no visible cause."
        )
        if twilio.empty:
            st.info("Needs Twilio data. Run `twilio-error-sync` first.")
        else:
            blocked = twilio[twilio["error_code"].isin(PERMANENT_CODES)]
            known = set(inbound["contact_cell"].dropna()) if not inbound.empty else set()
            invisible = blocked[~blocked["contact_cell"].isin(known)]

            g1, g2 = st.columns(2)
            g1.metric("Permanently blocked numbers", blocked["contact_cell"].nunique())
            g2.metric("Of those, invisible in Airtable", invisible["contact_cell"].nunique())

            summary = (
                invisible.groupby("contact_cell")
                .agg(failed_sends=("twilio_sid", "count"),
                     first_seen=("message_timestamp", "min"),
                     last_seen=("message_timestamp", "max"),
                     error_code=("error_code", "first"))
                .reset_index()
                .sort_values("failed_sends", ascending=False)
            )
            summary["error"] = summary["error_code"].apply(_pretty_code)
            st.dataframe(summary, use_container_width=True, hide_index=True)
            _csv_download(summary, "invisible_opt_outs", "dl_invisible")

    # ---- Pipeline health ---------------------------------------------------------
    with tab_health:
        status = _fetch("v_pipeline_status", {"select": "*"})
        usage = _fetch("api_usage", {"select": "*", "order": "period.desc"})
        runs = _fetch("detector_runs", {"select": "*", "order": "started_at.desc", "limit": 25})
        jobs = _fetch("job_log", {"select": "*", "order": "logged_at.desc", "limit": 50})

        if not status.empty:
            row = status.iloc[0]
            h1, h2, h3 = st.columns(3)
            h1.metric("Pipeline", "Running" if row.get("enabled") else "PAUSED")
            cap = row.get("airtable_monthly_cap") or 0
            used = int(usage[usage["service"] == "airtable"]["calls"].sum()) if not usage.empty else 0
            h2.metric("Airtable calls this month", f"{used:,}", help=f"Self-imposed allocation: {cap:,}")
            h3.metric("Auto-pause threshold", f"{row.get('pause_at_pct', 0)}%")
            if cap:
                st.progress(min(used / cap, 1.0))
            if not row.get("enabled"):
                st.error("The pipeline is paused. Check the job log below for why.")

        st.subheader("Recent runs")
        if runs.empty:
            st.info("No runs recorded.")
        else:
            cols = [c for c in ["started_at", "run_type", "outcome", "messages_processed",
                                "confirmed_count", "not_sure_count", "errors"] if c in runs.columns]
            st.dataframe(runs[cols], use_container_width=True, hide_index=True)
            _csv_download(runs[cols], "detector_runs", "dl_runs")

        st.subheader("Scheduled job log")
        if jobs.empty:
            st.info("No scheduled jobs logged yet.")
        else:
            st.dataframe(jobs, use_container_width=True, hide_index=True)
            _csv_download(jobs, "job_log", "dl_jobs")
