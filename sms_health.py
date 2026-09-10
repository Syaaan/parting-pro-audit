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

CONTROL IS CONTACT-FIRST. The first version of this page hung the opt-out tick boxes off
the MESSAGE review queue and applied them by re-deriving the contact from a phone
string. That could never work for the ~24% of queue rows whose Airtable message has no
contact link, or whose Contact Cell is truncated to "+1". Consent belongs to a person, so
the control surface is `v_opt_out_control`, one row per contact record per base built on
contacts_mirror, and writes address {base_id, contact_record_id} directly.

Wire into app.py:
    from sms_health import render_sms_health
    ...
    PAGE_RENDERERS = {..., "SMS Health": None}
    ...
    PAGE_RENDERERS["SMS Health"] = render_sms_health

Required Streamlit secrets (top level, ABOVE every [section] header):
    OPTOUT_SUPABASE_URL = "https://lzpdkykxmunljwharcln.supabase.co"
    OPTOUT_SUPABASE_KEY = "<publishable key for that project>"
    CRON_SECRET         = "<value of pipeline_settings.cron_secret>"
    AUDIT_ACTOR         = "you@partingpro.com"   # optional, stamped on every action
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import streamlit as st

# --------------------------------------------------------------------------------------
# Connection
# --------------------------------------------------------------------------------------

def _secret(*names: str) -> str:
    """Look up a secret tolerantly.

    Streamlit's secrets.toml is section-aware: anything pasted BELOW a `[section]`
    header belongs to that section, not the top level. Appending to the bottom of an
    existing file is therefore a common way to end up with keys the app cannot see at
    the top level. So check the top level first, then one level into each section.
    """
    for n in names:
        try:
            v = st.secrets.get(n)
            if v:
                return str(v)
        except Exception:
            pass
    try:
        for section in st.secrets:
            sub = st.secrets[section]
            if hasattr(sub, "get"):
                for n in names:
                    v = sub.get(n)
                    if v:
                        return str(v)
    except Exception:
        pass
    return ""


def _visible_secret_names() -> str:
    """Key NAMES only - never values - so a misconfiguration is diagnosable."""
    try:
        out = []
        for k in st.secrets:
            v = st.secrets[k]
            out.append(f"[{k}]" if hasattr(v, "keys") else k)
        return ", ".join(sorted(out)) or "(none)"
    except Exception as exc:  # noqa: BLE001
        return f"(could not read secrets: {exc})"


OPTOUT_URL = _secret("OPTOUT_SUPABASE_URL", "optout_supabase_url").rstrip("/")
OPTOUT_KEY = _secret("OPTOUT_SUPABASE_KEY", "optout_supabase_key")
CRON_SECRET = _secret("CRON_SECRET", "cron_secret")
AUDIT_ACTOR = _secret("AUDIT_ACTOR", "audit_actor") or "audit-app"

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
    30019: "Message too long",
    30032: "Toll-free number not verified",
    30034: "Our number is unregistered (A2P)",
    60005: "Verification service error",
}

# Which of those mean "stop texting this person, permanently".
PERMANENT_CODES = {21610, 21211, 21614}


def _configured() -> bool:
    return bool(OPTOUT_URL and OPTOUT_KEY)


@st.cache_data(ttl=300, show_spinner=False)
def _fetch(table: str, params: dict) -> pd.DataFrame:
    """GET one table or view via PostgREST. Returns an empty frame rather than raising,
    so a single bad panel never takes the whole page down."""
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


def _in_filter(values) -> str:
    """PostgREST in.() with values that may contain commas or spaces.

    'Uncertain reply, needs review' has a comma in it, which would otherwise be read
    as a list separator and silently return the wrong rows.
    """
    quoted = ",".join('"' + str(v).replace('"', '\\"') + '"' for v in values)
    return f"in.({quoted})"


def _csv_download(df: pd.DataFrame, stem: str, key: str) -> None:
    """Download button for any table on the page. Filenames carry the date so
    successive exports don't overwrite each other in the Downloads folder."""
    if df is None or df.empty:
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


def _call_apply(payload: dict) -> dict:
    """Route writes through the Edge Function, never straight to the database.

    This app is public, so the browser-side key is read-only by design. Approving an
    opt-out changes production contact records, so it goes through the function's
    shared secret instead - the same gate the scheduled jobs use.
    """
    if not CRON_SECRET:
        return {"error": "CRON_SECRET is not set in this app's secrets, so nothing can "
                         "be applied. Copy the value of pipeline_settings.cron_secret "
                         "into Streamlit App settings -> Secrets as CRON_SECRET, at the "
                         "top of the file above every [section] header."}
    try:
        res = requests.post(
            f"{OPTOUT_URL}/functions/v1/apply-opt-outs",
            headers={"Content-Type": "application/json", "x-cron-secret": CRON_SECRET},
            json=payload,
            timeout=120,
        )
        res.raise_for_status()
        return res.json()
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


def _report_result(result: dict, applied: bool) -> None:
    """One place that reads the function's response.

    The old version checked result["error"] only. The function returns `errors`,
    plural, as a list, so per-base failures rendered as a green success with 0 updated.
    """
    if result.get("error"):
        st.error(result["error"])
        return

    problems = result.get("errors") or []
    if applied:
        updated = result.get("updated", 0)
        failed = result.get("failedCount", 0)
        if updated:
            st.success(f"Applied. {updated} contact record(s) set to Opt-Out.")
        else:
            st.warning("Nothing was updated. Expand the response below for why.")
        if failed:
            st.error(f"{failed} record(s) failed to write.")
        extras = []
        if result.get("reconciled"):
            extras.append(f"{result['reconciled']} queue row(s) closed as already opted out")
        if result.get("dismissed"):
            extras.append(f"{result['dismissed']} row(s) dismissed")
        if extras:
            st.caption(" · ".join(extras))
    else:
        st.info(
            f"Dry run. {result.get('wouldChangeCount', 0)} contact record(s) would be "
            f"set to Opt-Out across {len(result.get('byBase') or {})} base(s), covering "
            f"{result.get('distinctNumbers', 0)} distinct number(s). Nothing was written."
        )

    if problems:
        st.error("The function reported problems:")
        for p in problems:
            st.markdown(f"- {p}")

    with st.expander("Full response"):
        st.json(result)


def _grid_key(prefix: str, keys) -> str:
    """A widget key that changes when the row set changes.

    st.data_editor stores edits by row POSITION. If the underlying rows shift while
    stored edits exist, those edits land on the wrong contacts. Deriving the key from
    the row identities means a changed row set gets a clean widget instead of
    silently misapplied ticks.
    """
    h = hashlib.sha1("|".join(sorted(map(str, keys))).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{h}"


# --------------------------------------------------------------------------------------
# Opt-Out Control
# --------------------------------------------------------------------------------------

CONTROL_COLS = [
    "control_key", "base_label", "contact_name", "all_names", "distinct_names",
    "phone", "funeral_home", "funeral_home_source", "record_count",
    "signal_state", "opted_out_in_airtable", "partially_opted_out",
    "inbound_signals", "twilio_failed_sends",
    "last_message", "last_reasoning", "last_signal_at", "last_failure_at",
    "contact_record_id", "base_id", "phone_d10",
]

# How the funeral home was established, spelled out so a derived value is never
# mistaken for one Airtable actually holds.
FH_SOURCE_SUFFIX = {
    "airtable": "",
    "duplicate-record": "  (from a duplicate of this number)",
    "creator": "  (derived from who created the record)",
    "creator-ambiguous": "  (needs a human, see the name)",
    "unknown": "",
}


def _funeral_home_label(row) -> str:
    """Never render a bare None, and always say when a value was derived."""
    fh = row.get("funeral_home")
    if fh is None or (isinstance(fh, float) and pd.isna(fh)) or str(fh).strip() == "":
        fh = "(not linked to a case)"
    return f"{fh}{FH_SOURCE_SUFFIX.get(row.get('funeral_home_source'), '')}"


def _render_control() -> None:
    st.caption(
        "One row per **person** per base, built from the nightly contacts mirror and "
        "every signal we hold about that number. Ticking **Opt out** sets the Opt-Out "
        "checkbox on that contact in Airtable. Opting out applies to **every base the "
        "number appears in**, because consent belongs to the person, not to a base. "
        "Where a number has more than one contact record in the same base, the rows are "
        "collapsed into one and **Records** shows how many sit behind it. Ticking the "
        "row covers all of them."
    )

    c1, c2, c3 = st.columns([1.4, 1.4, 1])
    with c1:
        search = st.text_input(
            "Search name or number",
            placeholder="e.g. Sara Lewis or 2545633156",
            help="Leave blank to see only contacts with an open opt-out signal.",
        )
    with c2:
        bases = _fetch("airtable_bases", {"select": "label", "order": "sort_order"})
        base_opts = list(bases["label"]) if not bases.empty else ["v1", "v1.2", "v1.3"]
        base_sel = st.multiselect("Base", base_opts, default=base_opts)
    with c3:
        show_all = st.toggle(
            "Include resolved",
            value=False,
            help="Also show contacts already opted out, and contacts with no signal.",
        )

    params = {"select": ",".join(CONTROL_COLS), "order": "signal_state,contact_name", "limit": 2000}
    if base_sel and len(base_sel) != len(base_opts):
        params["base_label"] = _in_filter(base_sel)

    if search.strip():
        needle = search.strip().replace("*", "").replace(",", " ")
        digits = "".join(ch for ch in needle if ch.isdigit())
        clauses = [f"contact_name.ilike.*{needle}*", f"funeral_home.ilike.*{needle}*"]
        if digits:
            clauses.append(f"phone_d10.ilike.*{digits}*")
        params["or"] = "(" + ",".join(clauses) + ")"
    elif not show_all:
        params["actionable"] = "is.true"
    else:
        params["signal_state"] = "neq.No signal"

    data = _fetch("v_opt_out_control", params)

    if data.empty:
        if search.strip():
            st.info("No contacts match that search.")
        else:
            st.success("No contacts have an open opt-out signal. Nothing to action.")
        return

    actionable = data[~data["opted_out_in_airtable"].fillna(False)] \
        if "opted_out_in_airtable" in data else data
    resolved = data[data["opted_out_in_airtable"].fillna(False)] \
        if "opted_out_in_airtable" in data else pd.DataFrame()

    m1, m2, m3 = st.columns(3)
    m1.metric("Shown", len(data))
    m2.metric("Still textable", len(actionable),
              help="Opt-Out is not set in Airtable, so every campaign run reaches these people.")
    m3.metric("Already opted out", len(resolved))

    if actionable.empty:
        st.success("Every contact in this view is already opted out in Airtable.")
        _csv_download(data, "opt_out_control", "dl_control_all")
        return

    actionable = actionable.copy()
    # Spell the funeral home out, including whether it was derived, so a blank cell
    # never renders as the string "None" the way a raw null does.
    actionable["funeral_home"] = actionable.apply(_funeral_home_label, axis=1)
    # Where a number carries more than one name, show them all: those are either a
    # shared handset or a duplicate entered under a different spelling.
    if "all_names" in actionable and "distinct_names" in actionable:
        actionable["contact_name"] = actionable.apply(
            lambda r: r["all_names"] if (r.get("distinct_names") or 0) > 1 else r["contact_name"],
            axis=1,
        )

    display = [
        "base_label", "contact_name", "phone", "funeral_home", "record_count",
        "signal_state", "inbound_signals", "twilio_failed_sends",
        "last_message", "last_reasoning",
    ]
    display = [c for c in display if c in actionable.columns]

    grid = actionable[["control_key"] + display].reset_index(drop=True).copy()
    grid.insert(0, "Opt out", False)
    grid.insert(1, "Dismiss", False)

    # A form, so ticking a box does NOT rerun the script. The old version re-queried
    # Airtable on every single click and rebuilt the row set from the response, which
    # is what made ticks appear to vanish.
    with st.form("opt_out_control_form", border=True):
        st.markdown("**Tick the contacts to opt out, then Preview or Apply.**")
        edited = st.data_editor(
            grid,
            hide_index=True,
            use_container_width=True,
            disabled=display,
            column_config={
                "Opt out": st.column_config.CheckboxColumn(
                    "Opt out", help="Set the Opt-Out checkbox on this contact in Airtable"),
                "Dismiss": st.column_config.CheckboxColumn(
                    "Dismiss", help="Mark the signal reviewed and not an opt-out. Writes nothing to Airtable."),
                "control_key": None,
                "base_label": st.column_config.TextColumn("Base", width="small"),
                "contact_name": st.column_config.TextColumn("Contact"),
                "phone": st.column_config.TextColumn("Number", width="small"),
                "funeral_home": st.column_config.TextColumn("Funeral home", width="medium"),
                "record_count": st.column_config.NumberColumn(
                    "Records", width="small",
                    help="Contact records behind this row in this base. More than 1 means "
                         "duplicates on the same number. Ticking the row covers all of them."),
                "signal_state": st.column_config.TextColumn("Why flagged"),
                "inbound_signals": st.column_config.NumberColumn("Replies", width="small"),
                "twilio_failed_sends": st.column_config.NumberColumn("Failed sends", width="small"),
                "last_message": st.column_config.TextColumn("Last message", width="large"),
                "last_reasoning": st.column_config.TextColumn("Classifier reasoning", width="medium"),
            },
            key=_grid_key("control_editor", grid["control_key"]),
        )

        fold_carrier = st.checkbox(
            "Also opt out every carrier-blocked number (Twilio 21610)",
            value=False,
            help="Off by default. Ticking it applies the whole carrier list in the same "
                 "run, not just the rows ticked above. The Carrier opt-outs tab shows "
                 "exactly which numbers that is.",
        )

        f1, f2, f3 = st.columns([1, 1, 2])
        with f1:
            preview = st.form_submit_button("Preview", use_container_width=True,
                                            help="Dry run. Shows what would change, writes nothing.")
        with f2:
            do_apply = st.form_submit_button("Apply to Airtable", type="primary",
                                             use_container_width=True)
        with f3:
            sure = st.checkbox("I'm sure", help="Required before Apply will write anything.")

    if not (preview or do_apply):
        st.caption(f"{len(grid)} contact(s) in this view. Nothing submitted yet.")
        _csv_download(actionable, "opt_out_control", "dl_control")
        return

    # Read the ticks as a positional mask against `grid`, not by pulling
    # control_key out of `edited`. Inside a form there are no reruns between
    # render and submit, so the two frames are row-for-row identical, and this
    # does not depend on whether a column hidden via column_config comes back in
    # the editor's return value.
    def _picked(col: str) -> list:
        if col not in edited:
            return []
        mask = edited[col].fillna(False).astype(bool).to_numpy()
        return list(grid.loc[mask, "control_key"])

    chosen_keys = _picked("Opt out")
    dismiss_keys = _picked("Dismiss")

    if not chosen_keys and not dismiss_keys and not fold_carrier:
        st.warning("Nothing was ticked, so there is nothing to preview or apply.")
        return

    if do_apply and not sure:
        st.warning("Tick **I'm sure** and press Apply again. Nothing was written.")
        return

    payload = {
        "apply": bool(do_apply),
        "actor": AUDIT_ACTOR,
        "scope": "all-bases",
        "keys": chosen_keys,
        "dismiss_keys": dismiss_keys,
        # Opt in only. This used to be hardcoded True, which meant every Apply on
        # this tab quietly also opted out the entire carrier-blocked list - a much
        # bigger write than the ticks on screen implied.
        "include_twilio_auto": bool(fold_carrier),
        "reconcile_queue": True,
    }
    with st.spinner("Talking to Airtable..."):
        result = _call_apply(payload)
    _report_result(result, applied=bool(do_apply))
    if do_apply and not result.get("error"):
        st.cache_data.clear()


# --------------------------------------------------------------------------------------
# Carrier opt-outs (Twilio 21610)
# --------------------------------------------------------------------------------------

CARRIER_COLS = [
    "phone_d10", "phone", "contact_name", "funeral_home", "funeral_home_source",
    "failed_sends", "first_seen", "last_seen", "seen_in_bases", "contact_records",
    "opted_out_in_airtable", "partially_opted_out", "status", "actionable",
    "control_keys",
]

# Where the funeral home on a carrier row came from. Same rule as Opt-Out Control:
# a derived value is never allowed to look like one Airtable holds.
CARRIER_FH_SUFFIX = {
    # Airtable held the link on the contact the send was addressed to. Trustworthy.
    "matched-contact": "",
    "message row": "",
    # The send record carried a home but its number never matched a contact record,
    # so the attribution is the send's, not the contact's.
    "unmatched-number": "  (from the send record, number not matched to a contact)",
    "contacts mirror": "  (from the contacts mirror, not the send record)",
    "duplicate-record": "  (from a duplicate of this number)",
    "creator": "  (derived from who created the record)",
    "creator-ambiguous": "  (needs a human, see the name)",
    "unresolved": "",
}


def _render_carrier() -> None:
    st.caption(
        "Numbers the **carrier** rejected, not the person. Twilio returns error "
        "**21610** when the handset has replied STOP to the carrier itself, so the "
        "message never reaches them and every later send is billed and wasted. These "
        "are a separate list from the message-based opt-outs on Opt-Out Control: "
        "nothing here came through the classifier, so there is no reply text to read. "
        "The funeral home is the one the number is attached to, taken from the message "
        "row where we have it and from the contacts mirror otherwise."
    )

    data = _fetch("v_carrier_opt_outs", {
        "select": ",".join(CARRIER_COLS),
        "order": "status,last_seen.desc",
        "limit": 2000,
    })

    if data.empty:
        st.success("No carrier-level opt-outs recorded. Nothing to action.")
        return

    for col in ("opted_out_in_airtable", "actionable", "partially_opted_out"):
        if col in data:
            data[col] = data[col].fillna(False).astype(bool)

    actionable = data[data["actionable"]] if "actionable" in data else data
    resolved = data[data.get("status").eq("Already opted out")] \
        if "status" in data else pd.DataFrame()
    orphaned = data[data.get("status").eq("No contact record to opt out")] \
        if "status" in data else pd.DataFrame()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Carrier-blocked numbers", len(data))
    m2.metric("Still textable", len(actionable),
              help="The carrier is blocking them but Opt-Out is not set in Airtable, so "
                   "every campaign run still tries to reach them and still gets billed.")
    m3.metric("Already opted out", len(resolved))
    m4.metric("Wasted sends", int(data["failed_sends"].fillna(0).sum()),
              help="Total sends Twilio rejected with 21610 across every number here.")

    if not orphaned.empty:
        st.warning(
            f"{len(orphaned)} number(s) are carrier-blocked but have **no contact record** "
            "in any base, so there is nothing to tick. They are listed at the bottom. "
            "Either the contact was deleted or the number never made it into a base."
        )

    st.divider()

    if actionable.empty:
        st.success("Every carrier-blocked number that has a contact record is already opted out.")
        _csv_download(data.drop(columns=["control_keys"], errors="ignore"),
                      "carrier_opt_outs", "dl_carrier_all")
        return

    view = actionable.copy()
    view["funeral_home"] = view.apply(
        lambda r: f"{r.get('funeral_home')}"
                  f"{CARRIER_FH_SUFFIX.get(r.get('funeral_home_source'), '')}",
        axis=1,
    )

    display = ["contact_name", "phone", "funeral_home", "contact_records",
               "failed_sends", "seen_in_bases", "last_seen", "partially_opted_out"]
    display = [c for c in display if c in view.columns]

    grid = view[["phone_d10"] + display].reset_index(drop=True).copy()
    grid.insert(0, "Opt out", False)

    # Same form-wrapped pattern as Opt-Out Control: no rerun between ticking a box
    # and submitting, so ticks cannot be lost to a shifting row set.
    with st.form("carrier_opt_out_form", border=True):
        st.markdown("**Tick the numbers to opt out, then Preview or Apply.**")
        edited = st.data_editor(
            grid,
            hide_index=True,
            use_container_width=True,
            disabled=display,
            column_config={
                "Opt out": st.column_config.CheckboxColumn(
                    "Opt out",
                    help="Set the Opt-Out checkbox on every contact record on this "
                         "number, in every base."),
                "phone_d10": None,
                "contact_name": st.column_config.TextColumn("Contact"),
                "phone": st.column_config.TextColumn("Number", width="small"),
                "funeral_home": st.column_config.TextColumn("Funeral home", width="medium"),
                "contact_records": st.column_config.NumberColumn(
                    "Records", width="small",
                    help="Contact records on this number across all three bases. "
                         "Ticking the row covers all of them."),
                "failed_sends": st.column_config.NumberColumn(
                    "Rejected sends", width="small",
                    help="How many sends the carrier has already thrown away."),
                "seen_in_bases": st.column_config.TextColumn("Seen in", width="small"),
                "last_seen": st.column_config.DatetimeColumn(
                    "Last rejection", width="small", format="YYYY-MM-DD HH:mm"),
                "partially_opted_out": st.column_config.CheckboxColumn(
                    "Split", width="small",
                    help="Some records on this number are already opted out and some "
                         "are not. Ticking the row brings them all into line."),
            },
            key=_grid_key("carrier_editor", grid["phone_d10"]),
        )

        f1, f2, f3 = st.columns([1, 1, 2])
        with f1:
            preview = st.form_submit_button(
                "Preview", use_container_width=True,
                help="Dry run. Shows what would change, writes nothing.")
        with f2:
            do_apply = st.form_submit_button("Apply to Airtable", type="primary",
                                             use_container_width=True)
        with f3:
            sure = st.checkbox("I'm sure", key="carrier_sure",
                               help="Required before Apply will write anything.")

    if not (preview or do_apply):
        st.caption(f"{len(grid)} number(s) still textable. Nothing submitted yet.")
        _csv_download(data.drop(columns=["control_keys"], errors="ignore"),
                      "carrier_opt_outs", "dl_carrier")
        if not orphaned.empty:
            st.markdown("**Carrier-blocked with no contact record to tick**")
            st.dataframe(
                orphaned[[c for c in ["phone", "funeral_home", "failed_sends",
                                      "seen_in_bases", "last_seen"]
                          if c in orphaned.columns]],
                use_container_width=True, hide_index=True,
            )
        return

    # Positional mask against `grid`, then expand each ticked number into the
    # contact records behind it. The view hands us those already, in the
    # "<recordId>:<baseId>" form apply-opt-outs takes, so one tick can cover
    # several records in several bases.
    mask = edited["Opt out"].fillna(False).astype(bool).to_numpy() \
        if "Opt out" in edited else []
    picked_phones = list(grid.loc[mask, "phone_d10"]) if len(mask) else []

    key_lookup = dict(zip(view["phone_d10"], view["control_keys"]))
    chosen_keys: list[str] = []
    for ph in picked_phones:
        chosen_keys.extend(list(key_lookup.get(ph) or []))

    if not chosen_keys:
        st.warning("Nothing was ticked, so there is nothing to preview or apply.")
        return

    if do_apply and not sure:
        st.warning("Tick **I'm sure** and press Apply again. Nothing was written.")
        return

    st.caption(
        f"{len(picked_phones)} number(s) ticked, covering {len(chosen_keys)} contact "
        "record(s) across the bases."
    )

    payload = {
        "apply": bool(do_apply),
        "actor": AUDIT_ACTOR,
        "scope": "all-bases",
        "keys": chosen_keys,
        "dismiss_keys": [],
        # The whole-list shortcut stays off here on purpose. This tab is the
        # per-number control, so it must write exactly what was ticked.
        "include_twilio_auto": False,
        "reconcile_queue": True,
    }
    with st.spinner("Talking to Airtable..."):
        result = _call_apply(payload)
    _report_result(result, applied=bool(do_apply))
    if do_apply and not result.get("error"):
        st.cache_data.clear()


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
        st.caption(
            f"URL found: **{bool(OPTOUT_URL)}** &nbsp;·&nbsp; key found: **{bool(OPTOUT_KEY)}**"
        )
        st.caption(f"Secret names this app can see (names only, no values): `{_visible_secret_names()}`")
        st.caption(
            "If the two names are missing from that list, they were most likely pasted "
            "below a `[section]` header in secrets.toml and belong to that section. "
            "Move them to the very top of the file, above every `[section]` line."
        )
        return

    if not CRON_SECRET:
        st.error(
            "`CRON_SECRET` is missing from this app's secrets, so **no opt-out can be "
            "applied**. Ticks will save nothing. Copy the value of "
            "`pipeline_settings.cron_secret` into App settings -> Secrets as "
            "`CRON_SECRET`, above every `[section]` header."
        )

    # ---- Filters -----------------------------------------------------------------
    with st.container(border=True):
        c1, c2, c3 = st.columns([1.2, 1, 1])

        with c1:
            preset = st.selectbox(
                "Date range",
                ["Last 7 days", "Last 14 days", "Last 30 days", "Last 60 days",
                 "Last 90 days", "Custom number of days", "All time"],
                index=2,
                help="Applies to the message-level tabs. The Opt-Out Control tab always "
                     "shows the current state of every contact.",
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
    queue = _fetch("opt_out_review_queue",
                   {"select": "*", "order": "message_timestamp.desc", **ts_filter})

    if not messages.empty and "detection_source" in messages:
        inbound = messages[messages["detection_source"] == "airtable-inbound"]
        twilio = messages[messages["detection_source"] == "twilio-api"]
    else:
        inbound = twilio = pd.DataFrame()

    # ---- Headline numbers --------------------------------------------------------
    open_rows = queue[~queue.get("opt_out_applied", pd.Series(dtype=bool)).fillna(False)] \
        if not queue.empty else pd.DataFrame()
    confirmed = open_rows[open_rows["classification"] == "Opt-Out Confirmed"] \
        if not open_rows.empty else pd.DataFrame()
    needs_review = open_rows[open_rows["classification"] == "Opt-Out Not Sure"] \
        if not open_rows.empty else pd.DataFrame()

    still_textable = _fetch("v_opt_out_control",
                            {"select": "control_key", "actionable": "is.true", "limit": 5000})

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Contacts still textable", len(still_textable),
              help="Have an open opt-out signal and Opt-Out is not set in Airtable. "
                   "Every campaign run reaches these people.")
    m2.metric("Confirmed, open", len(confirmed))
    m3.metric("Awaiting human review", len(needs_review))
    m4.metric("Twilio send failures", len(twilio))

    (tab_control, tab_carrier, tab_queue, tab_errors, tab_gap, tab_blocked,
     tab_audit, tab_health) = st.tabs(
        ["Opt-Out Control", "Carrier opt-outs", "Review queue", "Twilio errors",
         "Invisible opt-outs", "Cannot action", "Audit log", "Pipeline health"]
    )

    # ---- Opt-Out Control ---------------------------------------------------------
    with tab_control:
        _render_control()

        st.divider()
        st.subheader("Housekeeping")
        st.caption(
            "Queue rows whose contact is already opted out in Airtable, usually by the "
            "older Airtable automation or by hand. Closing them costs no Airtable calls "
            "and stops them reappearing in the review list."
        )
        if st.button("Close rows already opted out in Airtable"):
            with st.spinner("Reconciling..."):
                res = _call_apply({
                    "apply": True, "actor": AUDIT_ACTOR, "keys": [],
                    "include_twilio_auto": False, "reconcile_queue": True,
                })
            if res.get("error"):
                st.error(res["error"])
            else:
                st.success(f"{res.get('reconciled', 0)} queue row(s) closed.")
                st.cache_data.clear()

    # ---- Carrier opt-outs --------------------------------------------------------
    with tab_carrier:
        _render_carrier()

    # ---- Review queue ------------------------------------------------------------
    with tab_queue:
        if queue.empty:
            st.info("No opt-out signals in this window.")
        else:
            st.caption(
                "The raw message-level signals. This is a reading view. Opting anyone "
                "out happens on the **Opt-Out Control** tab, against the contact record."
            )
            f1, f2, f3 = st.columns(3)
            with f1:
                cls = st.multiselect(
                    "Classification",
                    sorted(queue["classification"].dropna().unique()),
                    default=sorted(queue["classification"].dropna().unique()),
                )
            with f2:
                bases = sorted(queue["source_base"].dropna().unique())
                base_sel = st.multiselect("Source base", bases, default=bases)
            with f3:
                only_open = st.toggle("Open rows only", value=True)

            view = queue[queue["classification"].isin(cls) & queue["source_base"].isin(base_sel)]
            if only_open and "opt_out_applied" in view:
                view = view[~view["opt_out_applied"].fillna(False)]

            cols = [c for c in ["message_timestamp", "classification", "status",
                                "contact_name", "contact_cell", "funeral_home", "source_base",
                                "message_content", "reasoning", "approve_opt_out",
                                "opt_out_applied"] if c in view.columns]
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

            if invisible.empty:
                st.success("Nothing invisible in this window.")
            else:
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

    # ---- Signals we cannot action -------------------------------------------------
    with tab_blocked:
        st.caption(
            "Opt-out signals with no usable contact behind them: the Airtable message "
            "has no Contact link, or the linked contact's Contact Cell is missing or "
            "truncated. These cannot be applied by any automation, and they used to sit "
            "in the review grid with a tick box that did nothing. They are an upstream "
            "Airtable data problem, so fix the message or the contact record."
        )
        orphans = _fetch("v_opt_out_orphans",
                         {"select": "*", "order": "message_timestamp.desc", "limit": 1000})
        if orphans.empty:
            st.success("Every open signal maps to a real contact. Nothing stuck.")
        else:
            st.metric("Signals that cannot be actioned", len(orphans))
            cols = [c for c in ["message_timestamp", "classification", "blocker",
                                "source_base", "funeral_home", "contact_name",
                                "contact_cell", "contact_record_id", "message_content",
                                "message_record_id"] if c in orphans.columns]
            st.dataframe(orphans[cols], use_container_width=True, hide_index=True)
            _csv_download(orphans[cols], "opt_out_cannot_action", "dl_orphans")

    # ---- Audit log ----------------------------------------------------------------
    with tab_audit:
        st.caption(
            "Every opt-out this app has applied, one row per contact record per "
            "attempt, with who did it and why."
        )
        actions = _fetch("opt_out_actions",
                         {"select": "*", "order": "requested_at.desc", "limit": 2000})
        if actions.empty:
            st.info("No opt-outs have been applied from this app yet.")
        else:
            a1, a2, a3 = st.columns(3)
            a1.metric("Total actions", len(actions))
            a2.metric("Applied", int((actions["result"] == "applied").sum()))
            a3.metric("Failed", int((actions["result"] == "failed").sum()))
            cols = [c for c in ["requested_at", "applied_at", "result", "actor", "reason",
                                "scope", "contact_name", "phone_raw", "funeral_home",
                                "base_id", "contact_record_id", "error_message"]
                    if c in actions.columns]
            st.dataframe(actions[cols], use_container_width=True, hide_index=True)
            _csv_download(actions[cols], "opt_out_actions", "dl_actions")

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
