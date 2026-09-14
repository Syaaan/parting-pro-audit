"""Recurring Task Tracker tab for the Parting Pro audit app.

Mirrors the "Recurring Task Tracker" page of the Airtable Task Management
interface (Title, Description, Type, Priority, Status, Due Date, Assigned To,
Source, Notes, Links) but reads and writes Supabase instead of Airtable.

Two levels, because recurring work has two:

  * Tasks         - the dated instances the generator creates each cycle.
                    This is the list you work from. Sub-tabs: All / Daily /
                    Weekly / Monthly.
  * Schedules     - the definitions behind them. Frequency, anchor day, grace
                    period, pause. Editing one changes every future instance.

Secrets:
  Reads use the same project the SMS Health page already talks to, so
  OPTOUT_SUPABASE_URL / OPTOUT_SUPABASE_KEY are enough to see the tab.
  Writes need TT_SUPABASE_SERVICE_KEY (the service role key). Without it the
  tab renders read-only and says so, rather than failing on save.

Public entry point:
  render_recurring()
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st

# --------------------------------------------------------------------------------------
# Time. Everything user-facing is Pacific; the database stores timestamptz.
# --------------------------------------------------------------------------------------

APP_TZ = ZoneInfo("America/Los_Angeles")


def local_today() -> date:
    return datetime.now(APP_TZ).date()


def local_now() -> datetime:
    return datetime.now(APP_TZ)


# --------------------------------------------------------------------------------------
# Connection
# --------------------------------------------------------------------------------------

def _secret(*names: str) -> str:
    """Look up a secret tolerantly, top level first then one level into each section.

    Same approach as sms_health.py: keys pasted below a [section] header in
    secrets.toml belong to that section, which is an easy way to end up with a
    secret the app cannot see.
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


TT_URL = (_secret("TT_SUPABASE_URL", "OPTOUT_SUPABASE_URL", "optout_supabase_url") or "").rstrip("/")
TT_READ_KEY = _secret("TT_SUPABASE_KEY", "OPTOUT_SUPABASE_KEY", "optout_supabase_key")
TT_WRITE_KEY = _secret("TT_SUPABASE_SERVICE_KEY", "tt_supabase_service_key")
ACTOR = _secret("AUDIT_ACTOR", "audit_actor") or "audit-app"

FREQUENCY_LABELS = {"daily": "Daily", "weekly": "Weekly", "monthly": "Monthly",
                    "quarterly": "Quarterly", "every_n_days": "Every N days"}
WEEKDAYS = {1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday",
            5: "Friday", 6: "Saturday", 7: "Sunday"}


def _configured() -> bool:
    return bool(TT_URL and TT_READ_KEY)


def _can_write() -> bool:
    return bool(TT_URL and TT_WRITE_KEY)


def _headers(write: bool = False) -> dict:
    key = TT_WRITE_KEY if write else TT_READ_KEY
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


@st.cache_data(ttl=60, show_spinner=False)
def _fetch(table: str, params: dict) -> pd.DataFrame:
    """GET one table via PostgREST. Returns an empty frame rather than raising, so one
    bad panel never takes the page down."""
    try:
        res = requests.get(
            f"{TT_URL}/rest/v1/{table}",
            headers=_headers(),
            params={**params, "limit": params.get("limit", 2000)},
            timeout=30,
        )
        res.raise_for_status()
        return pd.DataFrame(res.json())
    except Exception as exc:  # noqa: BLE001 - shown in the UI, not swallowed
        st.error(f"Could not read `{table}`: {exc}")
        return pd.DataFrame()


def _clear_cache() -> None:
    _fetch.clear()


def _patch(table: str, row_id, payload: dict, expected_updated_at=None) -> tuple[bool, str]:
    """PATCH one row. When expected_updated_at is given the write is refused if the row
    has changed since it was loaded, instead of silently overwriting someone else's edit.
    """
    if not _can_write():
        return False, "no write key configured"
    params = {"id": f"eq.{row_id}"}
    if expected_updated_at:
        params["updated_at"] = f"eq.{expected_updated_at}"
    try:
        res = requests.patch(
            f"{TT_URL}/rest/v1/{table}",
            headers={**_headers(write=True), "Prefer": "return=representation"},
            params=params,
            data=json.dumps(payload),
            timeout=30,
        )
        res.raise_for_status()
        rows = res.json()
        if not rows:
            return False, "row changed since it was loaded, reload and try again"
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _rpc(fn: str, payload: dict | None = None) -> tuple[bool, str]:
    if not _can_write():
        return False, "no write key configured"
    try:
        res = requests.post(
            f"{TT_URL}/rest/v1/rpc/{fn}",
            headers=_headers(write=True),
            data=json.dumps(payload or {}),
            timeout=60,
        )
        res.raise_for_status()
        return True, res.text
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _grid_key(prefix: str, ids) -> str:
    """st.data_editor stores edits by row POSITION, so the widget has to be re-keyed
    whenever the underlying row set changes or stale edits land on the wrong rows."""
    digest = hashlib.md5(",".join(str(i) for i in ids).encode()).hexdigest()[:10]
    return f"{prefix}_{digest}"


def _csv_download(df: pd.DataFrame, stem: str, key: str) -> None:
    if df is None or df.empty:
        st.caption("Nothing to export for the current filters.")
        return
    st.download_button(
        label=f"Download CSV ({len(df):,} rows)",
        data=df.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"{stem}_{local_today().isoformat()}.csv",
        mime="text/csv",
        key=key,
    )


# --------------------------------------------------------------------------------------
# Loaders
# --------------------------------------------------------------------------------------

def _load_options(field: str) -> list[str]:
    df = _fetch("tt_field_options", {
        "select": "value,sort_order,active",
        "field": f"eq.{field}",
        "active": "is.true",
        "order": "sort_order.asc",
    })
    return [] if df.empty else df["value"].tolist()


def _load_clients() -> pd.DataFrame:
    return _fetch("tt_clients", {"select": "id,name,active", "order": "name.asc"})


def _load_members() -> pd.DataFrame:
    return _fetch("tt_members", {"select": "id,name,active", "order": "name.asc"})


def _load_defs() -> pd.DataFrame:
    return _fetch("tt_recurring_defs", {
        "select": ("id,title,description,category,priority,client_id,frequency,interval_n,"
                   "weekday,day_of_month,grace_days,active,paused_until,next_due_date,"
                   "last_generated_on,source,links,notes,report_sheet_url,recipients,updated_at"),
        "order": "frequency.asc,title.asc",
    })


def _load_instances() -> pd.DataFrame:
    return _fetch("tt_tasks", {
        "select": ("id,title,description,category,status,priority,due_date,client_id,source,"
                   "links,notes,recurring_def_id,occurrence_date,report_sheet_url,completed_at,"
                   "updated_at"),
        "recurring_def_id": "not.is.null",
        "order": "due_date.asc,title.asc",
    })


def _load_completions() -> pd.DataFrame:
    return _fetch("tt_task_completions", {
        "select": "recurring_def_id,occurrence_date,completed_at,on_time",
        "order": "completed_at.desc",
    })


def _load_assignees() -> pd.DataFrame:
    return _fetch("tt_task_assignees", {"select": "task_id,member_id"})


# --------------------------------------------------------------------------------------
# Instance grid
# --------------------------------------------------------------------------------------

INSTANCE_READONLY = ["Title", "Description", "Type", "Client", "Source", "Sheet"]


def _instance_frame(inst: pd.DataFrame, defs: pd.DataFrame, clients: pd.DataFrame,
                    members: pd.DataFrame, assignees: pd.DataFrame) -> pd.DataFrame:
    """Shape the instances into the column set the Airtable page uses."""
    if inst.empty:
        return pd.DataFrame()

    freq_by_def = defs.set_index("id")["frequency"].to_dict() if not defs.empty else {}
    client_by_id = clients.set_index("id")["name"].to_dict() if not clients.empty else {}
    member_by_id = members.set_index("id")["name"].to_dict() if not members.empty else {}

    first_assignee = {}
    if not assignees.empty:
        for _, row in assignees.iterrows():
            first_assignee.setdefault(row["task_id"], member_by_id.get(row["member_id"], ""))

    out = pd.DataFrame({
        "id": inst["id"],
        "Done": inst["status"].eq("done"),
        "Title": inst["title"],
        "Description": inst["description"].fillna(""),
        "Type": inst["recurring_def_id"].map(lambda d: FREQUENCY_LABELS.get(freq_by_def.get(d), "")),
        "Priority": inst["priority"],
        "Status": inst["status"],
        "Due Date": pd.to_datetime(inst["due_date"], errors="coerce").dt.date,
        "Assigned To": inst["id"].map(lambda i: first_assignee.get(i, "")),
        "Client": inst["client_id"].map(lambda c: client_by_id.get(c, "")),
        "Source": inst["source"].fillna(""),
        "Notes": inst["notes"].fillna(""),
        "Links": inst["links"].fillna(""),
        "Sheet": inst["report_sheet_url"].fillna(""),
        "_updated_at": inst["updated_at"],
        "_freq": inst["recurring_def_id"].map(lambda d: freq_by_def.get(d, "")),
    })
    return out


def _render_instance_grid(frame: pd.DataFrame, statuses: list[str], priorities: list[str],
                          member_names: list[str], key_prefix: str) -> None:
    if frame.empty:
        st.info("Nothing scheduled here yet. Generated tasks appear once their cycle comes round.")
        return

    display_cols = ["Done", "Title", "Description", "Type", "Priority", "Status", "Due Date",
                    "Assigned To", "Client", "Source", "Notes", "Links", "Sheet"]
    grid = frame[["id"] + display_cols].copy()

    edited = st.data_editor(
        grid,
        hide_index=True,
        use_container_width=True,
        disabled=["id"] + INSTANCE_READONLY,
        column_config={
            "id": None,
            "Done": st.column_config.CheckboxColumn(
                "Done", help="Ticking this logs a completion and computes whether it was on time",
                width="small"),
            "Title": st.column_config.TextColumn("Title", width="large"),
            "Description": st.column_config.TextColumn("Description", width="medium"),
            "Type": st.column_config.TextColumn("Type", width="small"),
            "Priority": st.column_config.SelectboxColumn("Priority", options=priorities, width="small"),
            "Status": st.column_config.SelectboxColumn("Status", options=statuses, width="medium"),
            "Due Date": st.column_config.DateColumn("Due Date", format="YYYY-MM-DD", width="small"),
            "Assigned To": st.column_config.SelectboxColumn(
                "Assigned To", options=[""] + member_names, width="small"),
            "Client": st.column_config.TextColumn("Client", width="medium"),
            "Source": st.column_config.TextColumn("Source", width="small"),
            "Notes": st.column_config.TextColumn("Notes", width="large"),
            "Links": st.column_config.LinkColumn("Links", width="small"),
            "Sheet": st.column_config.LinkColumn("Report sheet", width="small"),
        },
        key=_grid_key(key_prefix, grid["id"].tolist()),
    )

    changed = _diff(grid, edited, display_cols)
    col_a, col_b = st.columns([1, 4])
    with col_a:
        save = st.button(
            f"Save {len(changed)} change{'s' if len(changed) != 1 else ''}",
            type="primary",
            disabled=not changed or not _can_write(),
            key=f"save_{key_prefix}",
        )
    with col_b:
        if changed and not _can_write():
            st.caption("Read-only: add TT_SUPABASE_SERVICE_KEY to the app secrets to save edits.")
        elif changed:
            st.caption(f"{len(changed)} row{'s' if len(changed) != 1 else ''} edited, not yet saved.")

    if save:
        _save_instance_changes(frame, changed)

    st.divider()
    _csv_download(edited.drop(columns=["id"], errors="ignore"), f"recurring_{key_prefix}",
                  f"dl_{key_prefix}")


def _diff(before: pd.DataFrame, after: pd.DataFrame, cols: list[str]) -> dict:
    """Return {row_id: {column: new_value}} for cells that actually changed."""
    changed: dict = {}
    if after is None or after.empty:
        return changed
    for pos, row_id in enumerate(before["id"].tolist()):
        for col in cols:
            try:
                old, new = before.iloc[pos][col], after.iloc[pos][col]
            except Exception:
                continue
            if pd.isna(old) and pd.isna(new):
                continue
            if str(old) != str(new):
                changed.setdefault(row_id, {})[col] = new
    return changed


def _save_instance_changes(frame: pd.DataFrame, changed: dict) -> None:
    by_id = frame.set_index("id")
    ok, failed = 0, []

    for row_id, edits in changed.items():
        stamp = by_id.loc[row_id, "_updated_at"] if row_id in by_id.index else None

        # "Done" is not a column write, it is a completion event, so it goes through the
        # function that also writes the completion log and the on-time verdict.
        if "Done" in edits:
            if bool(edits["Done"]):
                good, msg = _rpc("tt_complete_task", {"p_task_id": int(row_id), "p_actor": ACTOR})
            else:
                good, msg = _patch("tt_tasks", row_id, {"status": "todo", "completed_at": None}, stamp)
            if good:
                ok += 1
            else:
                failed.append(f"row {row_id}: {msg}")
            edits = {k: v for k, v in edits.items() if k != "Done"}
            stamp = None  # the row just moved on, so do not guard the rest of this edit

        if not edits:
            continue

        payload = {}
        for col, val in edits.items():
            if col == "Status":
                payload["status"] = val
            elif col == "Priority":
                payload["priority"] = val
            elif col == "Due Date":
                payload["due_date"] = str(val) if val else None
            elif col == "Notes":
                payload["notes"] = val or None
            elif col == "Links":
                payload["links"] = val or None
            elif col == "Assigned To":
                _set_assignee(row_id, val)
        if payload:
            good, msg = _patch("tt_tasks", row_id, payload, stamp)
            if good:
                ok += 1
            else:
                failed.append(f"row {row_id}: {msg}")

    if ok:
        st.success(f"Saved {ok} change{'s' if ok != 1 else ''}.")
    for f in failed:
        st.error(f)
    _clear_cache()
    st.rerun()


def _set_assignee(task_id, member_name: str) -> None:
    members = _load_members()
    if not _can_write():
        return
    try:
        requests.delete(
            f"{TT_URL}/rest/v1/tt_task_assignees",
            headers=_headers(write=True),
            params={"task_id": f"eq.{task_id}"},
            timeout=30,
        )
        if member_name:
            match = members[members["name"] == member_name]
            if not match.empty:
                requests.post(
                    f"{TT_URL}/rest/v1/tt_task_assignees",
                    headers=_headers(write=True),
                    data=json.dumps({"task_id": int(task_id),
                                     "member_id": int(match.iloc[0]["id"])}),
                    timeout=30,
                )
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not set assignee: {exc}")


# --------------------------------------------------------------------------------------
# Schedules grid
# --------------------------------------------------------------------------------------

def _render_schedules(defs: pd.DataFrame, clients: pd.DataFrame, completions: pd.DataFrame,
                      priorities: list[str]) -> None:
    if defs.empty:
        st.info("No recurring schedules defined yet.")
        return

    client_by_id = clients.set_index("id")["name"].to_dict() if not clients.empty else {}

    rate = {}
    if not completions.empty:
        grp = completions.dropna(subset=["recurring_def_id"]).groupby("recurring_def_id")["on_time"]
        for def_id, series in grp:
            done = series.notna().sum()
            rate[def_id] = f"{int(series.sum())}/{int(done)}" if done else "-"

    def _anchor(row) -> str:
        if row["frequency"] == "weekly":
            return WEEKDAYS.get(row["weekday"], "not set")
        if row["frequency"] in ("monthly", "quarterly"):
            return f"day {int(row['day_of_month'])}" if pd.notna(row["day_of_month"]) else "not set"
        return "every day"

    grid = pd.DataFrame({
        "id": defs["id"],
        "Active": defs["active"],
        "Title": defs["title"],
        "Type": defs["frequency"].map(lambda f: FREQUENCY_LABELS.get(f, f)),
        "Anchor": defs.apply(_anchor, axis=1),
        "Priority": defs["priority"],
        "Client": defs["client_id"].map(lambda c: client_by_id.get(c, "")),
        "Next due": pd.to_datetime(defs["next_due_date"], errors="coerce").dt.date,
        "Grace days": defs["grace_days"],
        "On time": defs["id"].map(lambda i: rate.get(i, "-")),
        "Notes": defs["notes"].fillna(""),
    })

    display_cols = ["Active", "Title", "Type", "Anchor", "Priority", "Client", "Next due",
                    "Grace days", "On time", "Notes"]

    edited = st.data_editor(
        grid[["id"] + display_cols],
        hide_index=True,
        use_container_width=True,
        disabled=["id", "Title", "Type", "Anchor", "Client", "On time"],
        column_config={
            "id": None,
            "Active": st.column_config.CheckboxColumn("Active", width="small"),
            "Title": st.column_config.TextColumn("Title", width="large"),
            "Type": st.column_config.TextColumn("Type", width="small"),
            "Anchor": st.column_config.TextColumn("Runs on", width="small"),
            "Priority": st.column_config.SelectboxColumn("Priority", options=priorities, width="small"),
            "Client": st.column_config.TextColumn("Client", width="medium"),
            "Next due": st.column_config.DateColumn("Next due", format="YYYY-MM-DD", width="small"),
            "Grace days": st.column_config.NumberColumn("Grace", min_value=0, max_value=31, width="small"),
            "On time": st.column_config.TextColumn("On time", width="small",
                                                   help="Cycles completed on time out of cycles completed"),
            "Notes": st.column_config.TextColumn("Notes", width="large"),
        },
        key=_grid_key("sched", grid["id"].tolist()),
    )

    changed = _diff(grid[["id"] + display_cols], edited, display_cols)
    c1, c2 = st.columns([1, 4])
    with c1:
        save = st.button(f"Save {len(changed)} change{'s' if len(changed) != 1 else ''}",
                         type="primary", disabled=not changed or not _can_write(), key="save_sched")
    with c2:
        if changed and not _can_write():
            st.caption("Read-only: add TT_SUPABASE_SERVICE_KEY to the app secrets to save edits.")

    if save:
        stamps = defs.set_index("id")["updated_at"]
        ok, failed = 0, []
        for row_id, edits in changed.items():
            payload = {}
            for col, val in edits.items():
                if col == "Active":
                    payload["active"] = bool(val)
                elif col == "Priority":
                    payload["priority"] = val
                elif col == "Next due":
                    payload["next_due_date"] = str(val) if val else None
                elif col == "Grace days":
                    payload["grace_days"] = int(val or 0)
                elif col == "Notes":
                    payload["notes"] = val or None
            if payload:
                good, msg = _patch("tt_recurring_defs", row_id, payload, stamps.get(row_id))
                ok += 1 if good else 0
                if not good:
                    failed.append(f"schedule {row_id}: {msg}")
        if ok:
            st.success(f"Saved {ok} change{'s' if ok != 1 else ''}.")
        for f in failed:
            st.error(f)
        _clear_cache()
        st.rerun()

    st.divider()
    _csv_download(edited.drop(columns=["id"], errors="ignore"), "recurring_schedules", "dl_sched")


# --------------------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------------------

def render_recurring() -> None:
    if not _configured():
        st.warning(
            "Not connected. This tab needs `OPTOUT_SUPABASE_URL` and `OPTOUT_SUPABASE_KEY` "
            "(or `TT_SUPABASE_URL` / `TT_SUPABASE_KEY`) in the app secrets."
        )
        return

    defs = _load_defs()
    inst = _load_instances()
    clients = _load_clients()
    members = _load_members()
    completions = _load_completions()
    assignees = _load_assignees()

    statuses = _load_options("status") or ["new", "todo", "in_progress", "waiting", "done"]
    priorities = _load_options("priority") or ["P1", "P2", "P3"]
    member_names = members["name"].tolist() if not members.empty else []

    frame = _instance_frame(inst, defs, clients, members, assignees)
    today = local_today()

    open_rows = frame[~frame["Done"]] if not frame.empty else frame
    overdue = open_rows[open_rows["Due Date"].apply(lambda d: bool(d) and d < today)] \
        if not open_rows.empty else open_rows
    due_today = open_rows[open_rows["Due Date"].apply(lambda d: d == today)] \
        if not open_rows.empty else open_rows
    done_today = frame[frame["Done"]] if not frame.empty else frame

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Open", 0 if open_rows is None or open_rows.empty else len(open_rows))
    m2.metric("Due today", 0 if due_today is None or due_today.empty else len(due_today))
    m3.metric("Overdue", 0 if overdue is None or overdue.empty else len(overdue),
              help="Past their due date and not yet ticked off")
    m4.metric("Schedules", 0 if defs.empty else int(defs["active"].sum()),
              help="Active recurring definitions")

    head_l, head_r = st.columns([3, 1])
    with head_l:
        st.caption(
            f"Times shown in Pacific. Today is {today.isoformat()}. "
            "New instances are created automatically each hour."
        )
    with head_r:
        if st.button("Generate due tasks now", use_container_width=True, disabled=not _can_write()):
            good, msg = _rpc("tt_generate_due_instances")
            if good:
                created = msg.strip()
                st.success(f"Generator ran, {created} task(s) created.")
                _clear_cache()
                st.rerun()
            else:
                st.error(f"Could not run the generator: {msg}")

    if not _can_write():
        st.info(
            "Viewing in read-only mode. Add `TT_SUPABASE_SERVICE_KEY` to the app secrets "
            "to edit, tick off and generate from here.",
            icon=None,
        )

    tab_all, tab_daily, tab_weekly, tab_monthly, tab_sched = st.tabs(
        ["All", "Daily", "Weekly", "Monthly", "Schedules"]
    )

    def _subset(freq: str) -> pd.DataFrame:
        if frame.empty:
            return frame
        return frame[frame["_freq"] == freq]

    with tab_all:
        _render_instance_grid(frame, statuses, priorities, member_names, "all")
    with tab_daily:
        _render_instance_grid(_subset("daily"), statuses, priorities, member_names, "daily")
    with tab_weekly:
        _render_instance_grid(_subset("weekly"), statuses, priorities, member_names, "weekly")
    with tab_monthly:
        _render_instance_grid(_subset("monthly"), statuses, priorities, member_names, "monthly")
    with tab_sched:
        st.caption(
            "The definitions behind the tasks. Editing one changes every future instance, "
            "not the ones already generated."
        )
        _render_schedules(defs, clients, completions, priorities)
