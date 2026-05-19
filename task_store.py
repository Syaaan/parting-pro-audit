"""Task store backed by Airtable (base appTv7HOVgk2hBEBG).

Public API (drop-in replacement for the previous JSON-file version):
  load_tasks() -> list[dict]
  add_task(data: dict) -> dict
  update_task(task_id: str, updates: dict)
  delete_task(task_id: str)
  reset_recurring_tasks()

New API (team / assignment support):
  load_members(active_only: bool = False) -> list[dict]
  add_member(data: dict) -> dict
  update_member(member_id: str, updates: dict)
  delete_member(member_id: str)
"""

from __future__ import annotations

import os
import requests
from datetime import date, datetime, timedelta

try:
    import streamlit as st
    _HAS_ST = True
except Exception:
    st = None
    _HAS_ST = False


BASE_ID = "appTv7HOVgk2hBEBG"
TASKS_TABLE = "tblaF1j6oS9s9IAUz"
MEMBERS_TABLE = "tblpNgRJHUe9vV4G8"

TYPE_NAME = {
    "daily":   "daily ",
    "weekly":  "weekly ",
    "monthly": "monthly ",
    "one-off": "one-off",
}
TYPE_FROM_NAME = {v: k for k, v in TYPE_NAME.items()}
# Map Python statuses ("todo" / "done") to the Airtable single-select option names.
# The Airtable Status field has been expanded beyond two states, but we keep the
# Streamlit app's two-state view ("not done" vs "done"):
#   - Writes from Streamlit: "todo" -> "to do",  "done" -> "completed"
#   - Reads from Airtable: any non-"completed" status is treated as "todo".
STATUS_NAME = {"todo": "to do", "done": "completed"}
STATUS_FROM_NAME = {
    "to do":      "todo",
    "completed":  "done",
    # Other Airtable workflow states show up as "todo" (not done) on the Streamlit side
    "new":        "todo",
    "inprogress": "todo",
    "waiting for something / doc / file / reply": "todo",
    # Legacy values (just in case any old records linger)
    "todo ":      "todo",
    "done":       "done",
}

_AIRTABLE = "https://api.airtable.com/v0"

_LEGACY_TOKEN = (
    "patm2acj3jyDwBfyD."
    "3fb175e7596542e2a9be3acc07700272cf8cb09028c58cc03a6d8bc5be022542"
)


def _get_token():
    if _HAS_ST:
        try:
            tok = st.secrets.get("AIRTABLE_TOKEN")
            if tok:
                return tok
        except Exception:
            pass
    env = os.environ.get("AIRTABLE_TOKEN")
    if env:
        return env
    return _LEGACY_TOKEN


def _headers():
    return {
        "Authorization": "Bearer " + _get_token(),
        "Content-Type": "application/json",
    }


def _request(method, url, **kwargs):
    r = requests.request(method, url, headers=_headers(), timeout=30, **kwargs)
    if r.status_code >= 400:
        try:
            err = r.json()
        except Exception:
            err = {"error": r.text}
        if isinstance(err.get("error"), dict):
            msg = err["error"].get("message") or err["error"].get("type") or str(err)
        else:
            msg = err.get("error") or str(err)
        raise RuntimeError("Airtable " + method + " -> " + str(r.status_code) + ": " + str(msg))
    if r.status_code == 204 or not r.content:
        return {}
    return r.json()


def _list_all(table_id):
    records = []
    offset = None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        data = _request("GET", _AIRTABLE + "/" + BASE_ID + "/" + table_id, params=params)
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return records


if _HAS_ST:
    @st.cache_data(ttl=15, show_spinner=False)
    def _fetch_tasks_raw():
        return _list_all(TASKS_TABLE)

    @st.cache_data(ttl=15, show_spinner=False)
    def _fetch_members_raw():
        return _list_all(MEMBERS_TABLE)

    def _invalidate_tasks_cache():
        _fetch_tasks_raw.clear()

    def _invalidate_members_cache():
        _fetch_members_raw.clear()
else:
    def _fetch_tasks_raw():
        return _list_all(TASKS_TABLE)

    def _fetch_members_raw():
        return _list_all(MEMBERS_TABLE)

    def _invalidate_tasks_cache():
        pass

    def _invalidate_members_cache():
        pass


def _parse_task(rec):
    f = rec.get("fields", {})
    return {
        "id": rec["id"],
        "title": f.get("Title", ""),
        "description": f.get("Description", ""),
        "type": TYPE_FROM_NAME.get(f.get("Type", ""), "one-off"),
        "priority": f.get("Priority", "P2"),
        "status": STATUS_FROM_NAME.get(f.get("Status", ""), "todo"),
        "source": f.get("Source", "manual"),
        "due_date": f.get("Due Date") or None,
        "created_at": f.get("Created At") or rec.get("createdTime"),
        "completed_at": f.get("Completed At") or None,
        "recurrence_last_reset": f.get("Recurrence Last Reset") or None,
        "assignee_ids": f.get("Assigned To", []) or [],
    }


def _parse_member(rec):
    f = rec.get("fields", {})
    return {
        "id": rec["id"],
        "name": f.get("Name", ""),
        "email": f.get("Email", ""),
        "role": f.get("Role", ""),
        "active": bool(f.get("Active", False)),
    }


def load_tasks():
    try:
        return [_parse_task(r) for r in _fetch_tasks_raw()]
    except Exception as e:
        if _HAS_ST:
            st.error("Could not load tasks from Airtable: " + str(e))
        return []


def add_task(data):
    fields = {
        "Title": data.get("title", ""),
        "Description": data.get("description", ""),
        "Type": TYPE_NAME.get(data.get("type", "one-off"), "one-off"),
        "Priority": data.get("priority", "P2"),
        "Status": STATUS_NAME.get(data.get("status", "todo"), "todo "),
        "Source": data.get("source", "manual"),
        "Created At": datetime.now().isoformat(),
        "Recurrence Last Reset": date.today().isoformat(),
    }
    if data.get("due_date"):
        fields["Due Date"] = str(data["due_date"])
    if data.get("assignee_ids"):
        fields["Assigned To"] = list(data["assignee_ids"])

    result = _request(
        "POST",
        _AIRTABLE + "/" + BASE_ID + "/" + TASKS_TABLE,
        json={"fields": fields},
    )
    _invalidate_tasks_cache()
    return _parse_task(result)


def update_task(task_id, updates):
    fields = {}
    if "title" in updates:
        fields["Title"] = updates["title"]
    if "description" in updates:
        fields["Description"] = updates["description"]
    if "type" in updates:
        fields["Type"] = TYPE_NAME.get(updates["type"], "one-off")
    if "priority" in updates:
        fields["Priority"] = updates["priority"]
    if "source" in updates:
        fields["Source"] = updates["source"]
    if "due_date" in updates:
        fields["Due Date"] = str(updates["due_date"]) if updates["due_date"] else ""
    if "recurrence_last_reset" in updates:
        fields["Recurrence Last Reset"] = updates["recurrence_last_reset"] or ""
    if "assignee_ids" in updates:
        fields["Assigned To"] = list(updates["assignee_ids"] or [])

    if "status" in updates:
        new_status = updates["status"]
        fields["Status"] = STATUS_NAME.get(new_status, "todo ")
        if new_status == "done":
            if "completed_at" in updates and updates["completed_at"]:
                fields["Completed At"] = str(updates["completed_at"])
            else:
                fields["Completed At"] = datetime.now().isoformat()
        else:
            fields["Completed At"] = ""
    elif "completed_at" in updates:
        fields["Completed At"] = updates["completed_at"] or ""

    if not fields:
        return

    _request(
        "PATCH",
        _AIRTABLE + "/" + BASE_ID + "/" + TASKS_TABLE + "/" + task_id,
        json={"fields": fields},
    )
    _invalidate_tasks_cache()


def delete_task(task_id):
    _request("DELETE", _AIRTABLE + "/" + BASE_ID + "/" + TASKS_TABLE + "/" + task_id)
    _invalidate_tasks_cache()


def reset_recurring_tasks():
    try:
        tasks = load_tasks()
    except Exception:
        return

    today = date.today()
    days_since_monday = today.weekday()
    this_monday = today - timedelta(days=days_since_monday)

    for t in tasks:
        if t.get("status") != "done":
            continue
        task_type = t.get("type", "one-off")
        if task_type == "one-off":
            continue

        raw = t.get("recurrence_last_reset")
        try:
            last_reset = date.fromisoformat(raw) if raw else today
        except Exception:
            last_reset = today

        should_reset = False
        if task_type == "daily":
            should_reset = last_reset < today
        elif task_type == "weekly":
            should_reset = last_reset < this_monday
        elif task_type == "monthly":
            should_reset = (last_reset.year < today.year) or (
                last_reset.year == today.year and last_reset.month < today.month
            )

        if should_reset:
            try:
                update_task(t["id"], {
                    "status": "todo",
                    "completed_at": None,
                    "recurrence_last_reset": today.isoformat(),
                })
            except Exception:
                continue


def load_members(active_only=False):
    try:
        members = [_parse_member(r) for r in _fetch_members_raw()]
    except Exception as e:
        if _HAS_ST:
            st.error("Could not load team members from Airtable: " + str(e))
        return []
    if active_only:
        members = [m for m in members if m["active"]]
    return members


def add_member(data):
    fields = {
        "Name": data.get("name", ""),
        "Email": data.get("email", ""),
        "Role": data.get("role", ""),
        "Active": bool(data.get("active", True)),
    }
    result = _request(
        "POST",
        _AIRTABLE + "/" + BASE_ID + "/" + MEMBERS_TABLE,
        json={"fields": fields},
    )
    _invalidate_members_cache()
    return _parse_member(result)


def update_member(member_id, updates):
    fields = {}
    if "name" in updates:
        fields["Name"] = updates["name"]
    if "email" in updates:
        fields["Email"] = updates["email"]
    if "role" in updates:
        fields["Role"] = updates["role"]
    if "active" in updates:
        fields["Active"] = bool(updates["active"])
    if not fields:
        return
    _request(
        "PATCH",
        _AIRTABLE + "/" + BASE_ID + "/" + MEMBERS_TABLE + "/" + member_id,
        json={"fields": fields},
    )
    _invalidate_members_cache()


def delete_member(member_id):
    _request("DELETE", _AIRTABLE + "/" + BASE_ID + "/" + MEMBERS_TABLE + "/" + member_id)
    _invalidate_members_cache()
