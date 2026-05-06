"""
task_store.py — Persistent task storage backed by Airtable.
Falls back to local JSON only if Airtable is not reachable.
"""
import json
import uuid
import requests
import streamlit as st
from datetime import date, datetime, timedelta
from pathlib import Path

# ── Constants ─────────────────────────────────────────────────────────────────
# Use table ID directly so it works regardless of the table's display name
TASKS_TABLE      = "tblaF1j6oS9s9IAUz"
_DEFAULT_BASE_ID = "appTv7HOVgk2hBEBG"
_LOCAL_FILE      = Path(__file__).parent / "tasks.json"

# ── Secrets ───────────────────────────────────────────────────────────────────
def _secret(key, default=""):
    try:
        return st.secrets[key]
    except Exception:
        return default

def _token():
    return _secret("AIRTABLE_TOKEN")

def _base_id():
    return _secret("TASKS_BASE_ID") or _DEFAULT_BASE_ID

def _headers():
    return {
        "Authorization": f"Bearer {_token()}",
        "Content-Type":  "application/json",
    }

def _table_url():
    return (
        f"https://api.airtable.com/v0/{_base_id()}/"
        f"{requests.utils.quote(TASKS_TABLE)}"
    )

def _use_airtable():
    return bool(_token())

# ── Airtable ↔ task dict ──────────────────────────────────────────────────────
def _rec_to_task(rec: dict) -> dict:
    f = rec.get("fields", {})
    return {
        "id":                    rec["id"],
        "title":                 f.get("Title", ""),
        "description":           f.get("Description", ""),
        "type":                  f.get("Type", "one-off"),
        "priority":              f.get("Priority", "P2"),
        "status":                f.get("Status", "todo"),
        "source":                f.get("Source", "manual"),
        "due_date":              f.get("Due Date") or None,
        "created_at":            f.get("Created At", ""),
        "completed_at":          f.get("Completed At") or None,
        "recurrence_last_reset": f.get("Recurrence Last Reset") or date.today().isoformat(),
    }

def _fields_from(data: dict) -> dict:
    fields = {
        "Title":    data.get("title", "Untitled"),
        "Type":     data.get("type",     "one-off"),
        "Priority": data.get("priority", "P2"),
        "Status":   data.get("status",   "todo"),
        "Source":   data.get("source",   "manual"),
    }
    for py_key, at_key in [
        ("description",           "Description"),
        ("due_date",              "Due Date"),
        ("created_at",            "Created At"),
        ("completed_at",          "Completed At"),
        ("recurrence_last_reset", "Recurrence Last Reset"),
    ]:
        val = data.get(py_key)
        if val is not None:
            fields[at_key] = str(val)
    return fields

# ── Auto-create Tasks table ───────────────────────────────────────────────────
def _create_tasks_table() -> bool:
    """Attempt to create the Tasks table via Airtable Meta API. Returns True on success."""
    url = f"https://api.airtable.com/v0/meta/bases/{_base_id()}/tables"
    payload = {
        "name": TASKS_TABLE,
        "fields": [
            {"name": "Title",                  "type": "singleLineText"},
            {"name": "Description",            "type": "multilineText"},
            {"name": "Type",                   "type": "singleSelect",
             "options": {"choices": [{"name": c} for c in ["daily","weekly","monthly","one-off"]]}},
            {"name": "Priority",               "type": "singleSelect",
             "options": {"choices": [{"name": c} for c in ["P1","P2","P3"]]}},
            {"name": "Status",                 "type": "singleSelect",
             "options": {"choices": [{"name": c} for c in ["todo","done"]]}},
            {"name": "Source",                 "type": "singleLineText"},
            {"name": "Due Date",               "type": "singleLineText"},
            {"name": "Created At",             "type": "singleLineText"},
            {"name": "Completed At",           "type": "singleLineText"},
            {"name": "Recurrence Last Reset",  "type": "singleLineText"},
        ],
    }
    r = requests.post(url, headers=_headers(), json=payload, timeout=15)
    return r.status_code in (200, 201)

# ── Public API ────────────────────────────────────────────────────────────────
def load_tasks() -> list:
    if not _use_airtable():
        return _local_load()
    try:
        records, offset = [], None
        while True:
            params = {"pageSize": 100}
            if offset:
                params["offset"] = offset
            r = requests.get(_table_url(), headers=_headers(), params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
        return [_rec_to_task(rec) for rec in records]
    except Exception:
        return _local_load()


def add_task(data: dict) -> dict:
    task_data = {
        "title":                 data.get("title", ""),
        "description":           data.get("description", ""),
        "type":                  data.get("type", "one-off"),
        "priority":              data.get("priority", "P2"),
        "status":                "todo",
        "source":                data.get("source", "manual"),
        "due_date":              data.get("due_date") or None,
        "created_at":            datetime.now().isoformat(),
        "completed_at":          None,
        "recurrence_last_reset": date.today().isoformat(),
    }
    if not _use_airtable():
        return _local_add(task_data)
    r = requests.post(
        _table_url(), headers=_headers(),
        json={"fields": _fields_from(task_data)}, timeout=15,
    )
    if not r.ok:
        # Raise with full Airtable error so the UI can show it
        raise RuntimeError(f"Airtable error {r.status_code}: {r.text}")
    return _rec_to_task(r.json())


def update_task(task_id: str, updates: dict):
    if _use_airtable() and task_id.startswith("rec"):
        try:
            fields = {}
            mapping = {
                "title":                 "Title",
                "description":           "Description",
                "type":                  "Type",
                "priority":              "Priority",
                "status":                "Status",
                "source":                "Source",
                "due_date":              "Due Date",
                "created_at":            "Created At",
                "completed_at":          "Completed At",
                "recurrence_last_reset": "Recurrence Last Reset",
            }
            for py_k, at_k in mapping.items():
                if py_k in updates:
                    fields[at_k] = str(updates[py_k]) if updates[py_k] is not None else ""

            if updates.get("status") == "done":
                fields["Completed At"] = datetime.now().isoformat()
            elif "status" in updates and updates["status"] != "done":
                fields["Completed At"] = ""

            if fields:
                r = requests.patch(
                    f"{_table_url()}/{task_id}",
                    headers=_headers(), json={"fields": fields}, timeout=15,
                )
                r.raise_for_status()
            return
        except Exception:
            pass
    # Local fallback
    tasks = _local_load()
    for t in tasks:
        if t["id"] == task_id:
            t.update(updates)
            if updates.get("status") == "done" and not t.get("completed_at"):
                t["completed_at"] = datetime.now().isoformat()
            elif "status" in updates and updates["status"] != "done":
                t["completed_at"] = None
            break
    _local_save(tasks)


def delete_task(task_id: str):
    if _use_airtable() and task_id.startswith("rec"):
        try:
            r = requests.delete(
                f"{_table_url()}/{task_id}",
                headers=_headers(), timeout=15,
            )
            r.raise_for_status()
            return
        except Exception:
            pass
    tasks = _local_load()
    _local_save([t for t in tasks if t["id"] != task_id])


def reset_recurring_tasks():
    tasks = load_tasks()
    today         = date.today()
    this_monday   = today - timedelta(days=today.weekday())

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
            should_reset = (last_reset.year, last_reset.month) < (today.year, today.month)

        if should_reset:
            update_task(t["id"], {
                "status":                "todo",
                "completed_at":          None,
                "recurrence_last_reset": today.isoformat(),
            })


# ── Local JSON fallback ───────────────────────────────────────────────────────
def _local_load() -> list:
    if not _LOCAL_FILE.exists():
        return []
    try:
        return json.loads(_LOCAL_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []

def _local_save(tasks: list):
    _LOCAL_FILE.write_text(json.dumps(tasks, indent=2, default=str), encoding="utf-8")

def _local_add(task_data: dict) -> dict:
    task_data["id"] = str(uuid.uuid4())
    tasks = _local_load()
    tasks.append(task_data)
    _local_save(tasks)
    return task_data
