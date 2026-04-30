import json
import uuid
from pathlib import Path
from datetime import date, datetime, timedelta

TASKS_FILE = Path(__file__).parent / "tasks.json"


def load_tasks() -> list[dict]:
    if not TASKS_FILE.exists():
        return []
    try:
        return json.loads(TASKS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []


def save_tasks(tasks: list[dict]):
    TASKS_FILE.write_text(
        json.dumps(tasks, indent=2, default=str), encoding="utf-8"
    )


def add_task(data: dict) -> dict:
    tasks = load_tasks()
    task = {
        "id": str(uuid.uuid4()),
        "title": data.get("title", ""),
        "description": data.get("description", ""),
        "type": data.get("type", "one-off"),
        "priority": data.get("priority", "P2"),
        "status": data.get("status", "todo"),
        "source": "manual",
        "due_date": data.get("due_date") or None,
        "created_at": datetime.now().isoformat(),
        "completed_at": None,
        "recurrence_last_reset": date.today().isoformat(),
    }
    tasks.append(task)
    save_tasks(tasks)
    return task


def update_task(task_id: str, updates: dict):
    tasks = load_tasks()
    for t in tasks:
        if t["id"] == task_id:
            t.update(updates)
            if updates.get("status") == "done":
                if not t.get("completed_at"):
                    t["completed_at"] = datetime.now().isoformat()
            elif "status" in updates and updates["status"] != "done":
                t["completed_at"] = None
            break
    save_tasks(tasks)


def delete_task(task_id: str):
    tasks = load_tasks()
    tasks = [t for t in tasks if t["id"] != task_id]
    save_tasks(tasks)


def reset_recurring_tasks():
    tasks = load_tasks()
    today = date.today()
    days_since_monday = today.weekday()  # 0 = Monday
    this_monday = today - timedelta(days=days_since_monday)
    changed = False

    for t in tasks:
        if t.get("status") != "done":
            continue
        task_type = t.get("type", "one-off")
        if task_type == "one-off":
            continue

        raw = t.get("recurrence_last_reset")
        last_reset = date.fromisoformat(raw) if raw else today

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
            t["status"] = "todo"
            t["completed_at"] = None
            t["recurrence_last_reset"] = today.isoformat()
            changed = True

    if changed:
        save_tasks(tasks)
