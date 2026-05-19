"""
Wrapper to run onboarding automation from Streamlit.
Pure-Python version — no Node.js required. Runs in a background thread.
"""

from queue import Queue, Empty
from threading import Thread


class OnboardingAutomation:
    def __init__(self):
        self.output_queue = Queue()
        self.answer_queue = Queue()
        self._thread = None
        self.running = False

    def start_step(self, step_number: str) -> None:
        """Start a specific onboarding step (1-7) in a background thread."""
        if self.running:
            raise RuntimeError("Onboarding process already running")

        if step_number not in {"1", "2", "3", "4", "5", "6", "7"}:
            raise ValueError(f"Invalid step number: {step_number}")

        self.running = True
        self._thread = Thread(target=self._run, args=(step_number,), daemon=True)
        self._thread.start()

    def _run(self, step_number: str) -> None:
        try:
            from onboarding_automation import run_step
            run_step(step_number, self.output_queue, self.answer_queue)
            self.output_queue.put({"t": "done"})
        except InterruptedError:
            self.output_queue.put({"t": "done"})  # cancelled cleanly
        except Exception as e:
            self.output_queue.put({"t": "error", "m": str(e)})
        finally:
            self.running = False

    def send_answer(self, answer: str) -> None:
        """Send an answer to a waiting prompt."""
        if not self.running:
            raise RuntimeError("No active onboarding process")
        self.answer_queue.put(answer)

    def get_output(self):
        """Get next message from the queue (non-blocking). Returns None if empty."""
        try:
            return self.output_queue.get_nowait()
        except Empty:
            return None

    def stop(self) -> None:
        """Cancel the running step."""
        self.running = False
        try:
            self.answer_queue.put("__STOP__")
        except Exception:
            pass

    def is_running(self) -> bool:
        return self.running and self._thread is not None and self._thread.is_alive()


# Steps metadata (used by app.py for the UI)
STEPS = [
    {
        "key": "1",
        "title": "Review Notification & Funeral Home Record",
        "emoji": "✅",
        "description": "Review and verify funeral home details, sync to Base 2"
    },
    {
        "key": "2",
        "title": "Twilio Setup – Call Forwarding & Numbers",
        "emoji": "📞",
        "description": "Configure Twilio flows and buy/configure phone numbers"
    },
    {
        "key": "3",
        "title": "Review Form & Google Place ID Setup",
        "emoji": "📝",
        "description": "Create review forms and set up Google Place ID lookups"
    },
    {
        "key": "4",
        "title": "Finalize Airtable & Set Up Zaps",
        "emoji": "🧾",
        "description": "Complete Airtable setup and create Zapier workflows"
    },
    {
        "key": "5",
        "title": "Build the Airtable Interface",
        "emoji": "🖥️",
        "description": "Create and configure the custom Airtable interface"
    },
    {
        "key": "6",
        "title": "QA Testing",
        "emoji": "🧪",
        "description": "Run QA checks (currently inactive)"
    },
    {
        "key": "7",
        "title": "Share Interface & Activate",
        "emoji": "🚀",
        "description": "Share with funeral home and activate the system"
    },
]
