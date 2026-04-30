"""
Wrapper to run onboarding.js in web mode from Streamlit
Handles spawning the Node.js process and communicating with it via JSON
"""

import subprocess
import json
import os
from pathlib import Path
from queue import Queue
from threading import Thread

ONBOARDING_DIR = Path(__file__).parent.parent / "Airtable Audit" / "onboarding-automation"

class OnboardingAutomation:
    def __init__(self):
        self.process = None
        self.output_queue = Queue()
        self.running = False

    def start_step(self, step_number: str) -> None:
        """Start a specific onboarding step (1-7)"""
        if self.running:
            raise RuntimeError("Onboarding process already running")

        # Validate step
        if step_number not in ["1", "2", "3", "4", "5", "6", "7"]:
            raise ValueError(f"Invalid step number: {step_number}")

        env = os.environ.copy()
        env["WEB_MODE"] = "1"
        env["WEB_STEP"] = step_number
        env["NODE_PATH"] = str(ONBOARDING_DIR / "node_modules")

        try:
            self.process = subprocess.Popen(
                ["node", str(ONBOARDING_DIR / "onboarding.js")],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=str(ONBOARDING_DIR),
                env=env
            )
            self.running = True

            # Start thread to read output
            Thread(target=self._read_output, daemon=True).start()
        except FileNotFoundError:
            raise RuntimeError("Node.js not found. Please install Node.js to use onboarding automation.")
        except Exception as e:
            raise RuntimeError(f"Failed to start onboarding process: {str(e)}")

    def _read_output(self):
        """Read output from the process and queue messages"""
        try:
            while self.process and self.process.poll() is None:
                try:
                    line = self.process.stdout.readline()
                    if not line:
                        break

                    # Try to parse as JSON (web mode output)
                    try:
                        msg = json.loads(line.strip())
                        self.output_queue.put(msg)
                    except json.JSONDecodeError:
                        # Regular text output
                        self.output_queue.put({"t": "log", "m": line.strip()})
                except:
                    break

            # Mark as done
            self.output_queue.put({"t": "done"})
            self.running = False
        except Exception as e:
            self.output_queue.put({"t": "error", "m": str(e)})
            self.running = False

    def send_answer(self, answer: str) -> None:
        """Send an answer to a prompt"""
        if not self.process or not self.running:
            raise RuntimeError("No active onboarding process")

        try:
            self.process.stdin.write(answer + "\n")
            self.process.stdin.flush()
        except Exception as e:
            raise RuntimeError(f"Failed to send answer: {str(e)}")

    def get_output(self):
        """Get next message from the queue (non-blocking)"""
        try:
            return self.output_queue.get_nowait()
        except:
            return None

    def stop(self):
        """Stop the onboarding process"""
        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except:
                self.process.kill()
            self.process = None
            self.running = False

    def is_running(self) -> bool:
        """Check if process is still running"""
        return self.running and self.process is not None


# Steps metadata
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
