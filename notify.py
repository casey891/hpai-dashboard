#!/usr/bin/env python3
"""
notify.py — Alert on HPAI dashboard build problems via macOS Notification Center.

Only fires when something is wrong: a failed build, a dataset that could not be
refreshed, or a dataset that has stopped advancing. A clean run is silent, so
the notification keeps meaning something.

Repeated problems escalate. A one-off blip (APHIS slow, Mac asleep) gets a
banner; a problem that persists across runs gets a louder banner and, if a
Todoist token is available, a task so it lands in the system of record instead
of scrolling out of Notification Center.

Todoist is optional and dormant unless TODOIST_API_TOKEN is set in the
environment. Create one at Todoist > Settings > Integrations > Developer.

Usage:
    python3 notify.py                      # read build_status.json, alert if needed
    python3 notify.py --build-failed "..."  # build itself crashed
    python3 notify.py --test               # send a sample banner
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

STATE_FILE = ".notify_state.json"
ESCALATE_AFTER = 2          # consecutive problem runs before escalating
TODOIST_API = "https://api.todoist.com/rest/v2/tasks"


def send_banner(title, subtitle, message, sound="Basso"):
    """Post a Notification Center banner. Returns True if osascript accepted it."""
    def esc(s):
        return s.replace("\\", "\\\\").replace('"', '\\"')

    script = (f'display notification "{esc(message)}" '
              f'with title "{esc(title)}" '
              f'subtitle "{esc(subtitle)}" '
              f'sound name "{esc(sound)}"')
    try:
        subprocess.run(["osascript", "-e", script], check=True,
                       capture_output=True, timeout=20)
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as e:
        print(f"  notify: banner failed — {e}", file=sys.stderr)
        return False


def create_todoist_task(content, description):
    """Create a Todoist task. No-op (returns None) when no token is configured."""
    token = os.environ.get("TODOIST_API_TOKEN", "").strip()
    if not token:
        return None
    try:
        import requests
        r = requests.post(
            TODOIST_API,
            headers={"Authorization": f"Bearer {token}"},
            json={"content": content, "description": description, "priority": 3},
            timeout=30,
        )
        r.raise_for_status()
        return r.json().get("id")
    except Exception as e:
        print(f"  notify: Todoist task failed — {e}", file=sys.stderr)
        return False


def load_state(base):
    try:
        return json.loads((base / STATE_FILE).read_text())
    except (OSError, ValueError):
        return {"streak": 0, "last_escalated": None}


def save_state(base, state):
    try:
        (base / STATE_FILE).write_text(json.dumps(state, indent=2))
    except OSError as e:
        print(f"  notify: could not save state — {e}", file=sys.stderr)


def collect_problems(base, build_failed_msg):
    """Return a list of human-readable problem strings."""
    if build_failed_msg:
        return [f"Build failed: {build_failed_msg}"]

    try:
        status = json.loads((base / "build_status.json").read_text())
    except (OSError, ValueError):
        return ["Build produced no status file — it may not have finished."]

    problems = []
    for name in status.get("download_failures", []):
        problems.append(f"Could not refresh {name}")
    problems.extend(status.get("stale", []))
    return problems


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--dir", default=".", help="Project directory")
    ap.add_argument("--build-failed", default="", help="Build crashed; message to show")
    ap.add_argument("--test", action="store_true", help="Send a sample banner and exit")
    args = ap.parse_args()

    if args.test:
        ok = send_banner("HPAI Dashboard", "Test — safe to dismiss",
                         "Sample alert. Real ones only fire on problems.")
        print("test banner sent" if ok else "test banner FAILED")
        return 0 if ok else 1

    base = Path(args.dir)
    problems = collect_problems(base, args.build_failed)
    state = load_state(base)

    if not problems:
        # Clean run: stay quiet and reset the streak.
        if state.get("streak"):
            print(f"  notify: recovered after {state['streak']} bad run(s)")
        save_state(base, {"streak": 0, "last_escalated": None})
        return 0

    streak = state.get("streak", 0) + 1
    escalate = streak >= ESCALATE_AFTER
    today = date.today().isoformat()

    headline = problems[0]
    if len(problems) > 1:
        headline += f" (+{len(problems) - 1} more)"

    if escalate:
        subtitle = f"Still failing — {streak} runs in a row"
        sound = "Sosumi"
    else:
        subtitle = "Dashboard still published from the last good copy"
        sound = "Basso"

    send_banner("HPAI Dashboard", subtitle, headline, sound=sound)

    # Escalate to Todoist once per streak, not once per run.
    task_id = None
    if escalate and state.get("last_escalated") != today:
        body = "\n".join(f"- {p}" for p in problems)
        task_id = create_todoist_task(
            f"HPAI dashboard: {len(problems)} data problem(s) for {streak} runs running",
            f"{body}\n\nThe dashboard is still live on the last good data.\n"
            f"Check whether APHIS restructured the Tableau dashboard, then run:\n"
            f"  python3 tableau_export.py",
        )
        if task_id:
            print(f"  notify: Todoist task created ({task_id})")
        elif task_id is None:
            print("  notify: escalated (set TODOIST_API_TOKEN to also create a task)")

    save_state(base, {"streak": streak,
                      "last_escalated": today if escalate else state.get("last_escalated")})

    print(f"  notify: {len(problems)} problem(s), streak {streak}"
          f"{' — ESCALATED' if escalate else ''}")
    for p in problems:
        print(f"    - {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
