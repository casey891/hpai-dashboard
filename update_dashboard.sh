#!/usr/bin/env bash
# update_dashboard.sh — Download APHIS data, build dashboard, push to GitHub, email report.
# Intended for cron: 0 8 * * 1-5 /path/to/update_dashboard.sh

set -uo pipefail

PROJECT_DIR="/Users/casey/Documents/Workspace/IAA_Code_Projects/hpai-dashboard"
LOG="$HOME/hpai_dashboard.log"
DATE=$(date +"%B %d, %Y")

# Source env vars (MARS_API_KEY, GMAIL_APP_PASSWORD)
# Use --no-rcs pattern: only grab exports, skip interactive stuff
eval "$(grep '^export ' "$HOME/.zshrc" 2>/dev/null || true)"

cd "$PROJECT_DIR"

echo "========================================" >> "$LOG"
echo "HPAI Dashboard Update — $(date)" >> "$LOG"
echo "========================================" >> "$LOG"

# ── Run the build ────────────────────────────────────────────────────────────
BUILD_OUTPUT=$(mktemp)
trap "rm -f $BUILD_OUTPUT" EXIT

if python3 build_dashboard.py >> "$BUILD_OUTPUT" 2>&1; then
    echo "Build successful." >> "$LOG"

    # Check for changes
    DIFF_STAT=$(git diff --stat 2>/dev/null)
    if [ -z "$DIFF_STAT" ]; then
        echo "No data changes detected." >> "$LOG"
        STATUS="success_no_changes"
    else
        echo "Changes detected, committing..." >> "$LOG"
        git add data.json index.html "data/*.csv" download_data.py build_dashboard.py parsers.py template.py 2>/dev/null || true
        git commit -m "Update HPAI dashboard data — $DATE" >> "$LOG" 2>&1 || true
        git push origin main >> "$LOG" 2>&1
        echo "Pushed to GitHub." >> "$LOG"
        STATUS="success_pushed"
    fi

else
    echo "BUILD FAILED." >> "$LOG"
    STATUS="failed"
fi

# ── Send email report ────────────────────────────────────────────────────────
DIFF_FILE=$(mktemp)
git diff --stat 2>/dev/null > "$DIFF_FILE" || true
trap "rm -f $BUILD_OUTPUT $DIFF_FILE" EXIT

python3 << PYEOF
import smtplib, os, sys
from email.mime.text import MIMEText
from datetime import datetime

password = os.environ.get("GMAIL_APP_PASSWORD", "")
if not password:
    print("WARNING: GMAIL_APP_PASSWORD not set, skipping email", file=sys.stderr)
    sys.exit(0)

from_addr = "casey.m.downey@gmail.com"
to_addr = "casey@innovateanimalag.org"
date_str = "$DATE"
status = "$STATUS"

# Read build output and diff
with open("$BUILD_OUTPUT") as f:
    build_log = f.read().strip()
with open("$DIFF_FILE") as f:
    diff_stat = f.read().strip()

if status == "failed":
    subject = f"HPAI Dashboard Build FAILED — {date_str}"
    body = f"""HPAI Dashboard build FAILED on {date_str}.

── Error Output ──
{build_log}

Check the full log at: $LOG
"""
elif status == "success_pushed":
    subject = f"HPAI Dashboard Updated — {date_str}"
    body = f"""HPAI Dashboard build completed and pushed to GitHub on {date_str}.

── Build Output ──
{build_log}

── Git Changes ──
{diff_stat}
"""
else:
    subject = f"HPAI Dashboard — No Changes — {date_str}"
    body = f"""HPAI Dashboard build completed on {date_str}. No new data detected.

── Build Output ──
{build_log}
"""

msg = MIMEText(body)
msg["Subject"] = subject
msg["From"] = from_addr
msg["To"] = to_addr

try:
    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.starttls()
        s.login(from_addr, password)
        s.send_message(msg)
    print(f"Email sent: {subject}")
except Exception as e:
    print(f"Email failed: {e}", file=sys.stderr)
PYEOF

echo "Done." >> "$LOG"
echo "" >> "$LOG"

[ "$STATUS" != "failed" ] && exit 0 || exit 1
