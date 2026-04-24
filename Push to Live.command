#!/usr/bin/env bash
# Double-click this file from Finder to push the latest dashboard changes to
# GitHub. The live site at https://catchsimon.github.io/kevin-dashboard/
# rebuilds in ~1 minute via the Actions workflow.
#
# First-time setup (one command, then never again):
#   chmod +x "Push to Live.command"

set -e
cd "$(dirname "$0")"

echo ""
echo "================================="
echo "  Kevin Dashboard — Push to Live"
echo "================================="
echo ""

# Clean up any stale git lock files left over from interrupted operations.
# Sweep everything under .git that ends in .lock (index.lock, main.lock,
# packed-refs.lock, etc.) plus gc.pid — safe to run even if none exist.
find .git -name "*.lock" -type f -delete 2>/dev/null || true
rm -f .git/gc.pid 2>/dev/null || true

# Summary of what's about to move
echo "Working tree status:"
git status --short
echo ""

HAS_WORKING_CHANGES=0
if ! git diff --quiet || ! git diff --cached --quiet; then
  HAS_WORKING_CHANGES=1
fi

UNPUSHED=$(git log @{u}.. --oneline 2>/dev/null | wc -l | tr -d ' ')

if [ "$HAS_WORKING_CHANGES" = "0" ] && [ "$UNPUSHED" = "0" ]; then
  echo "Nothing to push — you're up to date with origin/main."
  echo ""
  echo "Press any key to close…"
  read -n 1 -s -r
  exit 0
fi

# If only unpushed commits exist, skip the commit step.
if [ "$HAS_WORKING_CHANGES" = "0" ]; then
  echo "No uncommitted changes, but $UNPUSHED commit(s) waiting to push."
  echo ""
else
  echo "Enter commit message (or press Enter for a default):"
  read -r MSG
  if [ -z "$MSG" ]; then
    MSG="Update dashboard ($(date '+%Y-%m-%d %H:%M'))"
  fi
  echo ""
  git add -A
  git commit -m "$MSG"
  echo ""
fi

echo "Pushing to origin/main…"
git push
echo ""
echo "---------------------------------"
echo "Done. Live site rebuilds in ~1 min:"
echo "  https://catchsimon.github.io/kevin-dashboard/"
echo "---------------------------------"
echo ""
echo "Watch the build (optional):  gh run watch"
echo ""
echo "Press any key to close…"
read -n 1 -s -r
