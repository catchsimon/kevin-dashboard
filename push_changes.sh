#!/usr/bin/env bash
# Push the latest tweaks to GitHub. Live site updates ~1 minute later.
# Usage:  bash push_changes.sh "your commit message"
#         (or just bash push_changes.sh — uses a default message)
set -e
cd "$(dirname "$0")"
MSG="${1:-Update dashboard}"
git add -A
if git diff --cached --quiet; then
  echo "Nothing to commit."
  exit 0
fi
git commit -m "$MSG"
git push
echo ""
echo "Pushed. Live site rebuilds in ~1 minute:"
echo "  https://catchsimon.github.io/kevin-dashboard/"
echo "Watch the build:  gh run watch"
