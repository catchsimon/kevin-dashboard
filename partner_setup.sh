#!/usr/bin/env bash
# Partner setup: clone Simon's kevin-dashboard repo and prepare it for editing via Claude.
# Run after gh is installed and you're signed in (gh auth status to check).
set -e

REPO="catchsimon/kevin-dashboard"
DEST_DIR="${1:-$HOME/Documents/Claude/Projects/Kevin Dashboard}"

# 1. Verify gh is installed and authenticated
if ! command -v gh >/dev/null 2>&1; then
  echo "GitHub CLI (gh) is not installed. Install it from: https://cli.github.com"
  echo "Or, on macOS, download the .pkg from the latest release at:"
  echo "  https://github.com/cli/cli/releases/latest"
  exit 1
fi
if ! gh auth status >/dev/null 2>&1; then
  echo "==> Signing in to GitHub..."
  gh auth login --hostname github.com --git-protocol https --web
fi
gh auth setup-git

# 2. Clone the repo
mkdir -p "$(dirname "$DEST_DIR")"
if [ -d "$DEST_DIR/.git" ]; then
  echo "==> Repo already cloned at: $DEST_DIR — pulling latest"
  cd "$DEST_DIR"
  git pull
else
  echo "==> Cloning $REPO to: $DEST_DIR"
  gh repo clone "$REPO" "$DEST_DIR"
fi
cd "$DEST_DIR"

# 3. Install Python dependency (only thing the script needs)
pip3 install --quiet openpyxl 2>/dev/null \
  || pip3 install --quiet --user openpyxl 2>/dev/null \
  || python3 -m pip install --quiet openpyxl 2>/dev/null \
  || echo "    (Could not auto-install openpyxl. Run: pip3 install openpyxl)"

# 4. Test build works locally
echo "==> Test-building the dashboard locally..."
python3 build_dashboard.py "$DEST_DIR" >/dev/null 2>&1 \
  && echo "    ✓ Build successful — kevin_dashboard.html generated" \
  || echo "    (Build failed locally — that's OK, GitHub Actions still rebuilds it.)"

echo ""
echo "============================================================"
echo " You're set up."
echo "============================================================"
echo " Project folder:  $DEST_DIR"
echo " Live site:       https://catchsimon.github.io/kevin-dashboard/"
echo ""
echo " To make changes via Claude:"
echo "   1. Open Cowork (or Claude Code) and point it at the project folder"
echo "   2. Describe the change you want — Claude will edit the script"
echo "   3. Push your changes:"
echo "      bash \"$DEST_DIR/push_changes.sh\" \"description of change\""
echo "   4. Live site updates ~1 minute later"
echo ""
echo " To pull Simon's latest changes before editing:"
echo "   cd \"$DEST_DIR\" && git pull"
echo "============================================================"
