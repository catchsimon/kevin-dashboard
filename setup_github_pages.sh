#!/usr/bin/env bash
# One-shot setup: create GitHub repo, push, enable Pages, watch first build.
# Idempotent: safe to re-run if something fails partway through.
set -e

REPO_NAME="${REPO_NAME:-kevin-dashboard}"
VISIBILITY="${VISIBILITY:-public}"   # set VISIBILITY=private to override
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$PROJECT_DIR"
echo "==> Working in: $PROJECT_DIR"

# 1. Install Homebrew if missing
if ! command -v brew >/dev/null 2>&1; then
  echo "==> Installing Homebrew..."
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  # Add brew to PATH for this shell (Apple Silicon vs Intel)
  if [ -x /opt/homebrew/bin/brew ]; then eval "$(/opt/homebrew/bin/brew shellenv)"; fi
  if [ -x /usr/local/bin/brew ];   then eval "$(/usr/local/bin/brew shellenv)"; fi
fi

# 2. Install GitHub CLI if missing
if ! command -v gh >/dev/null 2>&1; then
  echo "==> Installing GitHub CLI..."
  brew install gh
fi

# 3. Sign in to GitHub if not already
if ! gh auth status >/dev/null 2>&1; then
  echo "==> Signing in to GitHub (a browser window will open)..."
  gh auth login --hostname github.com --git-protocol https --web
fi

# Confirm git uses gh credentials for HTTPS
gh auth setup-git >/dev/null 2>&1 || true

# 4. Reset any half-initialized git state from prior attempts
if [ -d .git ]; then
  echo "==> Resetting existing .git folder..."
  rm -rf .git
fi

# 5. Initial commit
echo "==> Creating initial commit..."
git init -b main >/dev/null
git config user.email "$(gh api user -q .email 2>/dev/null || echo catchsimon@gmail.com)"
git config user.name  "$(gh api user -q .name  2>/dev/null || echo Simon)"
git add .github .gitignore CLAUDE.md analyze_ivt_rpm.py build_dashboard.py setup_github_pages.sh
git commit -m "Initial commit: dashboard + daily build workflow" >/dev/null

# 6. Create the repo on GitHub and push (skip if it already exists)
GH_USER="$(gh api user -q .login)"
if gh repo view "$GH_USER/$REPO_NAME" >/dev/null 2>&1; then
  echo "==> Repo $GH_USER/$REPO_NAME already exists; pushing to it"
  git remote add origin "https://github.com/$GH_USER/$REPO_NAME.git" 2>/dev/null || \
    git remote set-url origin "https://github.com/$GH_USER/$REPO_NAME.git"
  git push -u origin main --force-with-lease
else
  echo "==> Creating GitHub repo: $GH_USER/$REPO_NAME ($VISIBILITY) and pushing..."
  gh repo create "$REPO_NAME" "--$VISIBILITY" --source=. --push
fi

# 7. Enable GitHub Pages with Actions as the source
echo "==> Enabling GitHub Pages (source: GitHub Actions)..."
gh api -X POST "/repos/$GH_USER/$REPO_NAME/pages" -f build_type=workflow >/dev/null 2>&1 \
  || gh api -X PUT "/repos/$GH_USER/$REPO_NAME/pages" -f build_type=workflow >/dev/null 2>&1 \
  || echo "    (Pages may already be enabled — that's fine.)"

# 8. Wait briefly for the workflow to register, then watch the run
echo "==> Triggering first build..."
sleep 3
gh workflow run "Build & deploy dashboard" >/dev/null 2>&1 || true
sleep 5

PAGES_URL="https://$GH_USER.github.io/$REPO_NAME/"
echo ""
echo "============================================================"
echo " GitHub Pages setup complete."
echo "============================================================"
echo " Repo:     https://github.com/$GH_USER/$REPO_NAME"
echo " Site:     $PAGES_URL"
echo " Actions:  https://github.com/$GH_USER/$REPO_NAME/actions"
echo ""
echo " The first build runs now. Watch live with:"
echo "   gh run watch"
echo ""
echo " Daily auto-rebuilds happen at 13:05 UTC."
echo " To trigger a manual rebuild any time:"
echo "   gh workflow run \"Build & deploy dashboard\""
echo "============================================================"
