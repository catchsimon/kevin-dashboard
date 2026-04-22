#!/usr/bin/env bash
# Finish the GitHub Pages setup — run after gh is installed and you're logged in.
set -e
cd "$(dirname "$0")"

GH_USER=$(gh api user -q .login)
REPO_NAME="${REPO_NAME:-kevin-dashboard}"

echo "==> Setting up git auth helper..."
gh auth setup-git

echo "==> Resetting any leftover git state..."
rm -rf .git

echo "==> Creating initial commit..."
git init -b main >/dev/null
git config user.email "catchsimon@gmail.com"
git config user.name  "Simon"
git add .github .gitignore CLAUDE.md analyze_ivt_rpm.py build_dashboard.py setup_github_pages.sh finish_setup.sh
git commit -m "Initial commit: dashboard + daily build workflow" >/dev/null

echo "==> Creating GitHub repo $GH_USER/$REPO_NAME and pushing..."
if gh repo view "$GH_USER/$REPO_NAME" >/dev/null 2>&1; then
  echo "    Repo already exists; pushing to it"
  git remote add origin "https://github.com/$GH_USER/$REPO_NAME.git" 2>/dev/null \
    || git remote set-url origin "https://github.com/$GH_USER/$REPO_NAME.git"
  git push -u origin main --force
else
  gh repo create "$REPO_NAME" --public --source=. --push
fi

echo "==> Enabling GitHub Pages..."
gh api -X POST "/repos/$GH_USER/$REPO_NAME/pages" -f build_type=workflow 2>/dev/null \
  || gh api -X PUT "/repos/$GH_USER/$REPO_NAME/pages" -f build_type=workflow 2>/dev/null \
  || echo "    (Pages may already be enabled, that's fine)"

echo "==> Triggering first build..."
sleep 3
gh workflow run "Build & deploy dashboard" 2>/dev/null || true

echo ""
echo "============================================================"
echo " Repo:  https://github.com/$GH_USER/$REPO_NAME"
echo " Site:  https://$GH_USER.github.io/$REPO_NAME/"
echo " Build: https://github.com/$GH_USER/$REPO_NAME/actions"
echo "============================================================"
echo " First build is running. Watch live with:  gh run watch"
echo " Site goes live ~1 minute after the build finishes."
