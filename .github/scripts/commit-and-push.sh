#!/usr/bin/env bash
set -euo pipefail

BRANCH="${1:?Branch name required}"
FALLBACK_MSG="${2:-Schema changes from copilot}"
COPILOT_OUTPUT="${3:-/tmp/copilot-output.log}"

# Check if copilot made any file changes
if [ -n "$(git status --porcelain)" ]; then
  # Extract commit message from copilot output
  COMMIT_MSG=$(grep -oP '^COMMIT_MSG: \K.*' "$COPILOT_OUTPUT" | tail -1) || true

  if [ -z "$COMMIT_MSG" ]; then
    COMMIT_MSG="$FALLBACK_MSG"
  fi

  git add -A
  git commit -m "$COMMIT_MSG"
fi

# Push if there are local commits
if [ "$(git rev-list @{u}..HEAD 2>/dev/null | wc -l)" -gt 0 ]; then
  # Rebase on latest remote to handle any concurrent changes
  git fetch origin "$BRANCH"
  git rebase "origin/$BRANCH" || {
    echo "Rebase failed, aborting and trying merge instead"
    git rebase --abort
    git pull origin "$BRANCH" --no-edit
  }
  git push origin HEAD
fi
