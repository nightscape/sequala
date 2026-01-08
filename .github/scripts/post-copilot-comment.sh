#!/usr/bin/env bash
set -euo pipefail

PR_NUMBER="${1:?PR number required}"
SESSION_ID="${2:-}"
COPILOT_OUTPUT="${3:-/tmp/copilot-output.log}"

# Build the comment body
{
  cat "$COPILOT_OUTPUT"
  echo ''
  if [ -n "$SESSION_ID" ]; then
    echo "<!-- copilot-response: ${SESSION_ID} -->"
  else
    echo '<!-- copilot-response -->'
  fi
} > /tmp/comment-body.md

gh pr comment "$PR_NUMBER" --body-file /tmp/comment-body.md
