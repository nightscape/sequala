#!/usr/bin/env bash
set -euo pipefail

# Test script for AI-assisted schema change workflow
# Usage: ./test-schema-copilot.sh "Add a CLEARING_MEMBER_ID to D_ST_BPM_MEMBER"
#
# Copilot will automatically load sql/oracle/AGENTS.md as custom instructions.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

USER_REQUEST="${1:-Add a CLEARING_MEMBER_ID to D_ST_BPM_MEMBER}"

# The prompt wraps the user request with context
# Copilot loads AGENTS.md automatically from sql/oracle/ when running in that context
FULL_PROMPT=$(cat <<EOF
# Schema Change Request

$USER_REQUEST

---

Please make this change. Use sensible defaults - the user prefers seeing a proposal they can adjust rather than answering questions.
EOF
)

echo "=== Testing Copilot Schema Change Workflow ==="
echo "User request: $USER_REQUEST"
echo "Working directory: $REPO_ROOT"
echo ""
echo "Running copilot..."
echo "---"

cd "$REPO_ROOT/sql/oracle"

# Run copilot with the prompt
# --allow-all-tools for testing; in production use more restrictive permissions like:
#   --allow-tool "shell(git:*)" --allow-tool "shell(gh:*)" --allow-tool write
# Running from sql/oracle/ so copilot loads AGENTS.md automatically
copilot -p "$FULL_PROMPT" \
  --allow-all-tools \
  2>&1 | tee /tmp/copilot-schema-output.log

EXIT_CODE=$?

echo ""
echo "---"
echo "Exit code: $EXIT_CODE"
echo "Output saved to: /tmp/copilot-schema-output.log"
