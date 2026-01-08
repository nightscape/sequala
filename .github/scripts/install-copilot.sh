#!/usr/bin/env bash
set -euo pipefail

COPILOT_VERSION="${COPILOT_VERSION:-0.0.374}"

mkdir -p "$HOME/.local/bin"
curl -fsSLk "https://github.com/github/copilot-cli/releases/download/v${COPILOT_VERSION}/copilot-linux-x64.tar.gz" \
  | tar -xz -C "$HOME/.local/bin"
chmod +x "$HOME/.local/bin/copilot"
echo "$HOME/.local/bin" >> "$GITHUB_PATH"
"$HOME/.local/bin/copilot" --version
