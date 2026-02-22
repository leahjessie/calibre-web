#!/usr/bin/env bash
set -euo pipefail

RUN_DIR=~/Developer/calibre-web/run

# Tag the current run/stable before overwriting it (idempotent — skip if tag exists)
git tag "run/stable-$(date +%Y%m%d)" run/stable 2>/dev/null || true

# Detach HEAD in run/ to free up run/stable for rebuilding (avoids worktree conflict)
git -C "$RUN_DIR" checkout --detach HEAD

git checkout -B run/stable base
git merge --no-ff bug/kobo-sync

# Add curated subset of bug/ and feat/ branches here as they are promoted to stable:
# git merge --no-ff bug/kobo-mode-switch

# Restore run/ to the newly built run/stable
git -C "$RUN_DIR" checkout run/stable

echo "run/stable built successfully"
