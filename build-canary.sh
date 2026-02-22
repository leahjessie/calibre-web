#!/usr/bin/env bash
set -euo pipefail

DEV_DIR=~/Developer/calibre-web/dev

# Tag the current run/canary before overwriting it (idempotent — skip if tag exists)
git -C "$DEV_DIR" tag "run/canary-$(date +%Y%m%d)" run/canary 2>/dev/null || true

# Remember dev/'s current branch so we can restore it after building
DEV_BRANCH=$(git -C "$DEV_DIR" branch --show-current)

# Detach dev/ to free up run/canary for rebuilding
git -C "$DEV_DIR" checkout --detach HEAD

# Build run/canary: start from base, merge all active branches
git -C "$DEV_DIR" checkout -B run/canary base
git -C "$DEV_DIR" merge --no-ff bug/kobo-sync

# Add additional bug/ and feat/ branches here as they are created:
# git -C "$DEV_DIR" merge --no-ff bug/kobo-mode-switch
# git -C "$DEV_DIR" merge --no-ff feat/series-author-add-shelf

# Restore dev/ to its original branch
git -C "$DEV_DIR" checkout "$DEV_BRANCH"

echo "run/canary built successfully"
