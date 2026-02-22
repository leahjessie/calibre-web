#!/usr/bin/env bash
set -euo pipefail

# Tag the current run/canary before overwriting it (idempotent — skip if tag exists)
git tag "run/canary-$(date +%Y%m%d)" run/canary 2>/dev/null || true

git checkout -B run/canary base
git merge --no-ff bug/kobo-sync

# Add additional bug/ and feat/ branches here as they are created:
# git merge --no-ff bug/kobo-mode-switch
# git merge --no-ff feat/series-author-add-shelf

echo "run/canary built successfully"
