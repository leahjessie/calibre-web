#!/usr/bin/env bash
set -euo pipefail

# Tag the current run/stable before overwriting it (idempotent — skip if tag exists)
git tag "run/stable-$(date +%Y%m%d)" run/stable 2>/dev/null || true

git checkout -B run/stable base
git merge --no-ff bug/kobo-sync

# Add curated subset of bug/ and feat/ branches here as they are promoted to stable:
# git merge --no-ff bug/kobo-mode-switch

echo "run/stable built successfully"
