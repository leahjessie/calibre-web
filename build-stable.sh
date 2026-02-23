#!/usr/bin/env bash
set -euo pipefail

DEV_DIR=~/Developer/calibre-web/dev
RUN_DIR=~/Developer/calibre-web/run

merge_branch() {
    local branch="$1"
    git -C "$DEV_DIR" merge --no-ff "$branch" || {
        if ! git -C "$DEV_DIR" rev-parse MERGE_HEAD &>/dev/null; then
            echo "Failed to merge $branch (branch missing or other error)" >&2; exit 1
        fi
        if [[ -n $(git -C "$DEV_DIR" ls-files -u) ]]; then
            echo "Unresolved conflicts merging $branch — fix manually then re-run" >&2; exit 1
        fi
        echo "rerere auto-resolved conflicts in $branch — committing"
        git -C "$DEV_DIR" commit --no-edit
    }
}

# Tag the current run/stable before overwriting it (idempotent — skip if tag exists)
git -C "$DEV_DIR" tag "run/stable-$(date +%Y%m%d)" run/stable 2>/dev/null || true

# Remember dev/'s current branch so we can restore it after building
DEV_BRANCH=$(git -C "$DEV_DIR" branch --show-current)

# Detach HEAD in run/ and dev/ to free up run/stable for rebuilding
git -C "$RUN_DIR" checkout --detach HEAD
git -C "$DEV_DIR" checkout --detach HEAD

# Build run/stable: start from base, merge curated branches
git -C "$DEV_DIR" checkout -B run/stable base
merge_branch bug/kobo-sync

# Add curated subset of bug/ and feat/ branches here as they are promoted to stable:
# merge_branch bug/kobo-mode-switch

# Restore both worktrees: dev/ back to its original branch, run/ to the new build
git -C "$DEV_DIR" checkout "$DEV_BRANCH"
git -C "$RUN_DIR" checkout run/stable

echo "run/stable built successfully"
