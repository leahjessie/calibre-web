#!/usr/bin/env bash
set -euo pipefail
# Throw-away integration branch for testing branch combinations.
# Usage:
#   build-dev.sh stable            — mirror run/stable branch set
#   build-dev.sh canary            — mirror run/canary branch set
#   build-dev.sh bug/a feat/b ...  — explicit branches (existing behavior)
# The 'dev' branch is always disposable — never deploy it directly.

META_DIR=~/Developer/calibre-web/meta

if [[ $# -eq 0 ]]; then
    echo "Usage: build-dev.sh stable|canary|<branch> [<branch> ...]"
    exit 1
fi

if [[ $# -eq 1 && ( "$1" == "stable" || "$1" == "canary" ) ]]; then
    branches=()
    while IFS= read -r line; do
        branches+=("$line")
    done < <(grep 'merge_branch' "$META_DIR/build-$1.sh" | grep -v '^\s*#' | awk '{print $NF}')
    if [[ ${#branches[@]} -eq 0 ]]; then
        echo "No branches found in build-$1.sh"
        exit 1
    fi
    echo "Branches from build-$1.sh: ${branches[*]}"
else
    branches=("$@")
fi

git checkout -fB dev base
for branch in "${branches[@]}"; do
    git merge --no-ff "$branch" || {
        if ! git rev-parse MERGE_HEAD &>/dev/null; then
            echo "Failed to merge $branch (branch missing or other error)"
            exit 1
        fi
        if [[ -n $(git ls-files -u) ]]; then
            echo "Unresolved conflicts merging $branch — fix manually then re-run"
            exit 1
        fi
        echo "rerere auto-resolved conflicts in $branch — committing"
        git commit --no-edit
    }
done
echo "dev built from: ${branches[*]}"
