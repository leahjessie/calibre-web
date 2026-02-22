#!/usr/bin/env bash
set -euo pipefail
# Throw-away integration branch for testing branch combinations.
# Usage: build-dev.sh bug/branch-a feat/branch-b ...
# The 'dev' branch is always disposable — never deploy it directly.

if [[ $# -eq 0 ]]; then
    echo "Usage: build-dev.sh <branch> [<branch> ...]"
    exit 1
fi

git checkout -B dev base
for branch in "$@"; do
    git merge --no-ff "$branch"
done
echo "dev built from: $*"
