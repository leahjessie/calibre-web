#!/usr/bin/env bash
set -euo pipefail
# Switch the run/ worktree to a branch and restart the launchd service.
# Usage:
#   deploy-cw.sh               — switch to run/stable (default)
#   deploy-cw.sh run/canary    — switch to run/canary
#   deploy-cw.sh bug/some-fix  — emergency: run a dev branch directly

BRANCH=${1:-run/stable}
RUN_DIR=~/Developer/calibre-web/run

git -C "$RUN_DIR" checkout "$BRANCH"

if [[ "$BRANCH" != run/stable && "$BRANCH" != run/canary ]]; then
    echo "WARNING: service running on non-standard branch: $BRANCH"
    echo "   Switch back when done: deploy-cw.sh"
fi

launchctl kickstart -k "gui/$(id -u)/com.calibre-web"
echo "Service restarted on branch: $BRANCH"
