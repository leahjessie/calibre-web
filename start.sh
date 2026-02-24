#!/usr/bin/env bash
BRANCH=$(git -C "$(dirname "$0")" branch --show-current 2>/dev/null || echo "detached/unknown")
INSTANCE=$(basename "$(dirname "$0")")
echo "$(date '+%Y-%m-%d %H:%M:%S') starting calibre-web [$INSTANCE] on branch: $BRANCH" \
    >> ~/Library/Logs/calibre-web.log
exec /Users/jessie/.pyenv/shims/python "$(dirname "$0")/cps.py"
