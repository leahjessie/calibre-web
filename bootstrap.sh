#!/usr/bin/env bash
set -euo pipefail
# Laptop / fresh-clone setup only.
# Installs build and deploy scripts from origin/meta into ~/bin/.
# Run once after cloning: git show origin/meta:bootstrap.sh | bash

REPO_DIR=$(git rev-parse --show-toplevel 2>/dev/null || echo ~/Developer/calibre-web/dev)

mkdir -p ~/bin
for script in build-canary.sh build-stable.sh build-dev.sh deploy-cw.sh; do
    git -C "$REPO_DIR" show "origin/meta:$script" > ~/bin/"$script"
    chmod +x ~/bin/"$script"
    echo "Installed ~/bin/$script"
done

echo ""
echo "Done. Ensure ~/bin is on your PATH:"
echo '  export PATH="$HOME/bin:$PATH"'
echo ""
echo "On mac mini, meta/ worktree is on PATH directly — bootstrap not needed there."
