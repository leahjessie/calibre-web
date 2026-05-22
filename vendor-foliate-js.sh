#!/usr/bin/env bash
# vendor-foliate-js.sh — fetch a pinned snapshot of foliate-js into calibre-web
#
# Usage:
#   vendor-foliate-js.sh                  # fetch using REPO/COMMIT below
#
# To update: change COMMIT (and optionally REPO) and re-run.
# To use your own fork: change REPO to e.g. "jessie/foliate-js"

set -euo pipefail

REPO="readest/foliate-js"
COMMIT="3c597a6dc5632b157cebd363e03f8ae33df516a1"

# Destination relative to the calibre-web dev worktree
DEST="$HOME/Developer/calibre-web/dev/cps/static/js/libs/foliate-js"

FILES=(
    view.js
    epubcfi.js
    progress.js
    overlayer.js
    text-walker.js
    epub.js
    fixed-layout.js
    footnotes.js
    paginator.js
    search.js
    reader.js
    ui/tree.js
    ui/menu.js
    vendor/zip.js
    vendor/fflate.js
)

BASE_URL="https://raw.githubusercontent.com/${REPO}/${COMMIT}"

echo "Vendoring foliate-js"
echo "  repo:   ${REPO}"
echo "  commit: ${COMMIT}"
echo "  dest:   ${DEST}"
echo ""

mkdir -p "${DEST}/ui" "${DEST}/vendor"

for file in "${FILES[@]}"; do
    url="${BASE_URL}/${file}"
    dest_file="${DEST}/${file}"
    echo "  fetching ${file}"
    curl --silent --show-error --fail --location -o "${dest_file}" "${url}"
done

cat > "${DEST}/VENDORED" <<EOF
repo:   ${REPO}
commit: ${COMMIT}
date:   $(date -u +"%Y-%m-%dT%H:%M:%SZ")
files:
$(printf '  %s\n' "${FILES[@]}")
EOF

echo ""
echo "Done. Wrote ${DEST}/VENDORED"
