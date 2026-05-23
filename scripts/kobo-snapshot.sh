#!/usr/bin/env bash
#
# kobo-snapshot.sh
#
# Snapshot a Kobo device's on-disk state — sqlite DBs, conf files, markups
# directory — to a timestamped output dir for later inspection.
#
# Used during annotation-sync investigation (see
# meta/notes/2026-05-22-annotation-sync-design.md) to compare device state
# across time + diff against cloud-side observations.
#
# Usage:
#   kobo-snapshot.sh                     # uses defaults; auto-discovers Kobo mount
#   kobo-snapshot.sh --label main        # tag snapshot with device label
#   kobo-snapshot.sh --label lab
#   kobo-snapshot.sh --mount /Volumes/KOBOeReader  # explicit mount path
#   kobo-snapshot.sh --outdir /tmp/snaps           # explicit output base dir
#
# Output: <outdir>/<timestamp>[-<label>]/
#   KoboReader.sqlite         the annotation/bookmark DB (queryable via sqlite3)
#   BookReader.sqlite         binary/encrypted; preserved for later analysis
#   Kobo_eReader.conf         text conf; per-resource URL cache lives here
#   affiliate.conf            small text file; identifies affiliate/build
#   markups/                  JPG + SVG files for every device-local markup
#   manifest.txt              file inventory with sizes and sha256s

set -euo pipefail

# Defaults
MOUNT=""
OUTBASE="/tmp/kobo-snapshots"
LABEL=""

# --- args -------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mount)  MOUNT="$2";   shift 2 ;;
    --outdir) OUTBASE="$2"; shift 2 ;;
    --label)  LABEL="$2";   shift 2 ;;
    --help|-h)
      sed -n '4,28p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

# --- discover mount ---------------------------------------------------------
if [[ -z "$MOUNT" ]]; then
  for candidate in /Volumes/KOBOeReader /Volumes/Kobo /Volumes/KOBOeReader\ * ; do
    if [[ -d "$candidate/.kobo" ]]; then
      MOUNT="$candidate"
      break
    fi
  done
fi

if [[ -z "$MOUNT" || ! -d "$MOUNT/.kobo" ]]; then
  echo "error: no Kobo device mount found. Plug in via USB and try again, or pass --mount." >&2
  exit 1
fi

echo "Kobo mount: $MOUNT"

# --- build output dir -------------------------------------------------------
TIMESTAMP=$(date +%Y-%m-%dT%H%M%S)
SUFFIX=""
[[ -n "$LABEL" ]] && SUFFIX="-$LABEL"
SNAPDIR="$OUTBASE/${TIMESTAMP}${SUFFIX}"
mkdir -p "$SNAPDIR/markups"

# --- copy files -------------------------------------------------------------
copy_if_exists() {
  local src="$1"
  local dst="$2"
  if [[ -f "$src" ]]; then
    cp "$src" "$dst"
    return 0
  fi
  return 1
}

copy_if_exists "$MOUNT/.kobo/KoboReader.sqlite"            "$SNAPDIR/KoboReader.sqlite"   && echo "  ✓ KoboReader.sqlite"  || echo "  ✗ KoboReader.sqlite (missing)"
copy_if_exists "$MOUNT/.kobo/BookReader.sqlite"            "$SNAPDIR/BookReader.sqlite"   && echo "  ✓ BookReader.sqlite (binary)"  || echo "  ✗ BookReader.sqlite (missing)"
copy_if_exists "$MOUNT/.kobo/Kobo/Kobo eReader.conf"       "$SNAPDIR/Kobo_eReader.conf"   && echo "  ✓ Kobo eReader.conf"  || echo "  ✗ Kobo eReader.conf (missing)"
copy_if_exists "$MOUNT/.kobo/affiliate.conf"               "$SNAPDIR/affiliate.conf"      && echo "  ✓ affiliate.conf"     || echo "  ✗ affiliate.conf (missing)"

# markups dir — copy whole directory if non-empty
if [[ -d "$MOUNT/.kobo/markups" ]]; then
  markup_count=$(find "$MOUNT/.kobo/markups" -type f | wc -l | tr -d ' ')
  if [[ "$markup_count" -gt 0 ]]; then
    cp -R "$MOUNT/.kobo/markups/." "$SNAPDIR/markups/"
    echo "  ✓ markups/ ($markup_count files)"
  else
    echo "  · markups/ (empty)"
  fi
fi

# --- write manifest ---------------------------------------------------------
{
  echo "# Kobo snapshot"
  echo "# Source mount: $MOUNT"
  echo "# Timestamp:    $TIMESTAMP"
  [[ -n "$LABEL" ]] && echo "# Label:        $LABEL"
  echo "#"
  echo "# columns: size  sha256  relative_path"
  echo
  find "$SNAPDIR" -type f ! -name manifest.txt -print0 \
    | xargs -0 -I{} bash -c '
        f="$1"
        size=$(stat -f "%z" "$f")
        sha=$(shasum -a 256 "$f" | awk "{print \$1}")
        rel="${f#'"$SNAPDIR"'/}"
        printf "%10d  %s  %s\n" "$size" "$sha" "$rel"
      ' _ {} | sort -k3
} > "$SNAPDIR/manifest.txt"

echo
echo "snapshot written: $SNAPDIR"
echo
echo "manifest preview:"
sed -n '1,10p' "$SNAPDIR/manifest.txt"
