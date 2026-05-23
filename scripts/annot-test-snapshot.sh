#!/usr/bin/env bash
#
# annot-test-snapshot.sh
#
# Capture a baseline snapshot of a book for the annotation-loss investigation
# (see meta/notes/2026-05-22-annotation-sync-design.md, "Investigation plan").
#
# Records: book UUID + Calibre-side metadata, EPUB/KEPUB file fingerprints
# (size, mtime, sha256), and the kepub's koboSpan id set. Run before an edit
# and again after to diff and isolate what actually changed.
#
# Usage:
#   annot-test-snapshot.sh <label> <book-id>
#       e.g.  annot-test-snapshot.sh before 43
#             annot-test-snapshot.sh after  43
#       then  annot-test-snapshot.sh diff   43
#
#   annot-test-snapshot.sh <label> --search "<title fragment>"
#       Resolve book id by title search (errors if not exactly one match).
#
# Options:
#   --library PATH    Override library path (default: lab = calibre-wt)
#   --outdir  DIR     Override output dir (default: /tmp/annot-test)
#
# Library default: /Volumes/Satechi/macMini/calibre-wt (lab).
# For main library: --library /Volumes/Satechi/macMini/calibre

set -euo pipefail

LIBRARY="/Volumes/Satechi/macMini/calibre-wt"
OUTDIR="/tmp/annot-test"

usage() {
  sed -n '4,25p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

# --- parse args -------------------------------------------------------------

[[ $# -lt 1 ]] && usage

LABEL="$1"; shift
BOOK_REF=""
SEARCH_MODE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --library) LIBRARY="$2"; shift 2 ;;
    --outdir)  OUTDIR="$2";  shift 2 ;;
    --search)  SEARCH_MODE="$2"; shift 2 ;;
    --help|-h) usage ;;
    *)         BOOK_REF="$1"; shift ;;
  esac
done

METADB="$LIBRARY/metadata.db"
[[ -f "$METADB" ]] || { echo "error: metadata.db not found at $METADB" >&2; exit 1; }

# --- resolve book id --------------------------------------------------------

resolve_book_id() {
  local search="$1"
  local matches
  matches=$(sqlite3 --readonly "$METADB" \
    "SELECT id FROM books WHERE title LIKE '%${search//\'/\'\'}%'")
  local count
  count=$(printf '%s\n' "$matches" | grep -c . || true)
  if [[ "$count" -eq 0 ]]; then
    echo "error: no books match '$search'" >&2; exit 1
  elif [[ "$count" -gt 1 ]]; then
    echo "error: multiple matches for '$search':" >&2
    sqlite3 --readonly "$METADB" \
      "SELECT id, title FROM books WHERE title LIKE '%${search//\'/\'\'}%'" >&2
    exit 1
  fi
  echo "$matches"
}

if [[ "$LABEL" == "diff" ]]; then
  : # book id resolved below; just need it for the snapshot dir
elif [[ -z "$BOOK_REF" && -z "$SEARCH_MODE" ]]; then
  echo "error: pass a book id or --search '<title fragment>'" >&2
  exit 1
fi

if [[ -n "$SEARCH_MODE" ]]; then
  BOOK_ID=$(resolve_book_id "$SEARCH_MODE")
elif [[ -n "$BOOK_REF" ]]; then
  BOOK_ID="$BOOK_REF"
else
  echo "error: book id required" >&2; exit 1
fi

# --- diff mode --------------------------------------------------------------

if [[ "$LABEL" == "diff" ]]; then
  BEFORE="$OUTDIR/$BOOK_ID/before"
  AFTER="$OUTDIR/$BOOK_ID/after"
  for d in "$BEFORE" "$AFTER"; do
    [[ -d "$d" ]] || { echo "error: missing snapshot $d" >&2; exit 1; }
  done
  echo "=== metadata diff ==="
  diff -u "$BEFORE/metadata.txt" "$AFTER/metadata.txt" || true
  echo
  echo "=== file fingerprints diff ==="
  diff -u "$BEFORE/files.txt" "$AFTER/files.txt" || true
  echo
  echo "=== kepub span diff ==="
  if [[ -f "$BEFORE/spans.txt" && -f "$AFTER/spans.txt" ]]; then
    diff -u "$BEFORE/spans.txt" "$AFTER/spans.txt" \
      | head -50 || true
    local_before=$(wc -l < "$BEFORE/spans.txt")
    local_after=$(wc -l < "$AFTER/spans.txt")
    echo
    echo "span count: before=$local_before  after=$local_after"
  else
    echo "(no kepub span captures to compare)"
  fi
  exit 0
fi

# --- snapshot mode ----------------------------------------------------------

SNAPDIR="$OUTDIR/$BOOK_ID/$LABEL"
mkdir -p "$SNAPDIR"

# Metadata: uuid + key fields + comments + tags
sqlite3 --readonly "$METADB" <<SQL > "$SNAPDIR/metadata.txt"
.headers on
.mode column
SELECT id, uuid, title, author_sort, series_index,
       datetime(last_modified) AS last_modified,
       datetime(timestamp) AS timestamp
FROM books WHERE id = $BOOK_ID;

SELECT 'series' AS field, s.name AS value
FROM books_series_link bsl JOIN series s ON s.id = bsl.series
WHERE bsl.book = $BOOK_ID
UNION ALL
SELECT 'tag', t.name FROM books_tags_link btl JOIN tags t ON t.id = btl.tag
WHERE btl.book = $BOOK_ID
UNION ALL
SELECT 'author', a.name FROM books_authors_link bal JOIN authors a ON a.id = bal.author
WHERE bal.book = $BOOK_ID;

SELECT 'comments' AS field, text AS value FROM comments WHERE book = $BOOK_ID;
SQL

# File path(s) for this book
BOOK_REL_PATH=$(sqlite3 --readonly "$METADB" \
  "SELECT path FROM books WHERE id = $BOOK_ID")
[[ -n "$BOOK_REL_PATH" ]] || { echo "error: no path for book $BOOK_ID" >&2; exit 1; }
BOOK_DIR="$LIBRARY/$BOOK_REL_PATH"

# Find each format file and fingerprint it
{
  echo "# file fingerprints for book $BOOK_ID"
  echo "# columns: format size mtime sha256 path"
  if [[ -d "$BOOK_DIR" ]]; then
    for f in "$BOOK_DIR"/*.{epub,kepub,opf,jpg} "$BOOK_DIR"/cover.jpg; do
      [[ -f "$f" ]] || continue
      ext="${f##*.}"
      size=$(stat -f '%z' "$f")
      mtime=$(stat -f '%Sm' -t '%Y-%m-%dT%H:%M:%S' "$f")
      sha=$(shasum -a 256 "$f" | awk '{print $1}')
      rel="${f#$LIBRARY/}"
      printf '%-8s %10d  %s  %s  %s\n' "$ext" "$size" "$mtime" "$sha" "$rel"
    done | sort -u
  else
    echo "(book dir $BOOK_DIR not found)"
  fi
} > "$SNAPDIR/files.txt"

# Kepub span fingerprint (the position-stability signal)
KEPUB=$(find "$BOOK_DIR" -maxdepth 1 -name '*.kepub' -o -name '*.kepub.epub' 2>/dev/null | head -1)
if [[ -n "$KEPUB" && -f "$KEPUB" ]]; then
  # unzip exits 11 if a requested pattern matches nothing in the archive (even
  # if other patterns matched). Tolerate that explicitly; we don't care which
  # pattern hit, only that grep ran over whatever did.
  (unzip -p "$KEPUB" '*.xhtml' '*.html' 2>/dev/null || true) \
    | grep -oE 'id="kobo\.[0-9]+\.[0-9]+"' \
    | sort -u > "$SNAPDIR/spans.txt"
  span_count=$(wc -l < "$SNAPDIR/spans.txt" | tr -d ' ')
  echo "captured $span_count kobo spans from $(basename "$KEPUB")"
else
  echo "(no kepub found — skipping span fingerprint)"
fi

echo "snapshot written to $SNAPDIR"
echo
echo "metadata preview:"
sed -n '1,12p' "$SNAPDIR/metadata.txt"
