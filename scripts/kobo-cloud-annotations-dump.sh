#!/usr/bin/env bash
#
# kobo-cloud-annotations-dump.sh
#
# Enumerate every book the user's Kobo account has synced through CW, GET
# annotations for each from Kobo's reading-services cloud, and dump the full
# corpus to JSON files. The annotation namespace is account-keyed on Kobo's
# side, so this captures annotations from ALL Kobo devices on the account —
# not just the one whose token we're using.
#
# Usage:
#   kobo-cloud-annotations-dump.sh <token-or-@logfile>
#
#   <token>          Direct JWT bearer (paste from log).
#   @<logfile>       Pull the freshest bearer from a CW log file. Example:
#                    kobo-cloud-annotations-dump.sh @~/.calibre-web/lab/calibre-web.log
#
# Options:
#   --library  PATH  Calibre library (default: /Volumes/Satechi/macMini/calibre-wt)
#   --app-db   PATH  CW app.db (default: matches --library; lab or run)
#   --outdir   DIR   Output dir (default: /tmp/kobo-cloud-dump)
#   --deviceid HEX   Override X-Kobo-Deviceid (default: lab Kobo's id)
#   --limit    N     Stop after N books (debugging; default: all)
#
# Output layout:
#   <outdir>/summary.tsv              tab-separated: uuid, title, annot_count, status
#   <outdir>/by-uuid/<uuid>.json      raw API body for each book with >=1 annotation
#   <outdir>/empty.txt                UUIDs that returned an empty annotation set
#   <outdir>/errors.txt               UUIDs that failed (non-200 response, etc.)

set -euo pipefail

# Defaults
LIBRARY="/Volumes/Satechi/macMini/calibre-wt"
APP_DB=""
OUTDIR="/tmp/kobo-cloud-dump"
DEVICE_ID="a82292c710a3f2cfaf2f502898bbf42f341d1016dca5fb6e0bbe1e5584c0a9fd"
LIMIT=""
TOKEN_ARG=""

usage() { sed -n '4,28p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }

# --- args -------------------------------------------------------------------
[[ $# -lt 1 ]] && usage
TOKEN_ARG="$1"; shift
while [[ $# -gt 0 ]]; do
  case "$1" in
    --library)  LIBRARY="$2"; shift 2 ;;
    --app-db)   APP_DB="$2";  shift 2 ;;
    --outdir)   OUTDIR="$2";  shift 2 ;;
    --deviceid) DEVICE_ID="$2"; shift 2 ;;
    --limit)    LIMIT="$2";   shift 2 ;;
    --help|-h)  usage ;;
    *)          echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

# Default APP_DB based on library
if [[ -z "$APP_DB" ]]; then
  case "$LIBRARY" in
    *calibre-wt) APP_DB="$HOME/.calibre-web/lab/app.db" ;;
    *)           APP_DB="$HOME/.calibre-web/run/app.db" ;;
  esac
fi

# --- token resolution -------------------------------------------------------
if [[ "$TOKEN_ARG" == @* ]]; then
  LOGFILE="${TOKEN_ARG#@}"
  LOGFILE="${LOGFILE/#\~/$HOME}"
  [[ -f "$LOGFILE" ]] || { echo "log not found: $LOGFILE" >&2; exit 1; }
  TOKEN=$(grep 'Authorization: Bearer ' "$LOGFILE" | tail -1 | sed 's/.*Bearer //; s/ *$//')
  [[ -n "$TOKEN" ]] || { echo "no Bearer found in $LOGFILE" >&2; exit 1; }
else
  TOKEN="$TOKEN_ARG"
fi

# Sanity-check token expiry from the JWT payload (best-effort base64 decode)
PAYLOAD=$(echo "$TOKEN" | awk -F. '{print $2}')
# Pad to multiple of 4
while [[ $((${#PAYLOAD} % 4)) -ne 0 ]]; do PAYLOAD="${PAYLOAD}="; done
EXP=$(echo "$PAYLOAD" | tr '_-' '/+' | base64 -d 2>/dev/null \
        | python3 -c 'import sys,json; print(json.load(sys.stdin)["exp"])' 2>/dev/null || echo "")
NOW=$(date +%s)
if [[ -n "$EXP" ]]; then
  if (( EXP <= NOW )); then
    echo "error: token expired $((NOW - EXP))s ago" >&2; exit 1
  fi
  echo "token valid for $((EXP - NOW))s more"
fi

# --- prepare output ---------------------------------------------------------
mkdir -p "$OUTDIR/by-uuid"
> "$OUTDIR/summary.tsv"
> "$OUTDIR/empty.txt"
> "$OUTDIR/errors.txt"
printf 'uuid\ttitle\tannot_count\tstatus\n' > "$OUTDIR/summary.tsv"

# --- build book list --------------------------------------------------------
# Join KoboSyncedBooks (CW app.db, "main" attached) with Calibre books (library
# metadata.db, "cal" attached) — single sqlite session to do the cross-DB join.
BOOKS_TSV=$(sqlite3 "$APP_DB" <<SQL
ATTACH DATABASE '$LIBRARY/metadata.db' AS cal;
SELECT cal.books.uuid, cal.books.title
FROM main.kobo_synced_books AS ksb
JOIN cal.books ON cal.books.id = ksb.book_id
ORDER BY cal.books.title;
SQL
)

if [[ -z "$BOOKS_TSV" ]]; then
  echo "no books found in KoboSyncedBooks joined with $LIBRARY" >&2
  exit 1
fi

TOTAL=$(printf '%s\n' "$BOOKS_TSV" | wc -l | tr -d ' ')
echo "found $TOTAL books in KoboSyncedBooks; querying Kobo cloud..."
echo

# --- per-book curl ----------------------------------------------------------
N=0
WITH_ANNOTATIONS=0
TOTAL_ANNOTATIONS=0
while IFS='|' read -r UUID TITLE; do
  N=$((N+1))
  [[ -n "$LIMIT" ]] && (( N > LIMIT )) && break

  URL="https://readingservices.kobo.com/api/v3/content/${UUID}/annotations?limit=100"
  RESP_FILE="$OUTDIR/by-uuid/${UUID}.json"
  HTTP=$(curl -sS -o "$RESP_FILE" -w '%{http_code}' \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept: application/json" \
    -H "X-Kobo-Affiliatename: kobo-Replacements" \
    -H "X-Kobo-Appversion: 4.45.23684" \
    -H "X-Kobo-Deviceid: $DEVICE_ID" \
    -H "X-Kobo-Devicemodel: Kobo Libra Colour" \
    -H "X-Kobo-Deviceos: 4.9.77" \
    -H "X-Kobo-Platformid: 00000000-0000-0000-0000-000000000390" \
    -H "User-Agent: Mozilla/5.0 (Linux; U; Android 2.0; en-us;) AppleWebKit/538.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/538.1 (Kobo Touch 0390/4.45.23684)" \
    "$URL" || echo "000")

  if [[ "$HTTP" != "200" ]]; then
    echo "$UUID  ($TITLE)  HTTP $HTTP" >> "$OUTDIR/errors.txt"
    printf '%s\t%s\t-\thttp-%s\n' "$UUID" "$TITLE" "$HTTP" >> "$OUTDIR/summary.tsv"
    rm -f "$RESP_FILE"
    printf '  [%3d/%d] %s  HTTP %s\n' "$N" "$TOTAL" "$TITLE" "$HTTP"
    continue
  fi

  COUNT=$(python3 -c 'import sys,json; print(len(json.load(open(sys.argv[1]))["annotations"]))' "$RESP_FILE" 2>/dev/null || echo 0)
  if [[ "$COUNT" == "0" ]]; then
    echo "$UUID  ($TITLE)" >> "$OUTDIR/empty.txt"
    printf '%s\t%s\t0\tempty\n' "$UUID" "$TITLE" >> "$OUTDIR/summary.tsv"
    rm -f "$RESP_FILE"
    printf '  [%3d/%d] %s  (empty)\n' "$N" "$TOTAL" "$TITLE"
  else
    printf '%s\t%s\t%d\tok\n' "$UUID" "$TITLE" "$COUNT" >> "$OUTDIR/summary.tsv"
    WITH_ANNOTATIONS=$((WITH_ANNOTATIONS + 1))
    TOTAL_ANNOTATIONS=$((TOTAL_ANNOTATIONS + COUNT))
    printf '  [%3d/%d] %s  → %d annotations\n' "$N" "$TOTAL" "$TITLE" "$COUNT"
  fi
done <<EOF
$BOOKS_TSV
EOF

echo
echo "=== summary ==="
echo "books queried:           $N"
echo "books with annotations:  $WITH_ANNOTATIONS"
echo "total annotations:       $TOTAL_ANNOTATIONS"
echo "output:                  $OUTDIR"
echo
echo "non-empty books listed in: $OUTDIR/summary.tsv (sort -t\\\$'\\t' -k3,3 -rn)"
