#!/usr/bin/env bash
set -euo pipefail
# Build calibre-web result branches from a profile or explicit branch list.
#
# Usage:
#   build.sh stable                              # build from profiles/stable.conf
#   build.sh canary                              # build from profiles/canary.conf
#   build.sh stable --target dev                 # mirror stable set, deploy to dev/
#   build.sh canary --name testing-fix           # override result branch name
#   build.sh stable --target none                # build branch without deploying
#   build.sh --branches 'bug/a feat/b' --name dev --target dev
#   build.sh stable --dry-run                    # print plan, no git writes
#
# Flags:
#   --target WORKTREE   deploy result to this worktree (overrides profile; 'none' = skip)
#   --name BRANCH       result branch name (overrides profile)
#   --branches 'b1 b2'  explicit branch list (bypasses profile BRANCHES)
#   --dry-run           print what would be done; no git writes
#   --help -h           print this help message

# --- Resolve paths ---
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BASE_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
DEV_DIR="$BASE_DIR/dev"
BUILD_DIR="$BASE_DIR/build"

# --- Argument parsing ---
PROFILE=""
TARGET_OVERRIDE=""
NAME_OVERRIDE=""
BRANCHES_OVERRIDE=""
DRY_RUN=0

if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
    awk 'NR==1{next} !found&&/^#/{found=1} found&&/^#/{sub(/^# ?/,"");print;next} found{exit}' "${BASH_SOURCE[0]}"
    exit 0
fi

if [[ $# -gt 0 && "$1" != --* ]]; then
    PROFILE="$1"
    shift
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --target)   TARGET_OVERRIDE="$2"; shift 2 ;;
        --name)     NAME_OVERRIDE="$2";   shift 2 ;;
        --branches) BRANCHES_OVERRIDE="$2"; shift 2 ;;
        --dry-run)  DRY_RUN=1; shift ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$PROFILE" && -z "$BRANCHES_OVERRIDE" ]]; then
    echo "Usage: build.sh <profile> [--target W] [--name B] [--dry-run]" >&2
    echo "       build.sh --branches 'b1 b2' --name B [--target W] [--dry-run]" >&2
    echo "Profiles: $(ls "$SCRIPT_DIR/profiles/"*.conf 2>/dev/null | xargs -n1 basename | sed 's/\.conf//' | tr '\n' ' ')" >&2
    exit 1
fi

# --- Load profile (sets TARGET, BRANCH, BRANCHES) ---
TARGET=""
BRANCH=""
BRANCHES=()

if [[ -n "$PROFILE" ]]; then
    CONF="$SCRIPT_DIR/profiles/$PROFILE.conf"
    [[ -f "$CONF" ]] || { echo "Profile not found: $CONF" >&2; exit 1; }
    # shellcheck source=/dev/null
    source "$CONF"
fi

# --- Apply flag overrides ---
[[ -n "$TARGET_OVERRIDE" ]] && TARGET="$TARGET_OVERRIDE"
[[ -n "$NAME_OVERRIDE" ]]   && BRANCH="$NAME_OVERRIDE"
if [[ -n "$BRANCHES_OVERRIDE" ]]; then
    read -ra BRANCHES <<< "$BRANCHES_OVERRIDE"
fi

# --- Defaults and validation ---
[[ -z "$TARGET" ]] && TARGET="none"

if [[ -z "$BRANCH" ]]; then
    echo "No result branch name — use a profile or --name BRANCH" >&2; exit 1
fi
if [[ ${#BRANCHES[@]} -eq 0 ]]; then
    echo "No branches to merge — use a profile or --branches 'b1 b2'" >&2; exit 1
fi

# --- Dry run: print plan and exit ---
if [[ $DRY_RUN -eq 1 ]]; then
    echo "=== DRY RUN ==="
    echo "Profile:  ${PROFILE:-<none>}"
    echo "Branch:   $BRANCH"
    echo "Target:   $TARGET"
    echo "Branches:"
    for b in "${BRANCHES[@]}"; do
        echo "  $b"
    done
    echo ""
    echo "Steps:"
    echo "  1. Tag ${BRANCH} as ${BRANCH}-$(date +%Y%m%d) (if ref exists, idempotent)"
    echo "  2. Ensure $BUILD_DIR worktree exists"
    if [[ "$TARGET" != "none" ]]; then
        echo "  3. Verify $BASE_DIR/$TARGET is clean"
        echo "  4. Detach HEAD in $BASE_DIR/$TARGET"
    fi
    echo "  5. Build in $BUILD_DIR:"
    echo "       git checkout --detach base"
    for b in "${BRANCHES[@]}"; do
        echo "       merge_branch $b"
    done
    echo "       Write BUILD_INFO; commit"
    echo "  6. Force-reset $BRANCH to HEAD"
    if [[ "$TARGET" != "none" ]]; then
        echo "  7. git -C $BASE_DIR/$TARGET checkout $BRANCH"
    fi
    exit 0
fi

# --- merge_branch ---
merge_branch() {
    local branch="$1"
    echo "Merging $branch..."
    git -C "$BUILD_DIR" merge --no-ff "$branch" || {
        if ! git -C "$BUILD_DIR" rev-parse MERGE_HEAD &>/dev/null; then
            echo "Failed to merge $branch (branch missing or other error)" >&2; exit 1
        fi
        if [[ -n $(git -C "$BUILD_DIR" ls-files -u) ]]; then
            echo "Unresolved conflicts in $branch — fix manually then re-run" >&2; exit 1
        fi
        echo "rerere auto-resolved conflicts in $branch — committing"
        git -C "$BUILD_DIR" commit --no-edit
    }
}

# 2. Tag existing result branch for history (idempotent — skips if tag already exists)
if git -C "$DEV_DIR" rev-parse --verify "$BRANCH" &>/dev/null; then
    git -C "$DEV_DIR" tag "${BRANCH}-$(date +%Y%m%d)" "$BRANCH" 2>/dev/null || true
fi

# 3. Create build worktree if missing (self-healing)
if [[ ! -d "$BUILD_DIR" ]]; then
    echo "Creating build worktree at $BUILD_DIR..."
    git -C "$DEV_DIR" worktree add --detach "$BUILD_DIR"
fi

# 4. Target worktree: clean check + detach HEAD to free up branch name
TARGET_DIR=""
if [[ "$TARGET" != "none" ]]; then
    TARGET_DIR="$BASE_DIR/$TARGET"
    [[ -d "$TARGET_DIR" ]] || { echo "Target worktree not found: $TARGET_DIR" >&2; exit 1; }
    if [[ -n $(git -C "$TARGET_DIR" status --porcelain) ]]; then
        echo "$TARGET_DIR is dirty — commit or stash changes first" >&2; exit 1
    fi
    git -C "$TARGET_DIR" checkout --detach HEAD
fi

# 5. Build in build/ worktree
# Abort any in-progress merge from a previous failed run, then reset to base
git -C "$BUILD_DIR" merge --abort 2>/dev/null || true
git -C "$BUILD_DIR" checkout --detach -f base

for branch in "${BRANCHES[@]}"; do
    merge_branch "$branch"
done

# Write BUILD_INFO and commit
BASE_SHA=$(git -C "$BUILD_DIR" rev-parse --short base)
BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%S)
{
    echo "Build:   $BRANCH"
    echo "Date:    $BUILD_DATE"
    echo "Profile: ${PROFILE:-<none>}"
    echo "Base:    $BASE_SHA"
    echo "Merged:"
    for b in "${BRANCHES[@]}"; do
        sha=$(git -C "$BUILD_DIR" rev-parse --short "$b")
        echo "  $b @ $sha"
    done
} > "$BUILD_DIR/BUILD_INFO"

git -C "$BUILD_DIR" add BUILD_INFO
git -C "$BUILD_DIR" commit -m "build: $BRANCH @ $BUILD_DATE"

# 6. Force-reset result branch to current HEAD
git -C "$BUILD_DIR" branch -f "$BRANCH" HEAD

# 7. Deploy to target worktree
if [[ -n "$TARGET_DIR" ]]; then
    git -C "$TARGET_DIR" checkout "$BRANCH"
fi

# Success summary
echo ""
echo "Built successfully: $BRANCH"
[[ -n "$TARGET_DIR" ]] && echo "Deployed to:       $TARGET_DIR"
echo ""
echo "BUILD_INFO:"
cat "$BUILD_DIR/BUILD_INFO"
