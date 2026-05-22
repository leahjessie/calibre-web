# calibre-web — Agent Context

Personal fork of [janeczku/calibre-web](https://github.com/janeczku/calibre-web) for self-hosting.
Upstream maintenance happens as time allows — local fixes and features are carried as local branches.

## Directory Layout

```
~/Developer/calibre-web/
  dev/     — main development worktree (this file lives here); any branch
  run/     — live launchd service; always on run/stable (default) or run/canary
  meta/    — build/deploy scripts on PATH; orphan branch, no code
  lab/     — test env with separate DB and second Kobo device
  build/   - worktree where builds are merged
```

## Branch Taxonomy

| Branch | Purpose |
|--------|---------|
| `master` | upstream mirror — never commit here, FF only |
| `base` | test infrastructure + graduated tests; base for all dev work |
| `bug/xxx` | local fix + tests — always rebases onto base |
| `feat/xxx` | feature work + tests — always rebases onto base |
| `debug/xxx` | logging-only instrumentation branch (see hygiene rule below); disposable |
| `pr/xxx`, `ref-pr/xxx` | branches with open upstream PRs — kept around even when stalled, in case the PR ever gets traction |
| `parked/xxx` | complete, working code that's just not needed right now (e.g. `parked/thumbnail-efficiency`); revivable |
| `backup/xxx` | safety net after a destructive operation (rebase, branch reset). Delete after a few weeks of confidence. |
| `run/stable` | proven deployed build — script output, never edit directly |
| `run/canary` | stable + standing debug + currently-investigated experimental work — script output |
| `run/lab/standard` | canary + lab-only branches (e.g. `feat/lab-flag`) — script output |
| `meta` | orphan: build/deploy scripts and WORKFLOW.md |

**Key rules:**
- Never commit to `master`
- Never edit `run/*` branches directly — always rebuild via `build.sh`
- Unless specified / necessary, always base new branches off `base`. Fetch and merge `upstream/master` into `master`, then rebase onto `base` first.
- Every `bug/` and `feat/` branch can run pytest without a build step.
- **`debug/` hygiene:** debug branches contain ONLY logging/tracing code, never functional changes. If a debug branch grows feature work, extract it to a `feat/*` branch. (Violated historically by `debug/kobo-store-reading-state-locator-logging`; surgically corrected 2026-05-21.)
- Do not push fixes upstream to janeczku/calibre-web — upstream is too quiet for the round-trip to pay off. The patches-on-top build is the destination.

## Tag Taxonomy

| Tag | Purpose |
|--------|---------|
| `archive/<original-prefix>/xxx` | superseded / historical work kept for "what we tried" context. Annotated tag — the message explains what was tried and why it didn't ship. Original branch is deleted. |

**Why tags, not branches:** archived work shouldn't clutter `git branch` output or imply it's still a development surface. Annotated tags hold the code state *and* a paragraph of context, live in their own namespace (`git tag --list 'archive/*'`), and can be resurrected as a branch in one command (`git branch revive archive/bug/foo`) if needed. Nested original prefix (`archive/bug/...`, `archive/debug/...`, `archive/feat/...`) preserves intent semantics.

Recover an archived item:
```bash
git show archive/bug/<name>          # tag message + commit + diff
git checkout archive/bug/<name>      # browse the code (detached HEAD)
git branch <name> archive/bug/<name> # resurrect as a branch
```

Narrative investigation histories live in `notes/` (e.g. `notes/kobo-popup-attempts.md`), cross-referencing the tags.

## Profile Layering

Profiles in `meta/profiles/` are stacked, conceptually:

```
stable  = proven baseline                          (run/stable, deploys to run/)
canary  = stable + standing debug branches + experimental  (run/canary, manual deploy)
lab     = canary + lab-only branches               (run/lab/standard, deploys to lab/)
```

When no investigation is in flight, `canary` == `stable` + the two standing debug branches. New experimental work appends below the `# Experimental` marker in `canary.conf`.

**Branch order matters in profiles:** `feat/epub-reader-foliate` must merge before `bug/kobo-popup-v2` because foliate contains a refactor of `cps/kobo.py` reading-state queries that popup-v2 layers on top of. Reversing produces real conflicts. Don't reorder without understanding the kobo.py reading-state-response evolution.

## Running Tests

```bash
# From dev/ worktree on any bug/ or feat/ branch:
pytest tests/test_kobo_sync_integration.py   # specific file
pytest                                        # all tests

# Check which test files are stubs vs real:
for f in tests/test_*.py; do
    grep -q "^def test_" "$f" && echo "real  $f" || echo "stub  $f"
done
```

Virtualenvs (managed by pyenv, resolved via .python-version in each worktree):
- `cw-dev` — dev/ worktree
- `cw-run` — run/ worktree

## Build and Deploy Scripts (meta/, on PATH)

```bash
build.sh stable                   # build run/stable from profiles/stable.conf
build.sh canary                   # build run/canary from profiles/canary.conf (no deploy by default)
build.sh canary --target run      # build run/canary and deploy to run/ worktree
build.sh canary --target none     # build branch without deploying
build.sh --branches 'bug/a feat/b' --name dev --target dev          # explicit branches
build.sh --branches 'bug/a debug/b' --name run/lab/my-test --target lab  # temporary debug build
build.sh canary --dry-run         # print plan, no git writes
deploy-cw.sh run/canary           # switch run/ to canary and restart service
deploy-cw.sh                      # switch back to run/stable
```

Active branch sets are configured in `meta/profiles/canary.conf` and `meta/profiles/stable.conf`.
Scripts live in `~/Developer/calibre-web/meta/` — edit there, changes take effect immediately.
Full workflow documentation: `~/Developer/calibre-web/meta/WORKFLOW.md`

## launchd Service

```bash
# Check what's running
tail ~/Library/Logs/calibre-web.log          # timestamped start events for both instances
git -C ~/Developer/calibre-web/run branch --show-current

# --- run instance (auto-started, production) ---
# label: com.calibre-web.app
launchctl kickstart -k gui/$(id -u)/com.calibre-web.app          # restart, branch unchanged
deploy-cw.sh run/canary                                           # switch to canary and restart
deploy-cw.sh                                                      # switch back to run/stable
# full reload after plist edit:
launchctl bootout gui/$(id -u)/com.calibre-web.app && launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.calibre-web.app.plist

# --- lab instance (not auto-started, for testing) ---
# label: com.calibre-web.app.lab
build.sh lab                                                                    # rebuild from profiles/lab.conf and deploy to lab/
launchctl kickstart -k gui/$(id -u)/com.calibre-web.app.lab                    # restart after rebuild
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.calibre-web.app.lab.plist  # start lab if not running
launchctl bootout gui/$(id -u)/com.calibre-web.app.lab                         # stop lab
```

Note: `svc` is a zsh shell function (defined in `~/.config/zsh/functions.zsh`) and is not available in non-interactive shells. Use `launchctl` commands above instead.

Plist (run): `~/Library/LaunchAgents/com.calibre-web.app.plist`
Plist (lab): `~/Library/LaunchAgents/com.calibre-web.app.lab.plist` — not auto-started
App log (run): `~/.calibre-web/run/calibre-web.log` — Python logging, rotated
App log (lab): `~/.calibre-web/lab/calibre-web.log` — Python logging, rotated
Startup log: `~/Library/Logs/calibre-web.log` — timestamped start events, both instances
launchd stdout/stderr (run): `~/Library/Logs/calibre-web.stdout.log`, `calibre-web.stderr.log`
launchd stdout/stderr (lab): `~/Library/Logs/calibre-web-lab.stdout.log`, `calibre-web-lab.stderr.log`

## Key Paths

| Resource | Path |
|----------|------|
| Calibre library (main) | `/Volumes/Satechi/macMini/calibre` — external HD, do not move |
| Calibre library (lab) | `/Volumes/Satechi/macMini/calibre-wt` — external HD, do not move |
| App database (run) | `~/.calibre-web/run/` |
| App database (lab) | `~/.calibre-web/lab/` |
| Service port (run) | 8083 (local), 8084 (Tailscale HTTPS) |
| Service port (lab) | 8085 |

Always open SQLite databases read-only unless deliberately writing:

```bash
sqlite3 --readonly ~/.calibre-web/run/app.db "SELECT ..."
sqlite3 --readonly ~/.calibre-web/lab/app.db "SELECT ..."
```

## Kobo Devices

- Main Kobo → `run/` instance — treat as production, don't break sync
- Second Kobo → `lab/` instance — low risk, for testing

## Multi-Machine

- **Mac mini**: all worktrees permanent, `meta/` on PATH
- **Laptop**: clone into `dev/`, then `git show origin/meta:bootstrap.sh | bash` to install scripts to `~/bin/`
