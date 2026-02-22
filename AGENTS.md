# calibre-web — Agent Context

Personal fork of [janeczku/calibre-web](https://github.com/janeczku/calibre-web) for self-hosting.
Upstream maintenance happens as time allows — local fixes and features are carried as local branches.

## Directory Layout

```
~/Developer/calibre-web/
  dev/     — main development worktree (this file lives here); any branch
  run/     — live launchd service; always on run/stable (default) or run/canary
  meta/    — build/deploy scripts on PATH; orphan branch, no code
  lab/     — (future) test env with separate DB and second Kobo device
```

## Branch Taxonomy

| Branch | Purpose |
|--------|---------|
| `master` | upstream mirror — never commit here, FF only |
| `base` | test infrastructure + graduated tests; base for all dev work |
| `bug/xxx` | local fix + tests — always rebases onto base |
| `feat/xxx` | feature work + tests — always rebases onto base |
| `pr/xxx` | clean branch off master for upstream PR submission |
| `run/stable` | curated deployed build — script output, never edit directly |
| `run/canary` | cutting-edge deployed build — script output, never edit directly |
| `meta` | orphan: build/deploy scripts and WORKFLOW.md |

**Key rules:**
- Never commit to `master`
- Never edit `run/*` branches directly — always rebuild via `build-canary.sh` / `build-stable.sh`
- Every `bug/` and `feat/` branch can run pytest without a build step

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
build-canary.sh             # rebuild run/canary from base + bug/* branches
build-stable.sh             # rebuild run/stable (curated subset)
build-dev.sh bug/a feat/b   # throw-away integration branch for testing
deploy-cw.sh run/canary     # switch run/ to canary and restart service
deploy-cw.sh                # switch back to run/stable
```

Scripts live in `~/Developer/calibre-web/meta/` — edit there, changes take effect immediately.
Full workflow documentation: `~/Developer/calibre-web/meta/WORKFLOW.md`

## launchd Service

```bash
# Check what's running
tail ~/Library/Logs/calibre-web.log          # timestamped start events + branch name
git -C ~/Developer/calibre-web/run branch --show-current

# Restart service (branch unchanged)
svc restart com.calibre-web.app

# Switch branch and restart
deploy-cw.sh run/canary

# After editing plist (bootout required to re-read plist):
svc bootout com.calibre-web.app && svc bootstrap com.calibre-web.app
```

Plist: `~/Library/LaunchAgents/com.calibre-web.app.plist`
Logs: `~/Library/Logs/calibre-web.stdout.log`, `calibre-web.stderr.log`

## Key Paths

| Resource | Path |
|----------|------|
| Calibre library (main) | `/Volumes/Satechi/macMini/calibre` — external HD, do not move |
| Calibre library (lab) | `/Volumes/Satechi/macMini/calibre-wt` |
| App database | `~/.calibre-web/` |
| Service port | 8083 (local), 8084 (Tailscale HTTPS) |

## Kobo Devices

- Main Kobo → `run/` instance — treat as production, don't break sync
- Second Kobo → `lab/` instance — low risk, for testing

## Multi-Machine

- **Mac mini**: all worktrees permanent, `meta/` on PATH
- **Laptop**: clone into `dev/`, then `git show origin/meta:bootstrap.sh | bash` to install scripts to `~/bin/`
