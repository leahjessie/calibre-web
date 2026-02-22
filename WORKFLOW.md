# Calibre-Web Local Fork Workflow

> Full rationale and edge cases: `GIT_STRATEGY.md` in `dev/`

## Directory Layout

```
~/Developer/calibre-web/
  dev/     — active development (main repo, any branch)
  run/     — launchd service (run/stable by default)
  lab/     — test environment with separate DB and second Kobo
  meta/    — build scripts and this doc (you are here, on orphan branch meta)
```

## Branch Taxonomy

| Branch | Base | Purpose |
|--------|------|---------|
| `master` | `upstream/master` | upstream mirror, FF only — never commit here |
| `base` | `master` | test infra + graduated tests; base for all dev work |
| `bug/xxx` | `base` | local fix + tests; tests always runnable |
| `feat/xxx` | `base` | feature work + tests; tests always runnable |
| `pr/xxx` | `master` | clean branch for upstream PR submission (code only) |
| `run/stable` | scripted | curated stable build, deployed by default |
| `run/canary` | scripted | cutting-edge build, for testing |
| `meta` | orphan | this branch — scripts and workflow docs only |

## Core Rules

1. `master` is read-only — fast-forward only from upstream, never commit here
2. `run/*` branches are build artifacts — never edit directly, always rebuild via script
3. Every `bug/` and `feat/` branch can run pytest without a build step
4. Rebase only when the build script fails or upstream changes a file your branch touches

## Scripts (this directory, on PATH)

```bash
build-canary.sh             # rebuild run/canary from base + current bug/* + feat/* branches
build-stable.sh             # rebuild run/stable (curated, more conservative subset)
build-dev.sh bug/a feat/b   # throw-away integration branch for testing combinations
deploy-cw.sh run/canary     # switch run/ to canary and restart service
deploy-cw.sh                # switch back to run/stable (default)
bootstrap.sh                # laptop/fresh-clone: installs scripts to ~/bin/
```

## Common Workflows

### Work on a local bug fix

```bash
git checkout -b bug/my-fix base
# write fix, write tests
pytest tests/test_my_fix.py
git commit -m "Fix: ..."
build-canary.sh
deploy-cw.sh run/canary   # test it live
```

### Upstream advances

```bash
git fetch upstream
git checkout master && git merge upstream/master   # FF only
git checkout base && git rebase master
# only rebase bug/ and feat/ branches if build-canary.sh fails
build-canary.sh && build-stable.sh
git push --force-with-lease origin run/canary run/stable
```

### Upstream merges a local PR (graduation pattern)

```bash
# 1. Pull fix into master
git fetch upstream
git checkout master && git merge upstream/master

# 2. Rebase base onto new master
git checkout base && git rebase master

# 3. Graduate test file into base
#    If base only has a stub (no real tests yet):
git checkout bug/some-fix -- tests/test_some_feature.py
git commit -m "Graduate X tests to base (fix merged upstream)"
#    If base already has real tests in that file, add new tests manually instead.

git push origin base

# 4. Edit build-canary.sh in meta/ — remove the bug/ branch line
# 5. Rebuild and push
build-canary.sh
git push --force-with-lease origin run/canary

# 6. Clean up
git branch -d bug/some-fix pr/some-fix
git push origin --delete bug/some-fix pr/some-fix
```

### Submit a fix upstream

```bash
git checkout -b pr/my-fix master
git cherry-pick <fix-commit-from-bug/my-fix>   # code only, no test files
git push origin pr/my-fix
# open PR on GitHub
```

## Stub Files in base

Stubs are empty test files (`"""Docstring only."""`) added to `base` as merge anchors.
They prevent add/add conflicts when two independent branches both add tests to the same file.

- **One branch will ever touch the file:** no stub needed, let the branch add it.
- **Two or more branches will touch the same file:** add stub to `base` first.
- **After adding a stub:** branches that haven't yet written to the file must rebase first.

Check stub vs real for all test files:
```bash
for f in tests/test_*.py; do
    grep -q "^def test_" "$f" && echo "real  $f" || echo "stub  $f"
done
```

## Pushing

| Branch | Rule |
|--------|------|
| `master` | FF only, always clean |
| `base` | after rebasing: `git push --force-with-lease origin base` |
| `bug/`, `feat/` | after rebasing: `--force-with-lease` |
| `run/stable`, `run/canary` | every rebuild: `--force-with-lease` |
| `pr/xxx` | plain push while PR is open |
| `meta` | append only, plain push |

Never `--force` on `master` or `meta`.
