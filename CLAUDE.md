# calibre-web — Claude Code Context

See AGENTS.md for general project context (directory layout, branches, tests, paths, service management).

## Claude-Specific Notes

### Don't edit run/* directly
`run/stable` and `run/canary` are build artifacts. Make changes in `dev/` on the appropriate `bug/` or `feat/` branch, then rebuild via `build-canary.sh`.

### Check the current branch before making changes
The dev/ worktree can be on any branch. Confirm with `git branch --show-current` before editing — especially before touching test files, which are branch-owned.

### Test file ownership
Each `bug/` or `feat/` branch owns specific test files. Two independent branches should not write to the same test file. If a conflict arises, the fix is to add a stub to `base` first. See WORKFLOW.md in meta/ for the stub mechanic.

### Graduation pattern (when an upstream PR is merged)
When a `pr/xxx` is accepted upstream: rebase `base` onto `master`, move the test file from `bug/xxx` into `base`, remove the branch from `build-canary.sh`, rebuild. Full steps in WORKFLOW.md.

### Prefer the dedicated tools
Use `Read`, `Edit`, `Write`, `Grep`, `Glob` over Bash equivalents for file operations. Reserve Bash for git commands and system operations.
