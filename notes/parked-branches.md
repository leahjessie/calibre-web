# Parked Branches

Branches under `parked/` are complete-or-near-complete work that isn't currently
needed but might be revisited. They're kept as **branches** (not archive tags)
specifically so they stay visible in `git branch` output as a reminder they
exist.

If you confirm you'll never want it: archive it (`archive/<orig-prefix>/...`
tag) and delete the branch. If you definitely want it: build it. Parked is
the "haven't decided" middle ground.

---

## Currently parked

### `parked/thumbnail-efficiency`
Batches local cover thumbnail generation: 1 image open + 1 DB commit per book
instead of 3× (one per resolution). Validated on lab.

- 2 substantive commits: `91892d37` feature, `0c2c2689` TDD coverage
- Adds `tests/test_thumbnail_generation.py` (343 lines)
- Modifies `cps/tasks/thumbnail.py`

**Why parked:** good work, real efficiency win, but day-to-day thumbnail
generation isn't a bottleneck on the current library size. Worth promoting
if/when the library grows large enough that thumbnail jobs become noticeable,
or if a new feature needs to regenerate thumbnails frequently.

### `parked/kobo-resync-book`
"Resync Kobo" button on the book detail page that removes a book from
KoboSyncedBooks tracking, forcing it to re-sync on the next device sync.
Useful when a book's cover failed to sync properly.

- 1 commit: `00a97f00 Attempt cover syncing fixes` (misleading message — the
  branch is more than just cover fixes)
- Adds POST endpoint `/ajax/kobo/resync/<book_id>` in cps/web.py
- Adds button + JS handler to cps/templates/detail.html
- Also includes minor improvements: epub.py file-existence check + BadZipFile
  catch, session_commit guards in sync_shelves, structured logging in
  HandleCoverImageRequest

**Why parked:** the Resync button is a real feature but unclear if it'd see
real use. Most cover-sync failures probably resolve themselves. The
session_commit and update_on_sync_shelfs improvements are partly obsoleted
by feat/kobo-sync-removals (which deleted the buggy code path entirely).
The epub.py defensive fix is still relevant but trivial.

If you start hitting cover-sync issues that require manual intervention,
promote the button. Otherwise archive eventually.

---

## Relation to `archive/`

| | `parked/<name>` (branch) | `archive/<orig-prefix>/<name>` (tag) |
|---|---|---|
| Intent | "might revisit" | "won't actively revisit, kept for reference" |
| Visibility | Shows in `git branch` | Hidden — `git tag --list 'archive/*'` to see |
| Storage | Branch | Annotated tag |
| Recover | Already a branch | `git branch <new> archive/...` |

If you decide a parked branch is not going to be revived, the conversion is:
```
git tag -a archive/<orig-prefix>/<descriptive-name> parked/<name> -m "..."
git branch -D parked/<name>
```
