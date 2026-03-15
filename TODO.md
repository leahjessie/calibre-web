# TODO

## bug/kobo-sync

- [ ] Clean up leftover debug scaffolding in `kobo.py` `HandleUserRequest`:
  - `log.error("Key: ...")` and `log.error("Remaining: ...")` (lines ~1040-1041) — wrong level, not useful, rate limit is cleared immediately anyway
  - Duplicate `log.error(limiter.current_limit)` × 2 (lines ~1116-1117)
  - Broader pass through `kobo.py` for anything else at wrong log level or pure noise
- [ ] Add temporary lab-only sync instrumentation for unexpected entitlement leaks:
  - In `kobo_only_shelves_sync` mode, log any entitlement book ids emitted by `/v1/library/sync` that are not on a `kobo_sync` shelf
  - Include whether each offending book id is present in `kobo_synced_books`
  - Prefer compact summary lines so one runaway sync does not rotate away the start of the evidence

## bug/kobo-priority-timestamp

- [ ] Remove `[kobo-ts]` debug logging once popup bug is confirmed fixed — it's diagnostic scaffolding, not permanent

## bug/kobo-sync (or new bug branch)

- [ ] Fix `get_statistics_response` dropping `SpentReadingMinutes`/`RemainingTimeMinutes` when value is `0` — truthiness check silently drops valid zero values
