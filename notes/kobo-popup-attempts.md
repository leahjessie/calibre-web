# Kobo "Return to Last Page Read" Popup — Investigation History

The popup on Kobo devices triggered on every sync, even when no actual position
change had happened. Took three attempted fixes before the working approach
(popup-v2, deployed 2026-03-10, clean in production since).

Each historical attempt is preserved as an annotated git tag. Recover them with:

```
git show archive/bug/<name>          # tag message + commit
git checkout archive/bug/<name>      # browse the code (detached HEAD)
git branch revive archive/bug/<name> # resurrect as a branch
```

---

## The bug

Kobo devices cache the server's `PriorityTimestamp` (PT). When a device GET sees
the server's PT > the device's cached PT, the device interprets that as "server
has a newer position" and prompts the user with "Return to last page read?" —
even when the actual reading position hasn't changed.

In calibre-web's KoboReadingState, PT was advancing on operations that didn't
actually change the device's stored position, so the device saw spurious newer-PT
signals on every sync.

---

## Attempts (in order)

### 1. `archive/bug/kobo-popup-freeze-pt` — freeze PT entirely

Single commit. Pinned PT to never advance on the server, on the theory that
device PT and server PT would then always match.

**Why it didn't ship:** early stab; superseded by the more precise
`onupdate-removal` approach which targeted only the specific cause of spurious
advancement.

### 2. `archive/bug/kobo-popup-onupdate-removal` — selective onupdate removal

5 commits including TDD red-phase tests. Diagnosed that
`KoboReadingState.priority_timestamp` had `onupdate=lambda: datetime.now()`,
which SQLAlchemy fired on every UPDATE — including UPDATEs from a status-only
`before_flush` listener that touched `last_modified`. So PT advanced on those
listener-driven updates even when no `KoboBookmark` actually changed.

Fix: removed `onupdate` and set PT explicitly in `receive_before_flush` only
when a `KoboBookmark` changed.

**Why it didn't ship:** diagnosis was correct, but the fix only closed one
*source* of unwanted PT advancement. Other sources (bookmark PUTs from device,
sleep/wake gaps between PUTs and the next confirming GET) still produced drift.

The TDD tests on this branch characterize the spurious-onupdate behavior and
could be salvaged if popup-v2 ever needs to be revisited.

### 3. `archive/bug/kobo-popup-put-response` — echo PT/state in PUT response

4 commits, two sub-approaches:

- (a) Added `PriorityTimestamp` to `UpdateResults[]` in PUT response, so the
      device could refresh its cached PT immediately rather than waiting for
      the next GET.
- (b) When (a) didn't help, added full `ReadingStates: [...]` to the PUT
      response body (same shape as a GET), on the theory that the device might
      require a GET-shaped payload to persist state to flash.

**Why it didn't ship:** field testing showed the device doesn't persist PT from
PUT responses *at all* — only GET responses get persisted to flash. Both
sub-approaches were no-ops from the device's perspective.

**Anti-pattern flag:** popup-v2 (the working fix) explicitly reverses this
direction — commit `8bd6ee7e`: *"drop LM/PT from PUT response to match official
Kobo cloud."* Adding LM/PT to the PUT response is now known to be wrong.

---

## What worked — `bug/kobo-popup-v2` (deployed 2026-03-10)

Two commits:

1. `8bd6ee7e`: keep `PT == LM` always; drop `LM`/`PT` from PUT response to match
   what the official Kobo cloud does.
2. `10126116`: use the device's own `LastModified` as both `PT` and `LM` on the
   server side. The device sees its own timestamp echoed back, never sees
   "server has newer PT."

The structural insight that made it work: **don't track PT as an independent
quantity at all.** PT and LM are the same thing semantically (both express
"latest authoritative state timestamp"), so unifying them eliminates the entire
class of "server PT drifted ahead of device cache" bugs.

Clean in production for 5+ weeks (as of 2026-05-21).

---

## Lessons

- The Kobo sync protocol's `PriorityTimestamp` field has no separate semantic
  meaning beyond what `LastModified` already conveys. Treating them as distinct
  invites drift.
- The device persists state from GET responses to flash; PUT response bodies
  are not authoritative for the device's local cache.
- When chasing a sync bug, sleep/wake gaps between operations are a significant
  source of state divergence — any timestamp the server advances during a sleep
  window will mismatch on wake.
- A spurious-onupdate audit (using `[kobo-ts]` style logging) was the diagnostic
  step that eventually pointed at the structural fix, even though the
  intermediate "selective onupdate removal" approach didn't ship.
