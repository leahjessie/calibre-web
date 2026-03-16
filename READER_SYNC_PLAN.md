# Reader/Kobo Position Sync Plan

## Current State

This note tracks the current state of reader/Kobo position sync work and the
next implementation steps.

What is already true:

- the web reader writes shared position data into `reader_position`
- browser-originated rows often contain enough Kobo-shaped data to synthesize a
  usable local-book Kobo bookmark:
  - `doc_href -> Location.Source`
  - extracted `kobo.<n>.<n>` token from browser `cfi -> Location.Value`
  - `doc_progress -> ContentSourceProgressPercent`
  - `book_progress -> ProgressPercent`
- that synthesized bookmark works on device much better than the earlier
  progress-only fallback
- winner selection for shared -> Kobo responses is now recency-first rather
  than progress-only:
  - compare shared source time against `KoboReadingState.last_modified`
  - if Kobo is fresher, Kobo wins even if web is numerically ahead
  - only if web is at least as fresh should forward-progress be consulted
- for genuine web wins, withholding `ReadingState` from `/sync` and serving it
  on `/v1/library/<uuid>/state` GET causes Kobo to show the expected popup

Current branch state:

- `feat/epub-reader-foliate` contains the real shared-reader and shared -> Kobo
  behavior changes
- `debug/kobo-single-book-trace` contains single-book trace instrumentation on
  top of the feature branch
- lab has already been validated against the branch stack from `lab.conf`

## Important Learnings

### Kobo payload facts

- Kobo gives exact local position as `Location.Source + Location.Value`
- `Location.Type` is consistently `KoboSpan`
- `ProgressPercent` is whole-book progress
- `ContentSourceProgressPercent` is document-local progress
- Kobo does not send EPUB CFI in `PUT /state`

Implication:

- a Kobo-originated `reader_position` row should be expected to carry
  `native_locator["kobo"]`, not `cfi`
- `cfi` remains a web-native precision field

### Browser payload facts

- browser rows carry exact web position in `cfi`
- browser KEPUB CFI can contain embedded Kobo tokens like `kobo.<n>.<n>`
- those tokens can be extracted to synthesize a Kobo-native locator for
  shared -> Kobo responses

Implication:

- browser rows can sometimes drive exact-enough Kobo resume even without Kobo
  having written the shared table yet

### Timestamp semantics

- `updated_at` is SQL/server write time
- `source_updated_at` is the source device/application event time
- when comparing web vs Kobo freshness, `source_updated_at` is the semantically
  correct field

Implication:

- do not use `updated_at` for source arbitration once both web and Kobo can
  write shared rows

### Delivery timing matters

- a fresh remote timestamp by itself was not enough to trigger the Kobo popup
- the popup appeared only when the genuine web-win `ReadingState` was withheld
  from `/sync` and first delivered on `/state` GET during book open

Implication:

- the server must preserve this delivery-timing behavior for genuine web wins

### The old single-row plan is no longer sufficient

Previous planning assumed one `reader_position` row per `(user_id, book_id)`.
That is no longer a good fit for the intended model.

Why:

- web and Kobo both need to preserve their own current shared position state
- a single shared row would force write-time clobbering
- true read-time arbitration requires both source rows to coexist

Implication:

- the next migration should move `reader_position` to per-source rows

## Work To Do

### Step 3 prerequisite: migrate `reader_position` to per-source rows

This should happen before Kobo dual-write.

Target schema:

- drop `uq_reader_position_user_book`
- add `uq_reader_position_user_book_source`

Result:

- one row per `(user_id, book_id, source)`
- web and Kobo rows can coexist for the same user/book

Required code changes:

- `upsert_reader_position()` should query by `(user_id, book_id, source)`
- Kobo-serving code should stop assuming there is at most one shared row
- add a small helper that loads shared rows by source and chooses the best
  candidate row to hand to the existing phase-2 response logic
- fix freshness comparisons so the value passed from
  `get_phase2_current_bookmark_response()` into
  `choose_phase2_current_bookmark_winner()` represents source time
  (`source_updated_at`), not SQL row-write time (`updated_at`)

What should not change in this prerequisite:

- legacy Kobo tables remain intact
- existing shared -> Kobo response shaping logic remains intact
- existing exact-Kobo reuse helpers remain intact

Validation goals:

- web and Kobo rows can coexist for one user/book
- current web-reader behavior still works
- current shared -> Kobo behavior still works
- lab/device behavior already validated on the current feature branch should not
  regress

### Step 3: Kobo -> shared dual-write

After the per-source migration is stable, have Kobo `PUT /state` also write
`reader_position(source = "kobo")` while keeping all legacy Kobo writes.

Scope:

- continue writing the legacy Kobo tables exactly as today
- also create or update the Kobo source row in `reader_position`
- do not change `/state` GET, `/sync`, or browser restore policy yet

Required mapping:

- `doc_href <- CurrentBookmark.Location.Source` when present
  - this should intentionally flow through the existing
    `upsert_reader_position()` normalization path
- `book_progress <- CurrentBookmark.ProgressPercent / 100`
- `doc_progress <- CurrentBookmark.ContentSourceProgressPercent / 100` when
  present
- `source_updated_at <- device-provided reading-state timestamp`
- `native_locator["kobo"] <- { ... exact Kobo fields ... }`

Required `native_locator["kobo"]` fields:

- `location_value`
- `location_type`
- `raw_source_path`
- `raw_progress_percent`
- `raw_content_source_progress_percent`

Implementation notes:

- write the Kobo-native payload under `native_locator["kobo"]`
- use `native_locator_updates={"kobo": {...}}`
- do not try to fabricate `cfi` for Kobo rows
- current Step 3 fallback behavior can use server time for `source_updated_at` if
  neither Kobo `LastModified` value parses
- that is acceptable for this write-only phase, but it should be revisited before
  Step 4 broadens trust/arbitration because freshness comparison uses
  `reader_updated_at` vs `kobo_last_modified`

Validation goals:

- every Kobo PUT leaves behind a usable `reader_position(source = "kobo")` row
- the exact locator payload can round-trip through the existing Kobo reuse path
- legacy Kobo behavior does not regress

### Step 4 pre-work: observability before arbitration changes

Before broadening shared-row trust or adjusting arbitration policy, add a debug
surface that makes the decision tree inspectable without reading raw logs.

Why first:

- Step 4 edge cases (finished books, first-Kobo-row-after-web-only, web-ahead-
  but-Kobo-fresher) are hard to reason about from logs alone
- having a visible explanation of the selection and winner decisions turns a
  multi-day debug loop into a single inspection

Browser debug surface should show:

- both shared rows (`source = "web"` and `source = "kobo"`) with key fields
- legacy Kobo state (`KoboReadingState`) for the same book
- selected shared candidate and the reason it was chosen
- final winner (shared vs legacy) and the reason it won
- a compact "popup eligibility" explanation (withheld from `/sync`? why?)
- a direct link or inline view for the Kobo-side book/state trace if a trace
  path is already active for the book

Log changes:

- include a `shared_selection_reason` field wherever the shared candidate is
  chosen
- include a `final_winner_reason` field wherever the Kobo response winner is
  chosen
- these are distinct: one explains web-vs-Kobo-shared, the other explains
  shared-vs-legacy

### Step 4: trust and arbitration between per-source rows

Step 4 is not primarily about inventing new core logic. Most of the important
pieces already exist.

Already present:

- `choose_phase2_current_bookmark_winner()` already handles `source == "kobo"`
- `_build_exact_kobo_native_bookmark_response()` already rebuilds exact Kobo
  locators from `native_locator["kobo"]`

What Step 4 must decide:

- how read paths choose between the web row and the Kobo row (Stage 1)
- how that chosen shared candidate is compared against legacy `KoboReadingState`
  during the mixed-storage period (Stage 2)
- when Kobo-originated shared rows become a normal first-class source rather
  than a dormant path

These are two distinct comparisons with different semantics and should be
implemented and reasoned about separately.

**Stage 1: choose the best shared candidate (web row vs Kobo row)**

Two concrete changes. The policy question of how to handle edge cases (Kobo
slightly fresher but far behind in progress) is left to observability data from
the pre-work step.

Change 1 — extend the web reader restore fallback:

Currently the book-detail page queries `source == "web"` only; if no web row
exists it falls back to the legacy `Bookmark` table. After Step 3, a Kobo row
may exist for books never opened in the browser. The fallback chain should
become: web row → Kobo row → legacy `Bookmark`. This is unambiguous: resuming
from a Kobo shared row is always better than ignoring it.

Change 2 — `select_reader_position_for_kobo()` becomes active:

This function already loads all sources and selects between them. Currently it
uses pure timestamp-wins, which is fine as a baseline. Once Step 3 adds real
Kobo rows it will start doing real work automatically. The pure-timestamp
baseline is the starting policy; refinements should be driven by what the
observability pre-work reveals, not by speculation.

**Stage 2: compare chosen shared candidate against legacy `KoboReadingState`**

The existing source-aware comparison rules apply here unchanged. The shared
candidate is handed to the existing response-shaping path; no new logic is
needed at this stage unless gaps are found during Step 4 pre-work observability.

Expected shape of Step 4 work:

- web restore: query web row first, fall back to Kobo row, then legacy Bookmark
- Kobo path: `select_reader_position_for_kobo()` selects the shared candidate
  (Stage 1); emit `shared_selection_reason`
- pass that single candidate into the existing response-shaping path (Stage 2);
  emit `final_winner_reason`
- keep safe fallthrough to legacy Kobo state while confidence is still being
  built

Validation goals:

- web reader opens at Kobo position for books with no web row
- `/state` GET can safely reuse exact Kobo locator data from the Kobo source row
- shared -> Kobo behavior still respects fresher-Kobo vs fresher-web rules
- browser behavior remains correct if the most recent shared row is Kobo-originated
- malformed or partial `native_locator["kobo"]` data still falls through safely
- Stage 1 and Stage 2 decisions are both visible in logs and the debug surface

### Design option: per-book sync-disable flag

For non-linear books (cookbooks, reference works) where position is not
meaningful across sessions, web↔Kobo position sync may cause more harm than
good. A per-user-per-book flag could opt individual books out entirely.

When disabled:

- Kobo reads from legacy `KoboReadingState` only — no shared row influence
- web reads from `source == "web"` row only — no Kobo row fallback
- Stage 1 and Stage 2 arbitration are both skipped

Where the flag would live: a boolean column on `ReadBook` (user×book) is the
natural fit — one column, one migration, no new relationships. Surface in the
book detail page or reader UI.

This is not required for Step 4 but worth considering before the arbitration
logic gets complex, since it eliminates the problem for a class of books where
sync is counterproductive.

## Short-Term Next Steps

1. Implement the per-source-row migration prerequisite.
2. Add or update tests proving current paths still work with coexisting web and
   Kobo rows.
3. Only then implement Kobo dual-write into `reader_position(source = "kobo")`.
4. Inspect real dual-written Kobo rows before broadening read-path trust.
5. Implement Step 4 pre-work: browser debug surface and `shared_selection_reason`
   / `final_winner_reason` log fields.
6. After that, implement Step 4 Stage 1 selection policy and Stage 2 trust logic
   for coexisting rows.
