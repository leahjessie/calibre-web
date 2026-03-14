# Reader/Kobo Position Sync Plan

## Status

This note captures the current planning state for syncing reading position between
the web reader and Kobo, and is stored in `meta/` so it persists across branch
switches.

The current approach is:

- implement a low-risk shared reading-position model first
- defer real bookmark and annotation implementation, but design with them in mind
- avoid modifying the legacy `bookmark` table beyond temporary compatibility use

## Confirmed Kobo Findings

Observed from run logs with `[kobo-ts]` logging:

- `Location.Type` is consistently `KoboSpan`
- `Location.Source` is a document path within the book, for example:
  - `OEBPS/text/9780061743511_Chapter_4.xhtml`
  - `OEBPS/xhtml/chapter1_split_015.xhtml`
  - `text/part0014.html`
- `Location.Value` is a Kobo-local within-document locator, for example:
  - `kobo.46.3`
  - `kobo.225.5`
  - `kobo.12.1`
  - `kobo.2.1`
- `Location.Value` is not meaningful globally without `Location.Source`
- `ContentSourceProgressPercent` behaves like document/chapter-local progress
- `ProgressPercent` behaves like global-book progress

Implications:

- the meaningful Kobo-native locator is at least `(Location.Source, Location.Value)`
- `ContentSourceProgressPercent` should not be treated as canonical global progress
- Kobo is giving us a usable document anchor plus a proprietary in-document token

## Current Web Reader Data

From `feat/epub-reader-foliate`:

- current web reader saved position contains:
  - `href`
  - `cfi`
  - `fraction`
  - `chapterLabel`
- `fraction` is whole-book progress
- `href` is the current document/chapter href
- `cfi` is precise web-reader position

Important Foliate finding:

- `View.#onRelocate()` already computes progress with:
  - `SectionProgress.getProgress(index, fraction, size)`
- `View.getCFIProgress(cfi)` already exists and does:
  - `PageProgress.getProgress(cfi)`
  - then `SectionProgress.getProgress(progress.index, progress.fraction)`

This strongly suggests browser-side document-local progress is exposable with a
small wiring change.

## Shared Position Model

### Canonical table

Proposed first table: `reader_position`

Columns:

- `id`
- `user_id`
- `book_id`
- `source`
- `created_at`
- `updated_at`
- `source_updated_at`
- `doc_href`
- `book_progress`
- `doc_progress`
- `cfi`
- `payload_version`
- `native_locator` JSON

Uniqueness:

- unique on `(user_id, book_id)` for v1
- one canonical row per user/book, not one row per source
- per-source exact resume data lives inside `native_locator`, not in duplicate
  top-level rows

### Rules

- top-level columns are canonical normalized fields
- `native_locator` is supplementary only
- do not duplicate canonical fields inside `native_locator`
- `native_locator` must be merged by source key (`kobo`, `web`) on update; do
  not replace the entire JSON blob and accidentally erase another source's
  exact locator data
- `source_updated_at` is preserved for debugging and future conflict handling
- `updated_at` remains server-authoritative
- `payload_version` versions the `native_locator` JSON schema for this table
- bump `payload_version` only when the shape or interpretation of `native_locator`
  changes

### Native payload examples

Example `native_locator` shape:

```json
{
  "kobo": {
    "location_type": "KoboSpan",
    "location_value": "kobo.12.1",
    "raw_source_path": "OEBPS/text/9780061743511_Chapter_5.xhtml",
    "raw_progress_percent": 31,
    "raw_content_source_progress_percent": 8
  },
  "web": {
    "chapter_label": "Chapter 5"
  }
}
```

Notes:

- `native_locator` is keyed by source so exact Kobo and web resume hints can be
  retained at the same time
- `doc_href` and `cfi` should not be duplicated in JSON
- `raw_source_path` intentionally duplicates the unnormalized Kobo source path
  so an exact Kobo locator can be reconstructed later even if canonical
  `doc_href` is normalized for cross-source comparison
- `doc_progress` is optional and source-specific
- web and Kobo can both populate `doc_progress`, but it should not be assumed
  interoperable across sources
- web may end up storing only `chapter_label` in `native_locator`, or nothing,
  if no additional web-only extras are needed

## Translation Layer

### Kobo -> shared

Write:

- `doc_href = Location.Source`
- `book_progress = ProgressPercent / 100`
- `doc_progress = ContentSourceProgressPercent / 100`
- store `Location.Type` and `Location.Value` in `native_locator`
- `source_updated_at` from Kobo request timestamp when available

### Web -> shared

Write:

- `doc_href = href` with browser fragment stripped before storage
- `book_progress = fraction`
- `cfi = cfi`
- `doc_progress` from Foliate `getCFIProgress(cfi)` when available
- optional display-only extras such as `chapter_label` in `native_locator.web`

### Shared -> Kobo

Preferred:

- if `source == "kobo"` and previously known Kobo-native locator exists,
  reuse it as the exact Kobo resume locator
- when building Kobo response fields from a valid native Kobo locator, prefer
  `raw_progress_percent` and `raw_content_source_progress_percent` from
  `native_locator` over recomputing percentages from normalized fractions
- rationale: raw Kobo integers round-trip exactly and avoid floating-point drift
  or mismatched rounding against device expectations

Fresh exact Kobo reuse means:

- `source == "kobo"`, i.e. the last canonical write came from Kobo
- a Kobo-native locator exists in `native_locator.kobo`
- when `source != "kobo"`, treat the stored Kobo-native locator as a stale
  exact-position hint and fall back rather than pretending it still represents
  the current canonical position
- `raw_source_path == doc_href` remains a useful sanity check, but not a
  sufficient freshness test on its own because a web read could move within the
  same document without changing `doc_href`

Fallback:

- if native Kobo locator is absent or stale, do not invent a fake
  `Location.Value`
- if `doc_href` is known, send `Location.Source = doc_href` only when Kobo
  accepts a partial location shape in testing
- otherwise omit the `Location` block entirely and send only coarse
  `ProgressPercent` derived from `book_progress`
- never send an empty or placeholder `Location.Value`
- this behavior must be verified against real device behavior before rollout

Known v1 limitation:

- if a position originated on web only and no Kobo-native locator exists, Kobo
  resume may be approximate rather than exact-word precise
- specifically, fallback may only be able to place Kobo at the right document or
  approximate area, not at an exact word/span position

## Migration Plan

### Migration timing

First migration is needed at the start of real implementation of shared reading
position.

### Phase 1 implementation scope

Implement first:

- web reader writes `reader_position`
- web reader continues dual-writing legacy `bookmark.bookmark_key`
- web restore may continue using legacy bookmark until the new path is proven
- Kobo does not read from or write to `reader_position` in phase 1; Kobo
  continues using existing Kobo reading-state tables and endpoints unchanged
- do not import Kobo writes into the shared model yet

### Migration policy

- additive only
- no destructive changes
- do not repurpose the legacy `bookmark` table
- keep legacy bookmark dual-write only for temporary compatibility

### Phase 2 implementation scope: shared -> Kobo consumption

Phase 2 should add a read-only adapter from `reader_position` into Kobo GET/sync
responses. Kobo PUT/import still remains out of scope.

Rules:

- consult `reader_position` only when preparing Kobo reading-state responses
- do not write Kobo PUT data into `reader_position` yet
- if `reader_position` has no row for the user/book, fall through to the
  existing `KoboReadingState` behavior unchanged
- if `reader_position` exists but does not provide a usable Kobo-native locator
  or viable fallback, fall through to the existing `KoboReadingState` behavior
  unchanged

Precedence when both sources have data:

- compare `reader_position.updated_at` against `KoboReadingState.last_modified`
- whichever is newer is the canonical source for the response
- this keeps phase 2 deterministic while both storage systems coexist

Kobo response shaping:

- best case: if the winning source is `reader_position` and
  `native_locator.kobo` is fresh, return exact Kobo-native location data
- otherwise use fallback behavior from the shared model only when it is known to
  be safe
- otherwise fall through to the current `KoboReadingState` response path

Already-known safe fallback:

- omitting the `Location` block entirely is already proven safe in current Kobo
  responses, because fresh/unpositioned books do this today when
  `location_value` is absent
- therefore phase 2 does not need to re-prove `ProgressPercent` without
  `Location`

Remaining open question:

- whether sending `Location.Source` without `Location.Value` is safe on real
  Kobo devices
- this is narrower than the earlier fallback question and should be verified
  before using partial `Location` shapes in production

### Why

This repo auto-runs app-db migrations at startup via `cps/ub.py`, so rollback of
code does not imply rollback of schema. Additive-only changes are safest for lab
and canary use.

### Dual-write compatibility window

During early rollout:

- web reader writes the new `reader_position` row
- web reader also continues writing the legacy `bookmark.bookmark_key`

Exit conditions for ending dual-write:

- web restore reads from `reader_position`
- lab Kobo consumption is stable enough to trust the new shared model
- there is a deliberate story for handling existing legacy bookmark rows

Do not allow dual-write to continue indefinitely without revisiting those exit
conditions.

### Lab rollback practice

- snapshot app DB before first schema rollout
- restore snapshot when testing older code that assumes old schema
- do not assume a branch checkout is a database rollback

## Format/Rendition Findings: EPUB vs KEPUB

Matched-title results for `How to AI`, same approximate location in Chapter 11:

- browser KEPUB `href` matched Kobo KEPUB `Location.Source` exactly after
  stripping the browser fragment (`#page_...`)
- browser EPUB `href` also matched the same Kobo KEPUB `Location.Source`
- therefore `doc_href` is currently the strongest validated shared anchor across
  browser EPUB, browser KEPUB, and Kobo KEPUB for at least one real title

What did not match exactly:

- browser KEPUB and browser EPUB `bookProgress` differed at the same rough
  location, so `book_progress` must be treated as coarse / approximate across
  renditions
- browser `docProgress` did not match Kobo
  `ContentSourceProgressPercent`, so these are not interchangeable and must
  remain source-specific
- browser KEPUB CFI contained `kobo.*` markers, while browser EPUB CFI did not;
  therefore `cfi` is rendition-specific precision data, not a shared canonical
  coordinate

Current conclusion:

- uniqueness on `(user_id, book_id)` is more plausible than before because
  document-path compatibility looks good on one matched title
- however, precise resume remains rendition-specific, so shared cross-rendition
  resume should still be treated as approximate in v1
- keep this as "validated on one title, not universally proven"

## Browser Progress Exposure Spike

Goal:

- expose browser-side document/chapter-local progress in the Foliate reader

Current best path:

- extend the web `relocate` handler detail to include `docProgress`
- derive it via existing `view.getCFIProgress(cfi)`

Expected implementation shape:

1. on `relocate`, keep current:
   - `fraction`
   - `cfi`
   - `href`
2. additionally compute:
   - `docProgress`
3. persist `docProgress` in the temporary bookmark JSON for inspection
4. compare browser `href` and `docProgress` against Kobo:
   - `Location.Source`
   - `ContentSourceProgressPercent`

### Success criteria

- browser `docProgress` is stable within a chapter/document
- browser `docProgress` resets appropriately on chapter/document change
- browser `href` can be compared meaningfully with Kobo `Location.Source`
- browser `docProgress` may still differ semantically from Kobo
  `ContentSourceProgressPercent`

## Deferred Future Entities

Not for v1 implementation, but the shared locator model should support:

- `reader_bookmark`
- `reader_annotation`

These should reuse:

- `doc_href`
- `book_progress`
- `doc_progress`
- `cfi`
- `native_locator`

## Immediate Next Steps

1. Keep validating the web-only phase 1 behavior in lab.
2. Design the phase 2 Kobo adapter around explicit precedence between
   `reader_position` and `KoboReadingState`.
3. Verify the remaining narrow fallback question: whether Kobo accepts
   `Location.Source` without `Location.Value`.
4. Implement the shared -> Kobo adapter with strict fallthrough to current
   behavior when `reader_position` is missing or unusable.
5. Only after that, consider Kobo -> shared import and full conflict handling.
