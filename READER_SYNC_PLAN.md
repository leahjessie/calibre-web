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

### Rules

- top-level columns are canonical normalized fields
- `native_locator` is supplementary only
- do not duplicate canonical fields inside `native_locator`
- `source_updated_at` is preserved for debugging and future conflict handling
- `updated_at` remains server-authoritative

### Native payload examples

Kobo:

```json
{
  "location_type": "KoboSpan",
  "location_value": "kobo.12.1",
  "raw_source_path": "OEBPS/text/9780061743511_Chapter_5.xhtml",
  "raw_progress_percent": 31,
  "raw_content_source_progress_percent": 8
}
```

Web:

```json
{
  "chapter_label": "Chapter 5"
}
```

Notes:

- `doc_href` and `cfi` should not be duplicated in JSON
- `doc_progress` is optional
- web may leave `doc_progress` null until we expose it reliably

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

- `doc_href = href`
- `book_progress = fraction`
- `cfi = cfi`
- `doc_progress` only if exposed reliably from Foliate
- optional display-only extras such as `chapter_label` in `native_locator`

### Shared -> Kobo

Preferred:

- if previously known Kobo-native locator exists, reuse it when still valid

Fallback:

- use `doc_href` plus coarse progress mapping if native Kobo locator is absent

Known v1 limitation:

- if a position originated on web only and no Kobo-native locator exists, Kobo
  resume may be approximate rather than exact-word precise

## Migration Plan

### Migration timing

First migration is needed at the start of real implementation of shared reading
position.

### Migration policy

- additive only
- no destructive changes
- do not repurpose the legacy `bookmark` table
- keep legacy bookmark dual-write only for temporary compatibility

### Why

This repo auto-runs app-db migrations at startup via `cps/ub.py`, so rollback of
code does not imply rollback of schema. Additive-only changes are safest for lab
and canary use.

### Lab rollback practice

- snapshot app DB before first schema rollout
- restore snapshot when testing older code that assumes old schema
- do not assume a branch checkout is a database rollback

## Format/Rendition Risk: EPUB vs KEPUB

Open question:

- are browser `href` paths and Kobo `Location.Source` paths comparable for the
  same title across EPUB vs KEPUB?

This must be checked before finalizing uniqueness on `(user_id, book_id)` alone.

Possible outcomes:

- if paths are compatible enough, one row per `(user_id, book_id)` is fine
- if not, either:
  - add rendition/format distinction
  - or introduce a rendition-neutral mapping layer

For local deployment, steering browser reading toward KEPUB may reduce this risk,
but it should still be verified with a real matched title.

## Browser Progress Exposure Spike

Goal:

- expose browser-side document/chapter-local progress in the Foliate reader

Current best path:

- extend the web `relocate` handler detail to include `docProgress`
- likely derive it via existing `view.getCFIProgress(cfi)`

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

1. Verify EPUB vs KEPUB path compatibility on one matched title.
2. Add a small browser-side spike to expose `docProgress`.
3. Compare browser `href/docProgress` with Kobo `Source/ContentSourceProgressPercent`.
4. Only then finalize the first schema migration in `cps/ub.py`.
