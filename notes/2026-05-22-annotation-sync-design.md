# Foliate ↔ Kobo Annotation Sync — Design Notes

**Started:** 2026-05-22
**Status:** design phase — not implemented
**Related branches:** `debug/kobo-reading-services-mitm` (wire-capture instrumentation, lab-only)
**Related memory files:** `project_nextgen_h1_annotations.md`, `project_annotation_sync_design.md`, `project_nextgen_migration_eval.md`

---

## Motivation

User-perceived problem: annotations on the Kobo device chronically disappear, which has suppressed the user's annotating behavior to the point of barely using the feature. The root causes turn out to be three distinct loss mechanisms (shelf-move/toggle archive-readd, polish + kepub regeneration, and metadata-edit-triggered redownload — all detailed below). All three converge on the same conclusion: device-side annotation state is fragile, and CW-side capture is the only durable store. The planned project addresses all three *and* surfaces the captured annotations somewhere durable and useful (CW DB + Hardcover Journal).

Importantly: empirical work on 2026-05-22 confirmed that Kobo's cloud retains annotations across device-local wipes — at least within our observation window (~1 year, oldest recoverable annotations are from May 2025). 39 annotations across 18 books in the user's main library are retrievable today with a one-shot script. Phase 1 backfill captures all of it.

**Two important nuances:**

1. **Retention is "observed-1-year-minimum," not proven indefinite.** Kobo doesn't document their retention policy. Annotations from beyond ~1 year haven't been tested (the user's Kobo usage doesn't extend that far back). For *historical recovery* the observed window covers basically everything that ever existed. For *future durability* — the larger concern — we're trusting that Kobo continues to keep annotations as they have been. Phase 1 is time-sensitive in that any delay risks more annotations aging out under whatever real retention policy applies. See § "Retention + endpoint uncertainty" below for more.

2. **The small annotation count is itself a symptom.** The user stopped annotating freely because annotations appeared to disappear. Self-limiting behavior is the most insidious usability damage — no error message, just modified behavior. Phase 1 inverts that loop: once capture is reliable and surfacing is rich (foliate + Hardcover), the cost of annotating drops and the value goes up. The *future* annotation volume is where Phase 1's value compounds; historical recovery is a one-time bonus.

Secondary motivation: the foliate-based CW web reader is becoming a real reading surface (replacing dead epub.js v0.3). It needs to participate in annotation state, not just display content.

---

## Wire findings (verified 2026-05-22)

Built `debug/kobo-reading-services-mitm` on lab — overrides `reading_services_host` in `HandleInitRequest` to point the device at CW, with `cps/readingservices.py` as a logging proxy for `/api/v3/*` and `/api/UserStorage/*`. Captured real traffic for both Kobo-store and sideloaded books.

### Endpoint & shape

- `PATCH /api/v3/content/<book_uuid>/annotations` — device pushes new/modified annotations. Carries the `AnnotationSpan` envelope NextGen documented.
- `GET /api/v3/content/<book_uuid>/annotations?limit=100` — device pulls full annotation list for a book. Paginated via `nextPageOffsetToken`. Server returns 304 (with `If-None-Match` ETag manifest match) or 200 with full list.
- All three text-based annotation types — `highlight`, `note`, `dogear` — ride the same endpoint and envelope. Dogears are Kobo's "bookmark" concept (a saved spot, no text content; uses `context` field instead of `highlightedText`).
- **`markup`** (fourth type, found late on 2026-05-23): stylus handwriting. Same endpoint, same envelope, but radically different payload shape — see § "Markup type" below.

### Position formats

Two `startPath` forms coexist within a single device's payload for a single book — same device, same book, different ages of annotation can carry either form:

- Escaped-dot form: `span#kobo\\.62\\.1` (mostly newer highlights/notes; observed on annotations created during this session's testing)
- Chapter-prefix form: `OEBPS/chapter-025.xhtml#kobo.12.2` (mostly older; many dogears)

Note that `chapterFilename` (separate field) is always present and uses the chapter-prefix style (`OEBPS/xhtml/chapter12.xhtml`) regardless of which form `startPath` uses. Position converter must handle both startPath forms and not assume one or the other.

### Sideloaded books work end-to-end

Confirmed mechanically that Kobo's annotation server is a **content-agnostic key-value store keyed by `(user, uuid)`** — no ownership validation. Verified two ways:

1. Device PATCHed annotations for sideloaded UUID `d5692307` (sideloaded "How to AI"); server returned 204.
2. Curl from the Mac with the device's bearer token: `GET /api/v3/content/d5692307.../annotations` returned 200 with 6 annotations.

So free-riding on Kobo's cloud for sideloaded books is *mechanically* possible. We are choosing not to do that (see § Architecture choices).

### Sync trigger is non-deterministic

PATCHes are not tied strictly to tap-Sync. Observed firing on debounce / idle / power-cycle / batch. Treat upward direction as eventually-consistent, not synchronous.

### Bulk-import is feasible

With a valid bearer token (lifetime ~1h) plus the user's UUID list (CW already has it), we can `GET .../annotations` per book and pull the full annotation corpus down. ~150ms per book over HTTPS — a few hundred books fits well within token validity. See § Cloud-side behavior for the empirical scale demonstration (39 annotations across 18 books recovered from main library, 2026-05-22).

---

## Annotation loss mechanisms (the real problem)

Three distinct mechanisms, all producing "annotations vanished" symptoms but via different paths. All converge on the same answer: device-side annotation state is fundamentally fragile and CW-side capture is the only durable store.

### Mechanism 1: Shelf-move via inter-sync diff window

This mechanism lives in `feat/kobo-sync-removals` (commit `9f3a1359`, in stable + canary + lab). That branch restructured removal handling — `update_on_sync_shelfs` no longer archives **books** at all (it only emits `ShelfArchive` rows for shelves whose `kobo_sync` flag was toggled off, which is a collection-level operation on the device, not a book-level one). All book removals — whether triggered by shelf-membership change or by toggling a shelf's `kobo_sync` flag — are now handled exclusively by the per-sync diff in `HandleSyncRequest` (`cps/kobo.py:248+`):

```python
synced_ids = {books CW thinks the device has}
shelf_ids  = {books currently on any kobo-enabled shelf}
for book_id in synced_ids - shelf_ids:
    emit ChangedEntitlement{archived=True, IsRemoved=True}
    kobo_sync_status.remove_synced_book(book_id)
```

**Two user-action patterns hit the diff and cause book + annotation loss:**

#### 1a. Shelf-move (book moves from synced A to synced B)

For a move from synced shelf A to synced shelf B, behavior depends on **whether a sync fires between the remove and the add operations**:

- **No sync between**: book stays in `synced_ids` and stays in `shelf_ids` (joins B before diff runs). Diff is empty. No removal notification. Book stays on device. Annotations preserved.
- **Sync fires between**: book is in `synced_ids` but transiently not in `shelf_ids` at diff time. `IsRemoved=true` goes out, device deletes the book locally (including annotations), `remove_synced_book` clears CW's tracking. Then the add to B happens. Next sync sees the book back on a kobo-synced shelf but missing from `KoboSyncedBooks`, so it gets re-added as a fresh entitlement. Device redownloads.

The user observes that this happens "even within a second" — suggesting either the Kobo polls aggressively enough to hit the window, or the CW UI triggers a sync on shelf-mutating actions. Worth investigating — see § Workflow improvements W2 for sharpened mitigations.

#### 1b. Shelf `kobo_sync` toggle off (and possibly back on)

When the user toggles `kobo_sync = 0` on a shelf that contains annotated books:

1. `update_on_sync_shelfs` queues a `ShelfArchive` (device drops the collection on next sync — this is fine).
2. On the *next* `HandleSyncRequest`, the diff sees every book that was on the toggled-off shelf and not on any *other* kobo-synced shelf. Each gets `IsRemoved=true`, gets removed from `KoboSyncedBooks`, and the device deletes the book locally — annotations included.
3. If the user later re-toggles `kobo_sync = 1` (e.g. exploring shelf organization, or reverting a misclick), the books re-add as fresh entitlements. Device redownloads. Annotations gone.

This is a separate trigger from 1a but the same underlying problem: the diff treats every transient absence-from-shelf as an intentional deletion, when actually toggling a shelf off → on is a recoverable user-flow that shouldn't be destructive.

#### Summary

The branch's design is correct in spirit (removals must propagate to the device) but doesn't protect against *transient or recoverable removals*. Pre-branch, `update_on_sync_shelfs` archived permanently — even worse. The fix is to make the diff window non-destructive for non-permanent removals, not to remove the diff. Mitigation candidates: see § W2.

### Mechanism 2: Polish + kepub regeneration span shift

When the user polishes an EPUB in Calibre app and re-converts to kepub:

- `book.uuid` should be stable (annotations on Kobo's server stay under that UUID).
- BUT the kepub's `<span id="kobo.N.M">` markers are inserted by kepubify based on content structure. Polish rewrites the EPUB internally; the new kepub gets different span IDs.
- Annotations still exist on Kobo's server keyed by the same UUID, but their `startPath` positions point at shifted (or nonexistent) text.

### Mechanism 3: Any redownload destroys device-side annotations

**Confirmed 2026-05-22** via A.1 testing on `Enchanting the Fae Queen` (book id 43 in lab library):

- Snapshot-diffed before/after a tag edit and a summary edit in Calibre app (lab CW stopped during edit). Result: `metadata.db` updated, `metadata.opf` sidecar regenerated, **EPUB and kepub byte-identical** (same sha256), kepub span set identical (1145 spans before and after).
- Synced lab Kobo. Device flagged "redownload required" purely from `last_modified` bump (`book.last_modified` flows into `changed_entries` in `HandleSyncRequest` → ChangedEntitlement).
- Device did not display updated synopsis pre-download.
- After download (of byte-identical kepub): **reading position preserved** (CW serves from `KoboBookmark`/`ReaderPosition` on state-request), **synopsis still not updated** (device reads metadata from the EPUB OPF, which is unchanged), **annotations destroyed**.

The redownload itself wipes annotations regardless of content change. This is independent of polish (Mechanism 2) and independent of shelf-move (Mechanism 1).

### Implications

The earlier "tags/summary edits without polish are safe" working hypothesis was wrong. The actual matrix:

| Outcome | Tag edit | Summary edit (no polish) | Polish + reconvert |
|---|---|---|---|
| Visible on device | No (Kobo doesn't surface tags by default) | **No** (device reads from EPUB OPF) | Yes |
| Triggers redownload-required | Yes | Yes | Yes |
| Annotations preserved on device | No | **No** | No (and positions also break) |
| Reading position preserved | Yes (CW serves it) | Yes | Yes |

So **every Calibre-side edit is destructive to device-side annotations**. There's no "safe" workflow within the current sync model.

This converges all three loss mechanisms onto the same conclusion: the only durable annotation store is CW's own DB. Server-side capture (Phase 1) is the only viable answer, not a nice-to-have.

---

## Cloud-side behavior (the good news)

While device-local state is fragile, Kobo's cloud is a content-agnostic, durable, additive-by-default store. All of this was confirmed empirically on 2026-05-22.

### Cloud retention: annotations survive every device-side loss event

Verified via authenticated curl against `readingservices.kobo.com` immediately after a Mechanism 3 wipe of the Fae Queen book on the lab Kobo:

```bash
curl ... "https://readingservices.kobo.com/api/v3/content/9c798050-.../annotations?limit=100"
# → HTTP 200, full AnnotationSpan list (2 highlights, both with full position payload)
```

Both annotations the device had locally before the redownload were *still on Kobo's cloud* after the device wiped them. Cloud retains; device wipes; the device's `If-None-Match` etag matches the server's current etag, so the next device GET gets 304 and never receives the body — *until* something else invalidates the etag.

**Annotation loss on the device is orphaning, not destruction.** Data sits on Kobo's servers indefinitely (across redownloads, polishes, shelf moves). Whether the device sees it again depends on whether anything triggers an etag mismatch — see below.

### Verified storage model (DB inspection 2026-05-22)

A copy of the lab Kobo's `KoboReader.sqlite` was inspected:

- **Annotations live in the `Bookmark` table** (one row per annotation, schema includes `BookmarkID`, `VolumeID`, `ContentID`, `StartContainerPath`, `EndContainerPath`, `Text` = highlighted text, `Annotation` = note text, `Type`, `Color`, `Hidden`, `Published`, `SyncTime`).
- **The etag is stored in `content.AnnotationsSyncToken`** — a per-book column on the `content` (books) table. Each book has its own annotation sync token. There's also a `user.AnnotationsSyncToken` and a `user.NotebookSyncToken` for higher-scope tokens (purpose not yet investigated).
- The etag and the Bookmark rows are in different tables. A Mechanism 3 redownload wipes the Bookmark rows but leaves `content.AnnotationsSyncToken` intact — that's why the next GET sends the stale etag and returns 304.

### Orphans are self-healing on any cloud mutation

**End-to-end validation on main Kobo (2026-05-22 → 2026-05-23):**

Multiple recoveries on real main-library books with orphans:
- **Fan Service** (`cdee3fe3-...`, 17 orphans) — recovered after one new highlight. Book opened, all 17 visible.
- **The Risk** (`219b48d5-...`, 3 orphans) — recovered, same mechanism, didn't even need to leave/reenter the book; UI updated in place.
- **Falling Into Bed With a Duke** (`dbc1a8f9-...`, 2 dogears from 2025-10-27) — recovered AND captured on the wire after main Kobo started routing through CW. PATCH at 12:15:57.136, GET 200 at 12:15:57.613 — ~500ms cycle, fully visible in `[kobo-rs-mitm]` logs in run.

Main Kobo's wire became observable only after the conf-file workaround (see § "Operational" below) — the deployed mitm doesn't catch traffic until the device picks up the new `reading_services_host` URL via `HandleInitRequest`, which the device doesn't reliably call on cold boot.

Consistent with the model from lab: the device's annotation-creation flow fires PATCH immediately followed by GET in one burst — the GET returns 200 with the full body (including orphans), and the device's `Bookmark` table is populated within sub-second. The "recovery" feels instant from the user's perspective because the same PATCH that triggers etag invalidation also drives the GET that pulls the now-200 response.

Critical correction to earlier framing. Timeline from the 2026-05-22 session shows:

| Time | Event | GET response |
|---|---|---|
| 20:53 | After Mechanism 3 wipe, before any new annotation | 304 — orphans unreachable |
| 21:34:01 | User PATCH'd a new annotation; same-sync GET right after | **200** — etag now mismatched |
| All later GETs after any PATCH | | **200** every time |

Once the user created their first new annotation on the book, cloud's etag changed, the next GET returned 200 with the full body, and **the device repopulated its Bookmark table with the orphans alongside the new annotation**. Verified by inspecting the device DB at end of session: all three orphans + new annotations were present.

**So orphans persist only until the next cloud-side mutation on the same book's annotations.** Any new annotation, edit, or delete — from device, from web, from another Kobo on the account, hypothetically from CW — triggers etag invalidation → 200 → device re-pulls everything.

The user-perceived "chronic loss" comes from: a redownload wipes local annotations, then nothing else changes on that book's cloud annotations because the user doesn't return to it for a while. During that gap, the orphans sit invisible. The moment the user (or anyone) writes anything to that book's annotations, recovery is automatic.

### Implications for the design

1. **W5 (`If-None-Match` strip workaround) becomes much less interesting.** The device's recovery mechanism is already correct; we just need *something* to trigger it. A no-op PATCH on cloud (e.g., re-PATCH an existing annotation with the same body and a slightly-advanced `clientLastModifiedUtc`) would invalidate the etag and force the device to pull. Way simpler than mitm-stripping a header.

2. **Phase 1 backfill is still the right plan for CW-side recovery.** CW's foliate UI + Hardcover egress don't depend on the device repopulating. But we now know: even on the device itself, recovery is *one PATCH away* whenever the user wants to trigger it.

3. **Designing W4 (suppress redownload-required for content-unchanged updates) gets even more value.** If we suppress the redownload entirely on metadata-only edits, the device-local annotations never get wiped in the first place — no self-healing dance needed.

4. **CW could optionally include a "force-sync orphans" UI action** that does a no-op PATCH on a book's annotation set, triggering device re-pull on next sync. Tiny feature, high satisfaction value for the recovery story.

### Wire model (all four operations)

| Operation | Wire shape |
|---|---|
| Create | PATCH `{"updatedAnnotations": [{full body}]}` |
| Update (incl. type promotion, position change, color change, note add/edit) | PATCH with same id in `updatedAnnotations`, mutated fields |
| Delete | PATCH `{"deletedAnnotationIds": [id, ...]}` |
| Read | GET `?limit=N` with optional `If-None-Match: <etag-manifest>` |

Delete is **explicit-by-id**, never implied by absence. PATCHes are **additive** — the device sends only deltas, never full set-replace. The same envelope can presumably carry both `updatedAnnotations` and `deletedAnnotationIds` keys in one body (untested but consistent with REST conventions).

**Position mutability:** drag-to-extend a highlight on the device produces a same-id PATCH with mutated `location.span` and the device-recomputed `highlightedText` reflecting the new selection. Color change is the same shape with mutated `highlightColor`. All mutations — type promotion, position change, color change, note add/edit — funnel through `updatedAnnotations` with the same id. **Annotation identity is stable across every kind of edit Kobo's UI supports.**

**Type is mutable per id.** A `highlight` becomes a `note` when noteText is added; same id, same row.

**`highlightedText` is included on every update PATCH, not just creates.** Useful: CW always has current text content on every captured row, enabling text-search-based re-anchoring when positions later become invalid.

### Schema implication

`INSERT ... ON CONFLICT(user_id, book_id, source, source_annotation_id) DO UPDATE` is the correct write pattern for all incoming PATCHes. Position fields (`startPath` / `endPath` / `startChar` / `endChar`), content fields (`highlighted_text` / `note_text` / `highlight_color` / `context`), and `type` are all mutable in place on the same row. `source_updated_at` advances from each PATCH's `clientLastModifiedUtc`.

For deletes: soft-delete via `deleted_at` on the same row. Don't hard-delete — a stale device might re-PATCH a same-id annotation later, and we'd want to keep the tombstone for conflict-resolution decisions.

### Backfill is provably safe

A read-only CW handler that GETs from cloud and stores to `reader_annotation` cannot cause any cloud-side mutation. There is no PATCH operation in the read path. The "device has empty local state, are we about to nuke cloud?" risk we worried about earlier doesn't exist — Kobo's API has no absence-based deletion semantic.

### Empirical scale of recovery

Two passes of `kobo-cloud-annotations-dump.sh` against main library on 2026-05-22 → 2026-05-23:

| Scope | Books queried | Books with annotations | Total annotations |
|---|---|---|---|
| `KoboSyncedBooks` only (currently-synced) | 336 | 18 | 39 |
| **Full Calibre library** (`--all-library` flag) | **4,151** | **30** | **59** |

The full-library pass surfaced **+12 books / +20 annotations** that weren't visible via the synced-books filter — those are books either previously on the initial (failed) Kobo, or removed from the current device, or never re-synced after some past event. All of them retain cloud-side annotations under the user's account + book UUID.

**Implication for Phase 1 backfill scope:** iterate the full Calibre library, not just `KoboSyncedBooks`. The retention is genuinely book-scope (per `(kobo_user_id, content_uuid)`), not device-scope. The script supports this via `--all-library`. Tradeoff: ~50s vs ~10min runtime against main library, but the comprehensive set is what you want for backfill — historical annotations from prior devices and de-synced books are all recoverable.

**UUID model clarification (relevant to the cross-library/cross-device finding):**
- `book.id` (Calibre integer): auto-increments per library; meaningless across libraries
- `book.uuid` (Calibre GUID): generated at first add to a library; stable for that book within that library
- Adding the "same" EPUB to two different libraries usually produces two different UUIDs (Calibre doesn't always preserve the OPF-embedded UUID across libraries — confirmed empirically for Fae Queen: `0760c08f` in main, `9c798050` in lab)
- Kobo cloud keys annotations by `(kobo_user_id, content_uuid)` — so all Kobos on one account *synced through the same library* share an annotation namespace, but the same book in two libraries does not cross-pollinate
- Device replacement transparently inherits: kobo_user_id is account-scoped, the UUID comes from CW serving the same library, so initial-Kobo annotations are accessible to the replacement Kobo without manual migration

### Web-origin annotation propagation (read.kobo.com → device)

**Confirmed 2026-05-22.** Added two annotations on a Kobo-store-purchased book ("Set the Night on Fire", UUID `2a6e11ff`) via read.kobo.com, then synced the lab Kobo:

- Device sent `GET /api/v3/content/2a6e11ff.../annotations` with its previously-cached `If-None-Match` etag.
- Server returned **200** (not 304) — etag invalidated by the web-side mutation.
- Response body contained the full annotation list including the 2 new web-origin annotations.
- Device displayed them. User verified.

So the etag invalidation gates the device's catch-up. Whenever cloud state changes — by web reader, by another device, hypothetically by CW PATCHing on behalf of foliate — the next device GET returns 200 with the full body and the device updates its local view.

**This proves the wire-level mechanism for Phase 3's injection model works.** If CW ever pushes foliate-origin annotations to Kobo cloud (the only viable path for Kobo-store books), the device picks them up on next sync without any device-side cooperation needed.

**Additional wire details observed:**
- Web-origin annotations carry a `"text": "<same-uuid-as-id>"` field absent from NextGen's TypedDict. Same as our test highlights from today. Schema should carry it via `native_locator` JSON to avoid drift.
- `chapterFilename` inconsistency: web annotations use `/OEBPS/chapter-030.xhtml` (leading slash); older device-origin annotations use `OEBPS/chapter-025.xhtml`. Position converter should normalize on read.
- Web annotations may omit `chapterProgress` (observed). Converter must treat that field as optional.

### Markup type (stylus handwriting)

Discovered 2026-05-23 by writing on a Kobo-store book ("Set the Night on Fire") with a Kobo stylus. Markup annotations come back in the GET response alongside text annotations, but with a different payload shape:

```json
{
  "id": "<uuid>",
  "type": "markup",
  "clientLastModifiedUtc": "...",
  "location": {"span": {... same position fields as text annotations ...}},
  "attachments": {
    "<uuid>.jpg": {"contentType": "image/jpeg", "size": 145775, "eTag": "..."},
    "<uuid>.svg": {"contentType": "image/svg+xml", "size": 14996, "eTag": "..."}
  },
  "darkMode": false,
  "markupRect": "197,554,193,127",   // pixel rect on page
  "rangeRect": "60,604,295,62",      // pixel rect of underlying text
  "screenDimensions": "1264x1680",   // device screen size at time of capture
  "percentOnPage": 100,
  "headerType": 3, "footerType": 6,  // page-chrome state
  "fingerprintVersion": 1,
  "endKey": 1459, "startKey": 0,
  "readingFontFamily": "default",
  "readingFontSize": 46,
  "readingLeftMargin": 5, "readingRightMargin": 5, "readingLineHeight": 1.5,
  "readingAlignment": "",
  "eTagJpg": "", "eTagSvg": "",
  "highlightedText": ""
}
```

**Key differences from text annotations:**
- Actual stroke data is stored in attachment files (JPG raster + SVG vector), NOT in the JSON. The annotation API only references them by content-hash etags.
- **Pixel-positioned**, not text-range-positioned. `markupRect`/`rangeRect` are screen pixels.
- Captures rendering context (`screenDimensions`, `readingFontSize`, margins, line height, `headerType`/`footerType`). Markup is *layout-bound* — change the layout and the markup floats off the text.
- `fingerprintVersion: 1` — Kobo treats this as schema-versioned forward-compat data.
- Attachment files almost certainly live in `/api/UserStorage/*` (we've seen `GET /api/UserStorage/Metadata` in earlier captures but never inspected bodies; the markup id appears in the manifest fetched there).

**Sideloaded markup DOES sync to cloud (corrected 2026-05-23 after better log analysis).** Earlier observation of "no wire traffic for sideloaded markup" was a grep mistake. Verified: Kobo's cloud accepts PATCHes for `type=markup` against sideloaded UUIDs and stores both the metadata and the attachment files (JPG + SVG). Cloud-side recovery via the standard wire path works for markup the same as for text annotations, regardless of book provenance.

**Device storage** (still useful as a backup channel + for understanding):
- **`Bookmark` table** has rows with `Type='markup'`. Includes `BookmarkID`, `VolumeID`, `ContentID`, `StartContainerPath`, `ChapterProgress`, `DateCreated`. Position uses the text-based format (`span#kobo.N.M`), same as other annotation types.
- **`<KoboMount>/.kobo/markups/<BookmarkID>.{jpg,svg}`** holds the stroke files — local cache; same data is also in cloud.
- **`ExtraAnnotationData` column is empty across the entire DB.** Not used for markup or anything else.
- **Pixel-position + render-layout data** (markupRect, rangeRect, screenDimensions, font, margins) is **NOT persisted on device** — only transmitted at PATCH time. Cloud holds it (as we observed in GET responses). Device's local Bookmark row has only text-position; the full render context is cloud-only.

**Recovery paths for markup**:
- **Primary**: same wire-capture path as text annotations (Phase 1 handler catches PATCH bodies + attachment uploads, stores in CW DB).
- **Backup / alternate**: USB read of `/markups/` directory + `Bookmark` table rows. Useful if cloud sync fails for some user, or to verify state. Not the main path.

**Update semantics for markup**: same as text annotations — same-id PATCH with mutated fields. A user's "delete this and redraw" UI action becomes a single same-id update on the wire with mutated `markupRect`/`rangeRect`/font-size/etc. No delete+create cycle.

### Other on-device files

- **`BookReader.sqlite`** is Kobo Plus subscription tracking, encrypted, not annotation-relevant. Don't decode.
- **`Kobo eReader.conf`** holds the device's URL cache (`reading_services_host`, `image_host`, etc.). Manually editable when device is plugged in — used as the workaround for the W6 init-cache-invalidation gotcha.

**Implications for the design:**

- **Schema**: `reader_annotation.type` gains `markup` as a fourth enum value. Markup-specific fields (pixel rects, render context, attachment manifest) can ride in `native_locator` JSON without column changes.
- **Position model breaks down**: pixel-positioned markup doesn't translate to foliate's text-range model. Re-anchoring algorithms (find text in new EPUB) don't apply — markup is bound to the rendered layout, not the text content. Foliate can't meaningfully *render* markup at all without a totally different surface area (handwriting display, vector rendering, layout reproduction).
- **Attachments are a new concern**: capturing markup means either downloading + storing JPG/SVG blobs (real blob storage in CW) or storing only the metadata and treating markup as opaque references.
- **Phase 1 recommendation**: capture markup as opaque rows. Surface in foliate as a placeholder ("✍️ handwritten note here — view on device"). Defer real attachment fetch + render to a much later phase. Hardcover egress doesn't apply (Hardcover's Journal is text-based).
- **Sideloaded markup is potentially CW-only territory** if Kobo refuses these uploads. Genuinely big feature scope if we ever want to fully support stylus handwriting on sideloaded books — would need CW to receive uploads, store blobs, render in foliate. Most ambitious feature in the whole project; not Phase 1.

### Retention + endpoint uncertainty

Two important honesty caveats on the "cloud is durable" finding:

**On retention:**
- Oldest annotations we've successfully retrieved are from **2025-05-02** — about 12 months back as of investigation.
- That's only an observed lower bound. Kobo doesn't publish a retention policy. We have no data on what happens after 2+ years, or what triggers cleanup (account inactivity? specific data-cleanup events? terms-of-service changes?).
- For the user's specific case, ~1 year covers the full Kobo-ownership history, so backfill recovers basically everything that ever existed. For other potential users or longer time horizons, the uncertainty grows.
- **Implication**: Phase 1 backfill is time-sensitive. Each month of delay risks more annotations aging out under whatever real retention policy applies.

**On endpoint stability:**
- We've verified the per-UUID `GET /api/v3/content/<uuid>/annotations` endpoint extensively. CW should depend on that as the stable interface.
- Probed candidate "list-all" endpoints (2026-05-23): `/api/v3/annotations`, `/api/v3/content/annotations`, `/api/v3/user/annotations`, `/api/v3/notebooks`, etc. — all returned 404. No simple bulk-enumeration endpoint exists at the obvious paths.
- One real-but-unparsed endpoint found: `storeapi.kobo.com/api/internal/notebooks` returns 400 "ArgumentOutOfRangeException", suggesting it's a real route with parameters we haven't matched. The `user.NotebookSyncToken` column in `KoboReader.sqlite` hints at a notebook-sync mechanism that might use it. Worth deeper probing in a future session (cross-reference NextGen's work; possibly inspect Kobo desktop app via Charles Proxy).
- Either way: **Kobo could deprecate any endpoint at any time without notice.** CW's design should treat per-UUID GET as the load-bearing interface and any bulk endpoint as an opportunistic optimization. Per-UUID enumeration of 336 books takes ~50s total — acceptable for backfill.

### Bulk-fetch + token-refresh caveat

Bulk fetch works mechanically: with one valid bearer token + a UUID list, ~150ms/book over standard HTTPS. Token lifetime is ~1h (`exp - nbf = 3600s` per JWT). For a one-time backfill of a few hundred books, that's plenty. For ongoing automated sync, we'd need to capture the device's **refresh-token flow** — not yet observed on the wire. Likely involves `auth.kobobooks.com` and would require a separate mitm session targeted at that host.

---

## Cross-device / cross-library considerations

### Two Kobos on one account share an annotation namespace

Both Kobos on the user's account carry the same `kobo_user_id` in their JWT tokens (`49d0b9d7-...`). Kobo cloud keys annotations by `(user, content_uuid)`, no per-device partition. So:

- An annotation made on either Kobo lands in the same cloud namespace for that UUID.
- A token from either Kobo retrieves all annotations for any UUID.
- Two devices that have the same book (same UUID, same content) effectively cross-sync annotations through Kobo cloud automatically.

### Same book in different libraries can have different UUIDs

Confirmed via the audit on 2026-05-22: "Enchanting the Fae Queen" exists in both main library (`/Volumes/Satechi/macMini/calibre`, UUID `0760c08f-...`) and lab library (`/Volumes/Satechi/macMini/calibre-wt`, UUID `9c798050-...`). Calibre generates UUIDs at book creation; importing the same book into two libraries independently produces two UUIDs.

Implications:
- Main Kobo's annotations on Fae Queen (Feb 24 highlights in Ch 23) live under `0760c08f` and are not visible to lab Kobo.
- Lab Kobo's annotations (today, in Ch 1 / 11 / 12) live under `9c798050` and are not visible to main Kobo.
- They are *not* cross-syncing despite being the "same book" semantically.

For Phase 1 backfill: each library is enumerated independently, so this happens naturally. For any future "merge annotations across libraries" UX, CW would have to do explicit cross-UUID matching by text content — not free.

### Phantom collisions when content diverges

Theoretical concern (not yet observed): if main and lab libraries hold the same UUID but with different kepub content (e.g., one polished, one not), and both devices PATCH annotations to the same cloud namespace, positions from one device may point at garbled text on the other. Mitigation: store kepub-content-sha alongside each annotation row so CW can detect "this position was authored against a different kepub" and surface accordingly.

---

## Strategic decisions

### Stay on janeczku fork; don't migrate to NextGen

Decided 2026-05-22. See `project_nextgen_migration_eval.md`. NextGen has good annotation infrastructure but is missing fixes we depend on, and the patches-on-top model still serves us better. We harvest selectively — Hardcover egress code, annotation schema patterns, position-converter algorithm — without taking the whole.

### Foliate (readest fork) is the reader baseline

epub.js v0.3 is dead. The foliate migration is the right call regardless of annotation work, and annotation design assumes foliate.

### CW is authoritative; device + Kobo cloud are best-effort

Driven by the loss mechanisms above. Device-side state is fragile (shelf moves wipe; polish breaks positions). CW already plays this role for reading position (`KoboBookmark` + `ReaderPosition` + `HandleStateRequest`). Extending to annotations follows an internal pattern, not introducing one.

Implication for conflict resolution: CW resolves; device sees a single resolved state on next poll, never the intermediate. Last-write-wins on `source_updated_at`. Delete-wins for soft-deletes. CW's view never has to be transmitted as a three-way merge.

The honest scope of "CW-authoritative": we can change what foliate shows, what we serve on GET proxy, what we PATCH up to Kobo cloud. We *cannot* push to the device or undo a user action on the device until the device pulls. Eventual consistency on the device side is unavoidable. That's fine — the device was already the least durable of the three stores.

### CW owns sideloaded annotations; don't free-ride on Kobo's cloud

Even though Kobo's cloud will accept and serve sideloaded annotations (verified), we choose to make CW the source of truth for sideloaded UUIDs. Reasons:

- Privacy: CW reading data leaving for books Kobo doesn't host.
- Fragility: Kobo could add ownership validation any time.
- 3-clock conflict surface vs. 2-clock (CW + device only).
- EPUB-swap invalidation can be done locally (we know when an EPUB changes).

Implementation: the proxy short-circuits `/api/v3/content/<uuid>/annotations` for sideloaded UUIDs — serve from CW's DB, never forward to Kobo cloud.

For Kobo-store books: proxy through transparently in v1. CW may capture-and-mirror as a sidecar, but Kobo cloud remains involved.

### Adding `reading_services_host` to CW's served hosts is not a categorical step

Earlier framing treated the mitm-to-production move as a big new commitment. It isn't. CW already permanently serves `store_api_host`, `image_host`, and effectively `download_host`. One more host of the same kind.

---

## Phased approach

Each phase delivers standalone user value. Phase 3 may never need to be built.

### Phase 1: Capture + Hardcover egress (Kobo → CW → Hardcover)

One-way flow. No eventual-consistency UX problems anywhere.

- **Capture handler** on `reading_services_host`. Replaces the mitm logging proxy with a real PATCH handler:
  - Extracts the full position payload (`startPath`, `endPath`, `startChar`, `endChar`, `chapterTitle`, `chapterFilename`, etc.) — NextGen's handler discards these; ours must keep them.
  - Persists to CW DB as `source = 'kobo'` rows in the new `reader_annotation` table.
  - For sideloaded UUIDs: terminates here, don't proxy onward.
  - For Kobo-store UUIDs: proxy onward to Kobo cloud after capturing, so the device's existing flow is undisturbed.
- **GET handler** on `reading_services_host` for sideloaded UUIDs. Serves CW-stored annotations in the same envelope shape.
- **Hardcover egress.** New HTTP push integration sending captured annotations to Hardcover's Journal feature. Lift NextGen's HTTP client + payload mapping; their integration is already production-tested for this exact case.
- **Optional foliate-side read-only view.** Surface captured annotations in the foliate sidebar without edit/create yet. Cheap to add and independently valuable for re-reading in context.

**Value delivered:** annotations stop being lost. They live in CW DB durably. They surface on Hardcover Journal automatically. The foliate reader (optionally) shows them.

**Crucially:** no bidirectional anything, no inject mechanism, no conflict resolution complexity. The eventual-consistency / can't-push-to-device problems all belong to Phase 3.

### Phase 2: Foliate-origin annotations land in CW

Still one-way to CW. No injection into device GET responses.

- Foliate UI for create / edit / delete.
- CW stores `source = 'foliate'` rows.
- These appear in foliate sidebar (they're in CW's DB).
- These egress to Hardcover via the Phase 1 pipeline.
- They do **not** propagate to the Kobo device.

**Value delivered:** the web reader is a real annotation surface that ends up on Hardcover. For books the user reads on the web, this closes the loop.

### Phase 3: Bidirectional — foliate annotations reach the device

Maybe never builds. The Phase 1 + 2 work captures the majority of the practical value; bidirectional is the optional polish.

- Inject mechanism in the GET annotations response — modify the proxied payload (Kobo-store) or serve from CW (sideloaded) so the device picks up foliate-origin annotations on next poll.
- Conflict resolution kicks in for real (multiple sources may now claim the same annotation).
- Round-trip identity: annotation IDs must be stable across foliate → CW → device. Either foliate generates UUIDs that we use directly, or CW assigns at ingest. Decide before any inject work.

---

## Storage schema

New `reader_annotation` table, modeled on the existing `reader_position` (`feat/epub-reader-foliate` branch) pattern. Not extending `KoboBookmark` (wrong cardinality, wrong scope), not lifting NextGen's `KoboAnnotationSync` wholesale (too coupled to their epub.js + Hardcover stack).

```python
class ReaderAnnotation(Base):
    __tablename__ = 'reader_annotation'
    __table_args__ = (UniqueConstraint('user_id', 'book_id', 'source', 'source_annotation_id',
                                       name='uq_reader_annotation_user_book_source_id'),)
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('user.id'), nullable=False)
    book_id = Column(Integer, nullable=False)
    source = Column(String, nullable=False)              # 'kobo' | 'foliate'
    source_annotation_id = Column(String, nullable=False)# Kobo UUID, or foliate-generated
    type = Column(String, nullable=False)                # 'highlight' | 'note' | 'dogear'
    created_at = Column(DateTime, ...)
    updated_at = Column(DateTime, ..., onupdate=...)
    source_updated_at = Column(DateTime, nullable=True)  # clientLastModifiedUtc on wire
    deleted_at = Column(DateTime, nullable=True)         # soft-delete tombstone
    # Destination sync state
    hardcover_synced_at = Column(DateTime, nullable=True)
    # Content
    highlighted_text = Column(Text, nullable=True)
    note_text = Column(Text, nullable=True)
    highlight_color = Column(String, nullable=True)      # hex; translate on egress
    context = Column(Text, nullable=True)                # dogear context snippet
    # Position
    doc_href = Column(String, nullable=True)             # chapterFilename
    book_progress = Column(Float, nullable=True)
    doc_progress = Column(Float, nullable=True)          # chapterProgress
    cfi_range = Column(String, nullable=True)            # derived if/when
    payload_version = Column(Integer, default=1, nullable=False)
    native_locator = Column(JSON, default=dict, nullable=False)
```

### Why this shape

- **`source` column + `(user_id, book_id, source, source_annotation_id)` uniqueness** — each source has its own row per annotation. No source overwrites another's. Source-tagged provenance is explicit. Mirrors `ReaderPosition`'s `(user, book, source)` design.
- **Multi-representation position** — `doc_href`, `book_progress`, `doc_progress`, `cfi_range`, `native_locator` (JSON). Same philosophy as `ReaderPosition`: don't pick a canonical form, carry whatever each source produces.
- **`payload_version` + `native_locator` JSON** — schema can evolve without ALTER TABLE for every new field Kobo adds (e.g. the unexplained `"text": "<uuid>"` field we observed on new highlights).
- **`source_updated_at` distinct from `updated_at`** — wire clock vs CW clock. Conflict resolution reads `source_updated_at`.
- **`hardcover_synced_at`** — per-destination sync state. Just one for now. Generalize to a `sync_state` JSON or separate destinations table if a second destination ever appears.
- **`deleted_at` soft-delete** — needed for delete-wins resolution; can't just hard-delete because the device might re-PATCH the deleted annotation later.

### Bookmarks vs annotations

Same table. Kobo treats "bookmark" (dogear) as just another type in the `/annotations` payload. Foliate's bookmark concept maps directly. Splitting them across tables would force rejoining on the wire and gives the reader UI nothing useful — render distinctions are cosmetic and live in the view layer.

### Schema migration

Standalone new table. No data migration. `migrate_reader_annotation_table(engine, _session)` following the `migrate_reader_position_table` pattern in `cps/ub.py` for additive evolution.

---

## Investigation plan

Status per item: ✅ done in 2026-05-22 session / ⏳ open / 🟡 partial.

### A. Where is the annotation loss actually originating?

1. ✅ **Metadata-only edit without polish (A.1)** — DONE for tag + summary on lab Kobo. Result: edits do not reach the device (Kobo reads metadata from the EPUB OPF, not SyncResponse), but the `last_modified` bump triggers redownload-required *and* the redownload destroys device-side annotations. This is **Mechanism 3** above. Falsified the "don't polish for metadata" hypothesis — see W1 below. Snapshot artifacts in `/tmp/annot-test/43/`.

   Sub-question still open: do title/author/series behave the same as tags/summary? Architecturally yes (same `get_metadata` flow), but not directly verified.

2. ⏳ **Polish + reconvert (A.2)** — NOT YET DONE. Mechanism 2 (span shift on polish) is the theoretical expectation. Direct verification would be: snapshot book → polish + reconvert in Calibre app → snapshot → diff. `annot-test-snapshot.sh` already supports this; it will show whether kepub sha changes, span set changes, and (separately) whether the UUID stays stable.

3. ⏳ **Shelf move A→B between two synced shelves (A.3)** — NOT YET DONE. Mechanism 1a documented from code; needs an empirical test to confirm whether the user's "even within a second" observation holds and whether annotations are wiped.

4. ⏳ **Shelf `kobo_sync` toggle off then on (A.3b)** — NOT YET DONE. Mechanism 1b documented; needs empirical confirmation.

5. 🟡 **UUID stability across polish (A.4-UUID)** — INDIRECT EVIDENCE so far. Fae Queen still has the same UUID it had in January, and Feb 24 annotations are still retrievable under that UUID, implying polish doesn't rotate UUIDs. But no direct before/after polish snapshot. Will be settled by A.2 when run.

6. 🟡 **Kepub span stability across polish (A.4-spans)** — INDIRECT. Strong prior that polish + reconvert generates different spans (kepubify is content-derived). A.2 will measure exactly how lossy.

### B. Does reading position also get lost on these events?

🟡 **Partially confirmed** — Mechanism 3 testing (A.1) showed reading position SURVIVED across the wipe-and-redownload, because CW serves position from `KoboBookmark`/`ReaderPosition` via `HandleStateRequest`. So the CW-side capture-and-serve pattern works for position. Whether it also works through shelf-move events (Mechanism 1a/1b) is not yet tested — A.3 / A.3b will cover this when run.

### C. Refresh-token flow

⏳ **NOT YET CAPTURED.** Bearer tokens last ~1h. A one-shot backfill works with any single captured token, but ongoing automation (e.g., a daily capture pass) needs to refresh tokens autonomously. Would need a separate mitm session targeted at `auth.kobobooks.com`.

### D. Hardcover Journal API surface

⏳ **NOT YET VERIFIED.** NextGen's H1 PR series (#245 likely) is the reference. Need to confirm:
- Endpoint(s) for posting a quote/note.
- Auth scheme (OAuth, API key?).
- Whether highlights vs notes vs dogears map cleanly to Journal types.
- Rate limits / batching expectations.

Worth doing before committing to Hardcover as Phase 1's egress target.

---

## Workflow improvements (separate from sync work)

Mitigations to annotation loss that don't require building the sync system. Some are user-side discipline; some are CW-side code changes. Ordered by actionability.

### W2. Sync-removal diff shouldn't fire for transient/recoverable removals

Blocks the chronic shelf-organize loss (Mechanism 1a/1b). The per-sync diff in `HandleSyncRequest` (`synced_ids - shelf_ids`) doesn't distinguish "intentionally removed" from "transiently absent" (shelf-move) or "temporarily off-synced" (shelf `kobo_sync` toggle).

Possible mitigations:

- **Grace window on the diff.** Skip emitting `IsRemoved=true` for books whose `KoboSyncedBooks` entry crossed into the "not on any kobo-synced shelf" state within the last N seconds (e.g. 30). Track transition timestamps. If the book returns to `shelf_ids` within the window, no removal is ever sent.
- **UI: add-before-remove for moves.** When the user moves a book from A to B in the UI, perform the add to B before the remove from A. The book is never absent from `shelf_ids` from CW's perspective. Requires identifying UI flows that do remove-then-add; the "edit shelves" multi-checkbox UI is the obvious one.
- **Defer-sync on shelf mutations.** Suppress device-sync triggers (if any are wired up) for a brief window after any shelf-membership change.
- **Audit sync-trigger hooks.** Find what (if anything) in CW causes the lab Kobo to poll right after a UI shelf change.
- **Explicit "delete from kobo" action vs. shelf mutations.** Most user shelf actions are organizational and recoverable. A separate, explicit "remove from kobo" action would tell CW the user really means it; normal shelf actions don't emit `IsRemoved`. Requires UX changes; more invasive but semantically cleaner.

Worth a separate small branch (e.g. `bug/kobo-sync-removal-grace`) once 1a/1b are empirically confirmed via investigation A.3 / A.3b. Independent of annotation sync work, but blocks the chronic-loss problem regardless of whether sync ever ships.

### W4. Suppress redownload-required for content-unchanged updates

Blocks the metadata-edit annotation loss (Mechanism 3). The device flags redownload whenever `book.last_modified` advances, but metadata-only edits don't actually change the kepub. Suppressing the device-facing ChangedEntitlement for these cases would preserve device-side annotations as a side effect.

Sketch:
- CW fingerprints the kepub on first sync (sha256 in a new column on `KoboSyncedBooks`).
- On subsequent syncs, before emitting `ChangedEntitlement`, compare current kepub sha256 to the stored fingerprint.
- If unchanged, don't emit the ChangedEntitlement at all — `book.last_modified` advanced but nothing the device cares about did.
- If changed, emit and update the stored fingerprint.

Side effects to think through:
- Device's `last_modified` view diverges from CW's. Probably fine — device doesn't use this beyond noting "something changed."
- If we ever want to push pure-metadata updates (future feature), this would suppress that. Bound the suppression to "kepub-content-bound" fields only.
- New books still flow through `NewEntitlement` unchanged.

Worth a small `bug/kobo-suppress-noop-changes` branch. Independent of annotation sync; would be a workflow win on its own merits.

### W5. Strip `If-None-Match` on device GET to force annotation re-fetch (experimental)

Driven by the cloud-retention finding: the device's annotation-ETag manifest stays in sync with the cloud's even when the device's local annotation data is wiped (Mechanism 3). So next-poll GET returns 304 and the device never restores its local view from cloud.

Sketch: in the mitm proxy (or its Phase-1 successor), detect "device just redownloaded book X" events and on the next annotation GET for book X, strip `If-None-Match` from the request before forwarding. Kobo cloud will return 200 with the full body. Whether the device then *re-populates* its local store from that body is the open question — Kobo firmware may keep its empty local state regardless.

Untested. Worth experimenting on lab after Phase 1 stabilizes CW's own annotation store. If it works, it's a device-side fix that doesn't require CW changes to the foliate reader; if it doesn't, Phase 1's foliate + Hardcover surfacing is the only recovery path. Either way, Phase 1 captures the data into CW's DB first; W5 is a stretch experiment, not a fallback.

### W1. ~~Don't polish for metadata-only edits~~ (FALSIFIED 2026-05-22)

Original idea: metadata fields flow through `metadata.db` directly to the Kobo sync response, so polishing for metadata-only changes is unnecessary.

A.1 testing falsified this. The device reads metadata from the EPUB's embedded OPF, not from the SyncResponse, so an unchanged kepub means no visible metadata change on the device. Worse, the `last_modified` bump triggers redownload-required regardless, and the redownload itself destroys annotations (Mechanism 3).

There's no benign workflow within the current sync model. The user was right to polish. Replaced by W4 (suppress noop redownloads) which addresses the actual problem from the CW side.

### W6. Force-trigger device re-init after CW URL changes (OPEN PROBLEM)

Identified 2026-05-23 during the canary mitm deployment to run. The device caches all CW-routed URLs from `HandleInitRequest` responses. Until the device next calls `/v1/initialization`, it keeps using whatever URLs it had cached previously — even after a fresh CW deploy that overrides different resources.

**The problem we hit:** deployed canary to run at 22:46 (added `reading_services_host` override). Main Kobo continued sending annotation traffic directly to `readingservices.kobo.com`, bypassing CW. A power-cycle of main Kobo on 2026-05-23 morning did **not** trigger a new `Init` call in CW's log. Cold boot alone is not sufficient — Kobo's firmware apparently only re-inits under certain conditions we haven't characterized.

**Workarounds we used:**
- **Manual conf-file edit** — plug in the device, edit `<KoboMount>/.kobo/Kobo/Kobo eReader.conf` to set `reading_services_host=http://<CW-host>:<port>` directly. Bypasses init entirely; takes effect on next sync. This is what unblocked the main-Kobo wire-observation work tonight.

**Approaches not yet tested:**
- Returning specific error codes from `HandleSyncRequest` to trigger re-init
- Regenerating CW's per-user API token (which would invalidate the device's URL cache)
- Special response headers on CW endpoints
- Account de/re-registration

**Broader-than-annotations implication:** any user whose CW URL ever changes (new domain, switched ports, internal IP → Tailscale, machine move) likely has Kobos with stale cached URLs that silently break things — covers missing, library not syncing, downloads failing. Plausible explanation for known-mysterious "covers don't work" issues in CW's broader user base. Anyone whose only fix is "reboot the Kobo" (which sometimes works) is hitting this exact issue. Worth searching upstream issues for evidence.

**For Phase 1 release:** annotation sync deployment to other users would need either reliable force-init OR a clear manual procedure. Documenting "edit your Kobo's conf file after enabling annotation sync" is achievable but ugly. A real solution (researched force-init mechanism) would be more polished. This becomes part of Phase 1 scoping work, not a pre-Phase-1 blocker.

### W3. Read-only CW on Calibre metadata (tabled)

Considered: switch from rsync-from-laptop to shared storage with CW configured read-only on `metadata.db` to prevent concurrent-write corruption. User decided to keep current workflow because:

- Calibre app UI is materially better for processing incoming books / editing metadata than CLI alternatives.
- Even with read-only CW + shared storage, the polish loss mechanism still occurs whenever the user *does* edit a book.
- Concurrent-write corruption isn't currently happening with the stop-sync-restart dance.

Note for future: if the laptop workflow ever becomes constraining, this option exists.

---

## Re-anchoring strategy (for when annotations survive structurally but positions shift)

Future work; not on Phase 1's critical path. Sketch only:

When CW detects a book's content has changed (kepub re-uploaded, hash differs from prior version), iterate the book's annotations. For each:

1. Read `highlighted_text` (we store it in the new schema).
2. Search the new kepub's content for that text.
3. If found exactly: derive new `startPath` / `endPath` / `startChar` / `endChar` from the match. Update the row in place.
4. If not found exactly: fuzzy-match (Levenshtein or similar) and either auto-update with a confidence flag or surface in the foliate sidebar as "needs review."
5. If not findable at all: keep the row with `payload_version` bumped and a `position_status = 'orphan'` flag; surface in UI.

Most annotations have substantial `highlighted_text` content (sentences, not single words). Match rate should be high. Dogears are the worst case — they may have only `context` instead of `highlightedText`.

---

## Operational notes

### Production rollout 2026-05-23

`debug/kobo-reading-services-mitm` was added to `canary.conf` and deployed via `build.sh canary --target run` at 2026-05-22 22:46. Run/stable → run/canary; main Kobo is now (after conf-file workaround) routing annotation traffic through CW where it's logged with `[kobo-rs-mitm]` prefix.

Branch is still labeled `debug/` despite now being load-bearing production infrastructure — violates the `debug/` hygiene rule in `AGENTS.md` (debug branches should be logging-only and disposable). Worth renaming to `feat/kobo-readingservices-proxy` or similar when convenient.

### Deployment cache-invalidation gotcha

See § W6. After tonight's deploy, main Kobo continued bypassing CW for annotations because `HandleInitRequest` wasn't called. Required manual conf-file edit on main Kobo to push the `reading_services_host` override. **Future CW deploys that add or change URL overrides will hit this same gotcha.** Document the workaround in build/deploy notes.

## Tooling built during the 2026-05-22 session

Two reusable scripts in `meta/`, both uncommitted to the `meta` branch at session end:

- **`meta/scripts/annot-test-snapshot.sh`** — capture before/after snapshots of a book in any Calibre library. Records UUID, metadata fields, EPUB/kepub/cover sha256 + size + mtime, and the kepub's koboSpan id set. `diff` mode shows what changed across snapshots. Used for investigations A.1, A.2, A.4. Output to `/tmp/annot-test/<book_id>/{before,after}/`.

- **`meta/scripts/kobo-cloud-annotations-dump.sh`** — enumerate every book in a `KoboSyncedBooks` table, GET annotations from Kobo cloud per UUID, dump as JSON. Accepts either a direct bearer token or `@<logfile>` to auto-extract from the freshest mitm-logged token. Used to prove cloud retention and quantify the recoverable-annotations dataset (39 across 18 books in main library). Output: `summary.tsv` + per-book JSON files.

- **`meta/scripts/kobo-snapshot.sh`** — copy a connected Kobo's on-disk state (KoboReader.sqlite, BookReader.sqlite, Kobo eReader.conf, affiliate.conf, full markups/ directory) to a timestamped output dir with sha256 manifest. Used to inspect device state, verify wire-vs-device divergence, and (in Phase 1) potentially as the basis for a USB-based markup ingestion path.

Both should be committed to the `meta` branch alongside this doc when convenient. They're not load-bearing (one-shot investigation tools), but they're useful and worth keeping.

---

## Hardcover Journal integration notes

Hardcover has a Journal feature designed for "notes/quotes while reading." Maps naturally to our annotation types — highlights become quotes, notes become annotations. Endpoint and payload shape: borrow from NextGen's H1 PR series (#245 likely the relevant one). Verify the Hardcover API actually accepts highlights / notes from third-party clients before committing.

---

## Out of scope for this design

- **Calibre desktop reader integration.** Future possibility; not in Phase 1-3.
- **Multi-device foliate annotation sync between web users.** Single-user assumption; expand later if needed.
- **Real-time push to the device.** Not mechanically possible without device firmware changes; eventual-consistency is accepted.
- **PDF / format-other-than-EPUB annotations.** EPUB / kepub only for v1.

---

## Next session starting point

The investigation is essentially complete. Next session is about deciding what to *do* with what we know.

1. **Decide Phase 1 scope.** All wire mechanics confirmed; recovery validated end-to-end on real main-Kobo data; backfill demonstrated working. Time to scope the first implementable slice. Probable branch: `feat/kobo-annotation-capture`.
2. **Independently scope W2, W4, W6** as `bug/` branches. Each addresses a real loss/UX problem and stands alone — workflow wins regardless of whether annotation sync ships.
3. **Open investigations remaining (cheap, can fold into Phase 1 planning):**
   - A.2 — polish + reconvert empirical kepub-span shift confirmation
   - A.3 / A.3b — shelf-move + shelf-toggle device behavior empirical confirmation
   - C — refresh-token capture (needed for long-term automated CW backfill, not for Phase 1 MVP)
   - D — Hardcover Journal API surface verification (needed before committing to Hardcover as Phase 1 egress)
   - W6 — research force-re-init mechanisms; affects deployment story for any user beyond ourselves
4. ~~Commit tonight's tooling~~ — done end of session; see `meta/scripts/` for the three helper scripts.
5. **Consider renaming** `debug/kobo-reading-services-mitm` → `feat/kobo-readingservices-proxy` since it's now load-bearing production infrastructure, not debug-only.
