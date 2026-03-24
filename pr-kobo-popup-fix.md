# Fix: Kobo "Return to Last Page Read" popup on sync

## Problem

When syncing a Kobo device, users see a "Do you want to return to the last page you read?" popup even
though they have been reading on that device the whole time and no other device is involved. The popup
is disruptive and the answer is always "no" — the device already has the correct position.

## Root cause

The popup is purely timestamp-driven. The Kobo compares the `PriorityTimestamp` (PT) it receives in
the GET `/reading-state` response against the PT from its previous GET. If the new PT is newer, it
interprets that as another device having updated the position and shows the popup.

The bug is that our server was always stamping PT with `datetime.now()` at processing time. Every PUT
from the device created a fresh server-side timestamp. When the device later did a GET, it saw a PT
newer than the one it had last confirmed — and showed the popup — even though the data itself was
unchanged.

**Comparison with official Kobo cloud (captured via proxy):** The official server does not generate
its own timestamps. It echoes the device's own `LastModified` value back in the GET response. The
device recognizes its own timestamp and sees no conflict.

## Fix

### Commit 2 — use the device's own timestamp (core fix)

In the PUT handler, read `LastModified` from the request body (the timestamp the device generated
itself) and use it as PT and LM for all state written in that request, via Flask's `g`. When the
device subsequently does a GET, it receives back the same timestamp it sent — no conflict, no popup.

```python
lm_str = request_reading_state.get("LastModified")
request_lm = datetime.strptime(lm_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
g.kobo_reading_state_lm = request_lm
```

The `receive_before_flush` SQLAlchemy event hook then uses this value (falling back to
`datetime.now()` for non-Kobo writes, so web UI updates are unaffected).

This also fixes a minor logic issue in status handling: `book_read.last_modified` was being set on
every PUT regardless of whether the status actually changed. It now only updates when the status
changes, matching official cloud behavior.

### Commit 1 — ensure PT always equals LM; revert an upstream addition

This commit does two related things:

**1a. Remove `onupdate` from `priority_timestamp`.**

`KoboReadingState` had both `last_modified` and `priority_timestamp` with independent `onupdate`
hooks that each called `datetime.now()`. This meant they could drift apart. A PT != LM in a GET
response is itself a popup trigger (the device expects them to be in sync). The fix removes
`onupdate` from PT and instead has the `receive_before_flush` hook set both fields to the same
value at once, maintaining the invariant PT == LM at all times.

**1b. Remove LM/PT from the PUT response body.**

Commit `2d4ca23d` (merged upstream March 1) added `LastModified` and `PriorityTimestamp` to the
`UpdateResults` object in the PUT `/state` response, with the intent of letting the device confirm
the new PT immediately without needing a GET.

This does not work. The Kobo device does not persist state from PUT responses — it only persists
state confirmed by GET responses. We verified this: the device reverts to its last GET-confirmed PT
after sleep regardless of what the PUT response contained. The official Kobo cloud PUT response
contains no LM or PT in `UpdateResults` at all. These two lines are therefore sending a
server-generated timestamp the device will never confirm, which makes the problem worse rather than
better.

## Why commit 1 is a prerequisite for commit 2

Commit 2 makes the flush hook stamp PT = LM = device timestamp. For this to be safe, we need the
guarantee that nothing else can independently advance PT (the old `onupdate`) or put a
server-generated PT back into the response (the removed PUT response lines). Commit 1 establishes
those invariants; commit 2 then plugs in the correct timestamp source.
