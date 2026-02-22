"""Tests for kobo reading state (progress percent, bookmark sync).

Graduated from pr/fix-kobo-progress, which was merged upstream in
upstream/master @ 32741be6.
"""
from datetime import datetime, timezone

from cps import ub

from kobo_test_support import import_kobo


def test_progress_percent_whole_number_returned_as_int():
    """ProgressPercent should be int (not float) when value is a whole number.

    Kobo devices send ProgressPercent as an integer (e.g., 33) but SQLite
    Float columns store it as 33.0. Returning 33.0 causes the device to
    show a spurious "Return to last page read" popup.
    """
    kobo = import_kobo()

    bookmark = ub.KoboBookmark()
    bookmark.last_modified = datetime.now(timezone.utc)
    bookmark.progress_percent = 33.0  # Stored as float by SQLite
    bookmark.content_source_progress_percent = 33.0
    bookmark.location_value = "/some/spine"
    bookmark.location_type = "SPINE_POSITION"
    bookmark.location_source = "spine"

    resp = kobo.get_current_bookmark_response(bookmark)

    assert resp["ProgressPercent"] == 33
    assert isinstance(resp["ProgressPercent"], int), \
        f"Expected int, got {type(resp['ProgressPercent']).__name__}: {resp['ProgressPercent']}"
    assert resp["ContentSourceProgressPercent"] == 33
    assert isinstance(resp["ContentSourceProgressPercent"], int)


def test_progress_percent_fractional_preserved_as_float():
    """ProgressPercent should remain a float when it has a fractional part."""
    kobo = import_kobo()

    bookmark = ub.KoboBookmark()
    bookmark.last_modified = datetime.now(timezone.utc)
    bookmark.progress_percent = 33.5
    bookmark.content_source_progress_percent = 33.5
    bookmark.location_value = None
    bookmark.location_type = None
    bookmark.location_source = None

    resp = kobo.get_current_bookmark_response(bookmark)

    assert resp["ProgressPercent"] == 33.5
    assert isinstance(resp["ProgressPercent"], float)
    assert resp["ContentSourceProgressPercent"] == 33.5
    assert isinstance(resp["ContentSourceProgressPercent"], float)


def test_progress_percent_zero_is_included():
    """ProgressPercent: 0 must be included in the response, not omitted.

    When a book is at position 0 (e.g. comics where ProgressPercent stays 0
    while ContentSourceProgressPercent tracks pages), the Kobo sends
    ProgressPercent: 0. SQLite stores it as 0.0. A truthiness check on 0.0
    is False, which previously caused the field to be dropped from the response.
    The Kobo then sees its local 0 vs the server's missing field and shows
    the "Return to last page read" popup.
    """
    kobo = import_kobo()

    bookmark = ub.KoboBookmark()
    bookmark.last_modified = datetime.now(timezone.utc)
    bookmark.progress_percent = 0.0
    bookmark.content_source_progress_percent = 0.0
    bookmark.location_value = None
    bookmark.location_type = None
    bookmark.location_source = None

    resp = kobo.get_current_bookmark_response(bookmark)

    assert "ProgressPercent" in resp, "ProgressPercent: 0 must not be omitted from response"
    assert resp["ProgressPercent"] == 0
    assert isinstance(resp["ProgressPercent"], int)
    assert "ContentSourceProgressPercent" in resp, "ContentSourceProgressPercent: 0 must not be omitted"
    assert resp["ContentSourceProgressPercent"] == 0
    assert isinstance(resp["ContentSourceProgressPercent"], int)


def test_progress_percent_none_is_omitted():
    """ProgressPercent should be absent when never set (fresh/unread book).

    None means the Kobo has never synced a reading position for this book.
    The field should be omitted entirely, not returned as null or 0.
    """
    kobo = import_kobo()

    bookmark = ub.KoboBookmark()
    bookmark.last_modified = datetime.now(timezone.utc)
    bookmark.progress_percent = None
    bookmark.content_source_progress_percent = None
    bookmark.location_value = None
    bookmark.location_type = None
    bookmark.location_source = None

    resp = kobo.get_current_bookmark_response(bookmark)

    assert "ProgressPercent" not in resp
    assert "ContentSourceProgressPercent" not in resp
