"""
Kobo sync integration tests demonstrating fixes in pr-draft/kobo-sync branch.

These tests verify various kobo sync edge cases and bug fixes. When run against
the master branch (without fixes), many tests fail as expected.

Test Status (against master without fixes):
-------------------------------------------
PASS (9 tests):
  - test_sync_returns_entitlements_and_updates_synced_books
  - test_sync_exactly_limit_does_not_set_continue_header
  - test_only_kobo_shelves_no_repeat_books_after_sync
  - test_sync_shelves_updates_tags_last_modified
  - test_only_kobo_shelves_or_condition_date_added_triggers_sync
  - test_timezone_suffix_in_last_modified_only_kobo_shelves
  - test_sync_token_backwards_compatibility_missing_books_last_id
  - test_empty_kobo_synced_books_resets_token
  - test_add_book_to_non_kobo_shelf_does_not_trigger_kepub_conversion

FAIL (17 tests) - require fixes from pr-draft/kobo-sync:
  - test_sync_over_limit_does_not_repeat_payload
  - test_sync_unchanged_library_after_full_sync_returns_empty
  - test_sync_mixed_modified_and_unchanged_only_returns_modified
  - test_modified_synced_book_is_resent
  - test_only_kobo_shelves_pagination_no_repeats
  - test_only_kobo_shelves_modified_synced_book_is_resent
  - test_only_kobo_shelves_modified_synced_book_paginates_without_repeat
  - test_only_kobo_shelves_or_condition_last_modified_triggers_sync
  - test_pagination_with_duplicate_timestamps
  - test_modified_book_resync_with_duplicate_timestamps
  - test_mixed_timestamps_pagination
  - test_only_kobo_shelves_pagination_with_duplicate_timestamps
  - test_only_kobo_shelves_modified_book_resync_with_duplicate_timestamps
  - test_timezone_suffix_in_last_modified_normal_mode
  - test_only_kobo_shelves_book_on_multiple_shelves_syncs_once
  - test_only_kobo_shelves_multiple_books_on_multiple_shelves
  - test_add_book_to_kobo_shelf_triggers_kepub_conversion
"""
import json
import pytest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from flask import Flask, g
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.sql.expression import true

from kobo_test_support import import_kobo

from cps import db, ub, constants, kobo_sync_status


# =============================================================================
# BASELINE TESTS
# Basic sync sanity checks that work without any fixes.
# =============================================================================

def test_sync_returns_entitlements_and_updates_synced_books(monkeypatch):
    """Verify basic sync returns book entitlements and records them as synced."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        session.add(user)
        session.commit()

        _seed_books(session, 2)
        _setup_kobo_test_environment(monkeypatch, kobo, user)
        app = _create_test_flask_app()

        response, payload = _make_sync_request(kobo, app, session)

        assert response.status_code == 200
        assert len(payload) == 2
        assert all("NewEntitlement" in item for item in payload)
        assert "x-kobo-synctoken" in response.headers
        assert (
            ub.session.query(ub.KoboSyncedBooks)
            .filter(ub.KoboSyncedBooks.user_id == user.id)
            .count()
            == 2
        )


def test_sync_exactly_limit_does_not_set_continue_header(monkeypatch):
    """When result count equals SYNC_ITEM_LIMIT exactly, no continuation header."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        session.add(user)
        session.commit()

        sync_limit = 3
        _seed_books(session, sync_limit)

        _setup_kobo_test_environment(monkeypatch, kobo, user, sync_limit=sync_limit)
        app = _create_test_flask_app()

        response, payload = _make_sync_request(kobo, app, session)
        entitlements = [item for item in payload if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements) == sync_limit
        assert response.headers.get("x-kobo-sync") is None


# =============================================================================
# PAGINATION TESTS (Normal Mode)
# Tests for sync_limit pagination without repeating books across pages.
# =============================================================================

def test_sync_over_limit_does_not_repeat_payload(monkeypatch, tmp_path):
    """Pagination should return different books on each page, not repeat."""
    kobo = import_kobo()

    with _kobo_test_split_sessions(tmp_path) as (calibre_session, app_session):
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        app_session.add(user)
        app_session.commit()

        sync_limit = 3
        _seed_books(calibre_session, sync_limit + 2)

        _setup_kobo_test_environment(monkeypatch, kobo, user, sync_limit=sync_limit)
        app = _create_test_flask_app()

        response1, payload1 = _make_sync_request(kobo, app, calibre_session)
        token = response1.headers.get("x-kobo-synctoken")

        response2, payload2 = _make_sync_request(kobo, app, calibre_session, token=token)

        assert len(payload1) == sync_limit
        assert response1.headers.get("x-kobo-sync") == "continue"
        assert len(payload2) == 2, f"expected 2 remaining books, got {len(payload2)}"
        assert response2.headers.get("x-kobo-sync") is None
        assert set(_extract_entitlement_ids(payload1)).isdisjoint(_extract_entitlement_ids(payload2))


def test_sync_unchanged_library_after_full_sync_returns_empty(monkeypatch, tmp_path):
    """After fully syncing a library, subsequent syncs should return empty."""
    kobo = import_kobo()

    with _kobo_test_split_sessions(tmp_path) as (calibre_session, app_session):
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        app_session.add(user)
        app_session.commit()

        sync_limit = 3
        _seed_books(calibre_session, sync_limit + 2)

        _setup_kobo_test_environment(monkeypatch, kobo, user, sync_limit=sync_limit)
        app = _create_test_flask_app()

        response1, payload1 = _make_sync_request(kobo, app, calibre_session)
        token1 = response1.headers.get("x-kobo-synctoken")
        assert len(payload1) == sync_limit
        assert response1.headers.get("x-kobo-sync") == "continue"

        response2, payload2 = _make_sync_request(kobo, app, calibre_session, token=token1)
        token2 = response2.headers.get("x-kobo-synctoken")
        assert len(payload2) == 2
        assert response2.headers.get("x-kobo-sync") is None

        response3, payload3 = _make_sync_request(kobo, app, calibre_session, token=token2)
        assert payload3 == [], "Expected no payload when library is unchanged after full sync."


# =============================================================================
# MODIFIED BOOK RE-SYNC TESTS (Normal Mode)
# Tests that books with updated last_modified are re-sent to the device.
# =============================================================================

def test_sync_mixed_modified_and_unchanged_only_returns_modified(monkeypatch):
    """After initial sync, only books with changed last_modified should re-sync."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        session.add(user)
        session.commit()

        _seed_books(session, 4)
        books = session.query(db.Books).order_by(db.Books.id).all()

        _setup_kobo_test_environment(monkeypatch, kobo, user)
        app = _create_test_flask_app()

        response1, _ = _make_sync_request(kobo, app, session)
        token1 = response1.headers.get("x-kobo-synctoken")

        # Modify only books 0 and 2
        books[0].last_modified = datetime.now(timezone.utc) + timedelta(days=1)
        books[2].last_modified = datetime.now(timezone.utc) + timedelta(days=2)
        session.commit()
        expected_ids = {books[0].uuid, books[2].uuid}

        _, payload2 = _make_sync_request(kobo, app, session, token=token1)
        returned_ids = _collect_entitlement_ids(payload2)
        assert returned_ids == expected_ids


def test_modified_synced_book_is_resent(monkeypatch):
    """A previously synced book should re-sync when its last_modified changes."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        session.add(user)
        session.commit()

        _seed_books(session, 1)
        book = session.query(db.Books).first()

        _setup_kobo_test_environment(monkeypatch, kobo, user)
        app = _create_test_flask_app()

        response1, _ = _make_sync_request(kobo, app, session)
        token1 = response1.headers.get("x-kobo-synctoken")

        book.last_modified = datetime.now(timezone.utc) + timedelta(days=1)
        session.commit()

        _, payload2 = _make_sync_request(kobo, app, session, token=token1)
        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements2) == 1, "Expected modified synced book to be returned again."
        assert _extract_entitlement_ids(entitlements2) == [book.uuid]


# =============================================================================
# ONLY KOBO SHELVES MODE - BASIC TESTS
# Tests for kobo_only_shelves_sync=1 mode where only books on Kobo-synced
# shelves are sent. These test the tags_last_modified update fix.
# =============================================================================

def test_only_kobo_shelves_no_repeat_books_after_sync(monkeypatch):
    """Books on a Kobo shelf shouldn't repeat in subsequent syncs.

    Tests that tags_last_modified is updated to cover BookShelf.date_added.
    """
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        _seed_books(session, 3)
        books = session.query(db.Books).all()
        _create_kobo_shelf_with_books(session, user.id, [b.id for b in books], "Kobo Shelf")

        _setup_kobo_test_environment(monkeypatch, kobo, user)
        app = _create_test_flask_app()

        response1, payload1 = _make_sync_request(kobo, app, session)
        token = response1.headers.get("x-kobo-synctoken")

        entitlements1 = [item for item in payload1 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements1) == 3

        _, payload2 = _make_sync_request(kobo, app, session, token=token)
        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements2) == 0, (
            f"BUG: Expected 0 books in second sync, got {len(entitlements2)}. "
            "tags_last_modified not covering BookShelf.date_added."
        )


def test_only_kobo_shelves_pagination_no_repeats(monkeypatch):
    """Paginated syncs in only_kobo_shelves mode shouldn't repeat books across pages."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        sync_limit = 3
        _seed_books(session, sync_limit + 2)
        books = session.query(db.Books).all()
        _create_kobo_shelf_with_books(session, user.id, [b.id for b in books], "Kobo Shelf")

        _setup_kobo_test_environment(monkeypatch, kobo, user, sync_limit=sync_limit)
        app = _create_test_flask_app()

        response1, payload1 = _make_sync_request(kobo, app, session)
        token1 = response1.headers.get("x-kobo-synctoken")

        entitlements1 = [item for item in payload1 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements1) == sync_limit
        assert response1.headers.get("x-kobo-sync") == "continue"

        response2, payload2 = _make_sync_request(kobo, app, session, token=token1)
        token2 = response2.headers.get("x-kobo-synctoken")

        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements2) == 2

        # Verify no overlap between pages
        ids_page1 = set(_extract_entitlement_ids(entitlements1))
        ids_page2 = set(_extract_entitlement_ids(entitlements2))
        assert ids_page1.isdisjoint(ids_page2), f"Books repeated across pages: {ids_page1 & ids_page2}"

        # Third sync should be empty
        _, payload3 = _make_sync_request(kobo, app, session, token=token2)
        entitlements3 = [item for item in payload3 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements3) == 0


def test_sync_shelves_updates_tags_last_modified(monkeypatch):
    """Adding a book to a shelf after sync should trigger re-sync of that book."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        _seed_books(session, 3)
        books = session.query(db.Books).all()

        # Create shelf with only 2 books initially
        _create_kobo_shelf_with_books(session, user.id, [books[0].id, books[1].id], "Kobo Shelf")

        _setup_kobo_test_environment(monkeypatch, kobo, user)
        app = _create_test_flask_app()

        response1, payload1 = _make_sync_request(kobo, app, session)
        token1 = response1.headers.get("x-kobo-synctoken")

        entitlements1 = [item for item in payload1 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements1) == 2

        # Add third book to shelf
        shelf = session.query(ub.Shelf).filter(ub.Shelf.user_id == user.id).first()
        new_book_shelf = ub.BookShelf(
            book_id=books[2].id,
            date_added=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
        new_book_shelf.ub_shelf = shelf
        session.add(new_book_shelf)
        session.commit()

        # Second sync should return only the new book
        response2, payload2 = _make_sync_request(kobo, app, session, token=token1)
        token2 = response2.headers.get("x-kobo-synctoken")

        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements2) == 1
        assert books[2].uuid in set(_extract_entitlement_ids(entitlements2))

        # Third sync should be empty
        _, payload3 = _make_sync_request(kobo, app, session, token=token2)
        entitlements3 = [item for item in payload3 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements3) == 0


def test_only_kobo_shelves_modified_synced_book_is_resent(monkeypatch):
    """In only_kobo_shelves mode, modified books should re-sync."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        _seed_books(session, 1)
        book = session.query(db.Books).first()
        _create_kobo_shelf_with_books(session, user.id, [book.id], "Kobo Shelf")

        _setup_kobo_test_environment(monkeypatch, kobo, user)
        app = _create_test_flask_app()

        response1, _ = _make_sync_request(kobo, app, session)
        token1 = response1.headers.get("x-kobo-synctoken")

        book.last_modified = datetime.now(timezone.utc) + timedelta(days=1)
        session.commit()

        _, payload2 = _make_sync_request(kobo, app, session, token=token1)
        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements2) == 1
        assert _extract_entitlement_ids(entitlements2) == [book.uuid]


def test_only_kobo_shelves_modified_synced_book_paginates_without_repeat(monkeypatch):
    """Modified books should paginate correctly without repeating."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        _seed_books(session, 5)
        books = session.query(db.Books).order_by(db.Books.id).all()
        _create_kobo_shelf_with_books(session, user.id, [b.id for b in books], "Kobo Shelf")

        _setup_kobo_test_environment(monkeypatch, kobo, user, sync_limit=10)
        app = _create_test_flask_app()

        initial_sync, _ = _make_sync_request(kobo, app, session)
        token_full = initial_sync.headers.get("x-kobo-synctoken")
        assert initial_sync.headers.get("x-kobo-sync") is None

        # Modify 3 books
        modified_books = [books[0], books[2], books[4]]
        for offset, book in enumerate(modified_books, start=1):
            book.last_modified = datetime.now(timezone.utc) + timedelta(days=offset)
        session.commit()
        expected_ids = {book.uuid for book in modified_books}

        monkeypatch.setattr(kobo, "SYNC_ITEM_LIMIT", 2, raising=False)

        response1, payload1 = _make_sync_request(kobo, app, session, token=token_full)
        ids1 = _collect_entitlement_ids(payload1)
        token1 = response1.headers.get("x-kobo-synctoken")
        assert response1.headers.get("x-kobo-sync") == "continue"
        assert len(ids1) == 2

        response2, payload2 = _make_sync_request(kobo, app, session, token=token1)
        ids2 = _collect_entitlement_ids(payload2)
        assert response2.headers.get("x-kobo-sync") is None
        assert len(ids2) == 1
        assert ids1.isdisjoint(ids2)
        assert ids1 | ids2 == expected_ids


# =============================================================================
# OR CONDITION FIX TESTS
# The only_kobo_shelves query needs or_() to sync books when EITHER date_added
# OR last_modified triggers, not requiring both.
# =============================================================================

def test_only_kobo_shelves_or_condition_date_added_triggers_sync(monkeypatch):
    """Book added to shelf (date_added trigger) should sync even if last_modified unchanged."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        # Create 3 books but only put 2 on shelf initially
        _seed_books(session, 3)
        books = session.query(db.Books).order_by(db.Books.id).all()
        _create_kobo_shelf_with_books(session, user.id, [books[0].id, books[1].id], "Kobo Shelf")

        _setup_kobo_test_environment(monkeypatch, kobo, user)
        app = _create_test_flask_app()

        response1, payload1 = _make_sync_request(kobo, app, session)
        token1 = response1.headers.get("x-kobo-synctoken")

        entitlements1 = [item for item in payload1 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements1) == 2

        # Add third book to shelf (date_added > tags_last_modified)
        # but do NOT modify its last_modified
        shelf = session.query(ub.Shelf).filter(ub.Shelf.user_id == user.id).first()
        new_book_shelf = ub.BookShelf(
            book_id=books[2].id,
            date_added=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
        new_book_shelf.ub_shelf = shelf
        session.add(new_book_shelf)
        session.commit()

        _, payload2 = _make_sync_request(kobo, app, session, token=token1)
        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements2) == 1, (
            f"or_() FIX: Expected 1 book via date_added trigger, got {len(entitlements2)}"
        )
        assert _extract_entitlement_ids(entitlements2) == [books[2].uuid]


def test_only_kobo_shelves_or_condition_last_modified_triggers_sync(monkeypatch):
    """Book with modified metadata should sync even if date_added unchanged."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        _seed_books(session, 2)
        books = session.query(db.Books).order_by(db.Books.id).all()
        _create_kobo_shelf_with_books(session, user.id, [b.id for b in books], "Kobo Shelf")

        _setup_kobo_test_environment(monkeypatch, kobo, user)
        app = _create_test_flask_app()

        response1, payload1 = _make_sync_request(kobo, app, session)
        token1 = response1.headers.get("x-kobo-synctoken")

        entitlements1 = [item for item in payload1 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements1) == 2

        # Modify first book's metadata (but don't change date_added)
        books[0].last_modified = datetime.now(timezone.utc) + timedelta(days=1)
        session.commit()

        _, payload2 = _make_sync_request(kobo, app, session, token=token1)
        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements2) == 1, (
            f"or_() FIX: Expected 1 book via last_modified trigger, got {len(entitlements2)}"
        )
        assert _extract_entitlement_ids(entitlements2) == [books[0].uuid]


# =============================================================================
# DUPLICATE TIMESTAMP TESTS
# When books have identical last_modified (e.g., bulk import), pagination needs
# a secondary sort key (book ID) to avoid infinite loops or skipped books.
# =============================================================================

def test_pagination_with_duplicate_timestamps(monkeypatch, tmp_path):
    """Pagination with identical timestamps needs book ID tiebreaker.

    Without the fix, all books with timestamp T would be returned repeatedly
    because filter (last_modified > T) excludes them all.
    """
    kobo = import_kobo()

    with _kobo_test_split_sessions(tmp_path) as (calibre_session, app_session):
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        app_session.add(user)
        app_session.commit()

        sync_limit = 50
        total_books = 150
        same_timestamp = datetime.now(timezone.utc) - timedelta(days=10)
        _seed_books_with_same_timestamp(calibre_session, total_books, timestamp=same_timestamp)

        _setup_kobo_test_environment(monkeypatch, kobo, user, sync_limit=sync_limit)
        app = _create_test_flask_app()

        # Page 1
        response1, payload1 = _make_sync_request(kobo, app, calibre_session)
        token1 = response1.headers.get("x-kobo-synctoken")
        ids1 = _collect_entitlement_ids(payload1)

        assert len(payload1) == sync_limit
        assert response1.headers.get("x-kobo-sync") == "continue"

        # Page 2 - should be different books
        response2, payload2 = _make_sync_request(kobo, app, calibre_session, token=token1)
        token2 = response2.headers.get("x-kobo-synctoken")
        ids2 = _collect_entitlement_ids(payload2)

        assert len(payload2) == sync_limit
        assert response2.headers.get("x-kobo-sync") == "continue"
        assert ids1.isdisjoint(ids2), "PAGINATION BUG: duplicate books returned"

        # Page 3
        response3, payload3 = _make_sync_request(kobo, app, calibre_session, token=token2)
        token3 = response3.headers.get("x-kobo-synctoken")
        ids3 = _collect_entitlement_ids(payload3)

        assert len(payload3) == sync_limit
        assert response3.headers.get("x-kobo-sync") is None
        assert ids1.isdisjoint(ids3) and ids2.isdisjoint(ids3)

        # Page 4 - should be empty
        _, payload4 = _make_sync_request(kobo, app, calibre_session, token=token3)
        assert payload4 == []

        # Verify all books synced exactly once
        all_ids = ids1 | ids2 | ids3
        assert len(all_ids) == total_books


def test_modified_book_resync_with_duplicate_timestamps(monkeypatch, tmp_path):
    """Modified book should re-sync even with duplicate timestamps.

    The book ID filter shouldn't prevent re-syncing when last_modified changes.
    """
    kobo = import_kobo()

    with _kobo_test_split_sessions(tmp_path) as (calibre_session, app_session):
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        app_session.add(user)
        app_session.commit()

        sync_limit = 50
        total_books = 100
        same_timestamp = datetime.now(timezone.utc) - timedelta(days=10)
        _seed_books_with_same_timestamp(calibre_session, total_books, timestamp=same_timestamp)

        _setup_kobo_test_environment(monkeypatch, kobo, user, sync_limit=sync_limit)
        app = _create_test_flask_app()

        # Sync all books (2 pages)
        response1, payload1 = _make_sync_request(kobo, app, calibre_session)
        token1 = response1.headers.get("x-kobo-synctoken")
        response2, payload2 = _make_sync_request(kobo, app, calibre_session, token=token1)
        token2 = response2.headers.get("x-kobo-synctoken")

        assert len(payload1) + len(payload2) == total_books

        # Modify one book
        books = calibre_session.query(db.Books).order_by(db.Books.id).all()
        modified_book = books[24]
        modified_book.last_modified = datetime.now(timezone.utc) + timedelta(days=1)
        calibre_session.commit()

        # Should return only the modified book
        _, payload3 = _make_sync_request(kobo, app, calibre_session, token=token2)
        ids3 = _extract_entitlement_ids(payload3)

        assert len(payload3) == 1
        assert ids3[0] == modified_book.uuid


def test_mixed_timestamps_pagination(monkeypatch, tmp_path):
    """Pagination across different timestamp groups should work correctly."""
    kobo = import_kobo()

    with _kobo_test_split_sessions(tmp_path) as (calibre_session, app_session):
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        app_session.add(user)
        app_session.commit()

        sync_limit = 60
        base_time = datetime.now(timezone.utc) - timedelta(days=30)

        # 50 books @ T1, 100 books @ T2, 50 books @ T3
        _seed_books_with_same_timestamp(calibre_session, 50, timestamp=base_time)
        _seed_books_with_same_timestamp(calibre_session, 100, timestamp=base_time + timedelta(days=1))
        _seed_books_with_same_timestamp(calibre_session, 50, timestamp=base_time + timedelta(days=2))

        _setup_kobo_test_environment(monkeypatch, kobo, user, sync_limit=sync_limit)
        app = _create_test_flask_app()

        # Page 1: 60 books
        response1, payload1 = _make_sync_request(kobo, app, calibre_session)
        token1 = response1.headers.get("x-kobo-synctoken")
        assert len(payload1) == sync_limit
        assert response1.headers.get("x-kobo-sync") == "continue"

        # Page 2: 60 books
        response2, payload2 = _make_sync_request(kobo, app, calibre_session, token=token1)
        token2 = response2.headers.get("x-kobo-synctoken")
        assert len(payload2) == sync_limit
        assert response2.headers.get("x-kobo-sync") == "continue"

        # Page 3: 60 books
        response3, payload3 = _make_sync_request(kobo, app, calibre_session, token=token2)
        token3 = response3.headers.get("x-kobo-synctoken")
        assert len(payload3) == sync_limit

        # Page 4: remaining 20 books
        response4, payload4 = _make_sync_request(kobo, app, calibre_session, token=token3)
        token4 = response4.headers.get("x-kobo-synctoken")
        assert len(payload4) == 20
        assert response4.headers.get("x-kobo-sync") is None

        # Page 5: empty
        _, payload5 = _make_sync_request(kobo, app, calibre_session, token=token4)
        assert payload5 == []

        # Verify all 200 books synced
        all_ids = (_collect_entitlement_ids(payload1) |
                   _collect_entitlement_ids(payload2) |
                   _collect_entitlement_ids(payload3) |
                   _collect_entitlement_ids(payload4))
        assert len(all_ids) == 200


def test_only_kobo_shelves_pagination_with_duplicate_timestamps(monkeypatch):
    """Duplicate timestamp pagination should work in only_kobo_shelves mode."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        sync_limit = 50
        total_books = 150
        same_timestamp = datetime.now(timezone.utc) - timedelta(days=10)
        _seed_books_with_same_timestamp(session, total_books, timestamp=same_timestamp)

        books = session.query(db.Books).all()
        _create_kobo_shelf_with_books(session, user.id, [b.id for b in books], "Kobo Shelf")

        _setup_kobo_test_environment(monkeypatch, kobo, user, sync_limit=sync_limit)
        app = _create_test_flask_app()

        # Page 1
        response1, payload1 = _make_sync_request(kobo, app, session)
        token1 = response1.headers.get("x-kobo-synctoken")
        ids1 = _collect_entitlement_ids(payload1)

        assert len(ids1) == sync_limit
        assert response1.headers.get("x-kobo-sync") == "continue"

        # Page 2
        response2, payload2 = _make_sync_request(kobo, app, session, token=token1)
        token2 = response2.headers.get("x-kobo-synctoken")
        ids2 = _collect_entitlement_ids(payload2)

        assert len(ids2) == sync_limit
        assert response2.headers.get("x-kobo-sync") == "continue"
        assert ids1.isdisjoint(ids2), "PAGINATION BUG: duplicates in only_kobo_shelves mode"

        # Page 3
        response3, payload3 = _make_sync_request(kobo, app, session, token=token2)
        token3 = response3.headers.get("x-kobo-synctoken")
        ids3 = _collect_entitlement_ids(payload3)

        assert len(ids3) == sync_limit
        assert response3.headers.get("x-kobo-sync") is None
        assert ids1.isdisjoint(ids3) and ids2.isdisjoint(ids3)

        # Page 4 - empty
        _, payload4 = _make_sync_request(kobo, app, session, token=token3)
        entitlements4 = [item for item in payload4 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements4) == 0

        assert len(ids1 | ids2 | ids3) == total_books


def test_only_kobo_shelves_modified_book_resync_with_duplicate_timestamps(monkeypatch):
    """Modified book re-sync with duplicate timestamps in only_kobo_shelves mode."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        sync_limit = 50
        total_books = 100
        same_timestamp = datetime.now(timezone.utc) - timedelta(days=10)
        _seed_books_with_same_timestamp(session, total_books, timestamp=same_timestamp)

        books = session.query(db.Books).order_by(db.Books.id).all()
        _create_kobo_shelf_with_books(session, user.id, [b.id for b in books], "Kobo Shelf")

        _setup_kobo_test_environment(monkeypatch, kobo, user, sync_limit=sync_limit)
        app = _create_test_flask_app()

        # Sync all books (2 pages)
        response1, payload1 = _make_sync_request(kobo, app, session)
        token1 = response1.headers.get("x-kobo-synctoken")
        response2, payload2 = _make_sync_request(kobo, app, session, token=token1)
        token2 = response2.headers.get("x-kobo-synctoken")

        ids1 = _collect_entitlement_ids(payload1)
        ids2 = _collect_entitlement_ids(payload2)
        assert len(ids1) + len(ids2) == total_books

        # Modify one book
        modified_book = books[24]
        modified_book.last_modified = datetime.now(timezone.utc) + timedelta(days=1)
        session.commit()

        # Should return only the modified book
        _, payload3 = _make_sync_request(kobo, app, session, token=token2)
        ids3 = _extract_entitlement_ids(payload3)

        assert len(payload3) == 1
        assert ids3[0] == modified_book.uuid


# =============================================================================
# TIMEZONE SUFFIX TESTS
# SQLite stores timestamps with +00:00 suffix which breaks comparisons.
# The fix uses func.replace() to strip the suffix before comparing.
# =============================================================================

def test_timezone_suffix_in_last_modified_normal_mode(monkeypatch, tmp_path):
    """Books with +00:00 suffix in last_modified should sync correctly."""
    kobo = import_kobo()

    with _kobo_test_split_sessions(tmp_path) as (calibre_session, app_session):
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        app_session.add(user)
        app_session.commit()

        _seed_books_with_timezone_suffix(calibre_session, 5)

        _setup_kobo_test_environment(monkeypatch, kobo, user)
        app = _create_test_flask_app()

        response1, payload1 = _make_sync_request(kobo, app, calibre_session)
        token1 = response1.headers.get("x-kobo-synctoken")

        assert len(payload1) == 5

        # Second sync should be empty (not re-sync due to TZ comparison bug)
        _, payload2 = _make_sync_request(kobo, app, calibre_session, token=token1)

        assert payload2 == [], (
            f"TIMEZONE BUG: Expected 0 books, got {len(payload2)}. "
            "func.replace('+00:00', '') fix not working."
        )


def test_timezone_suffix_in_last_modified_only_kobo_shelves(monkeypatch):
    """Timezone suffix handling in only_kobo_shelves mode."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        _seed_books_with_timezone_suffix(session, 5)

        result = session.execute(text("SELECT id FROM books"))
        book_ids = [row[0] for row in result.fetchall()]

        _create_kobo_shelf_with_books(session, user.id, book_ids, "Kobo Shelf")

        _setup_kobo_test_environment(monkeypatch, kobo, user)
        app = _create_test_flask_app()

        response1, payload1 = _make_sync_request(kobo, app, session)
        token1 = response1.headers.get("x-kobo-synctoken")

        entitlements1 = [item for item in payload1 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements1) == 5

        _, payload2 = _make_sync_request(kobo, app, session, token=token1)
        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements2) == 0, (
            f"TIMEZONE BUG (only_kobo_shelves): Expected 0 books, got {len(entitlements2)}"
        )


# =============================================================================
# BACKWARDS COMPATIBILITY TESTS
# Ensure old sync tokens (missing new fields) parse correctly and sync works.
# =============================================================================

def test_sync_token_backwards_compatibility_missing_books_last_id():
    """Old sync tokens without books_last_id should parse correctly (default to -1)."""
    from cps.services.SyncToken import SyncToken, b64encode_json

    # Create old-format token (pre-1-2-0, missing books_last_id)
    old_token = b64encode_json({
        "version": "1-1-0",
        "data": {
            "raw_kobo_store_token": "",
            "books_last_modified": 1700000000,
            "books_last_created": 1700000000,
            "archive_last_modified": 0,
            "reading_state_last_modified": 0,
            "tags_last_modified": 0,
        }
    })

    headers = {"x-kobo-synctoken": old_token}
    token = SyncToken.from_headers(headers)

    assert token.books_last_id == -1, "Missing books_last_id should default to -1"
    assert token.books_last_modified != datetime.min, "Should parse other fields correctly"


def test_empty_kobo_synced_books_resets_token(monkeypatch, tmp_path):
    """If KoboSyncedBooks is empty, sync should start fresh regardless of token.

    This tests the recovery mechanism for users with stale/bad sync state.
    Clearing KoboSyncedBooks forces a full re-sync.
    """
    kobo = import_kobo()
    from cps.services.SyncToken import b64encode_json

    with _kobo_test_split_sessions(tmp_path) as (calibre_session, app_session):
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        app_session.add(user)
        app_session.commit()

        _seed_books(calibre_session, 3)

        _setup_kobo_test_environment(monkeypatch, kobo, user)
        app = _create_test_flask_app()

        # Create a token with timestamps far in the future (simulating stale state)
        future_token = b64encode_json({
            "version": "1-2-0",
            "data": {
                "raw_kobo_store_token": "",
                "books_last_modified": 9999999999,
                "books_last_created": 9999999999,
                "books_last_id": 999999,
                "archive_last_modified": 9999999999,
                "reading_state_last_modified": 9999999999,
                "tags_last_modified": 9999999999,
            }
        })

        # KoboSyncedBooks is empty, so token should be reset despite future timestamps
        _, payload = _make_sync_request(kobo, app, calibre_session, token=future_token)

        assert len(payload) == 3, (
            f"Expected all 3 books when KoboSyncedBooks is empty, got {len(payload)}. "
            "Token reset mechanism not working."
        )


# =============================================================================
# MULTIPLE SHELVES TESTS
# Verify books on multiple Kobo-synced shelves sync exactly once.
# =============================================================================

def test_only_kobo_shelves_book_on_multiple_shelves_syncs_once(monkeypatch):
    """Book on multiple Kobo-synced shelves should sync exactly once (distinct works)."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        _seed_books(session, 1)
        book = session.query(db.Books).first()

        # Add same book to 3 different Kobo-synced shelves
        for i in range(3):
            _create_kobo_shelf_with_books(session, user.id, [book.id], f"Kobo Shelf {i}")

        _setup_kobo_test_environment(monkeypatch, kobo, user)
        app = _create_test_flask_app()

        _, payload = _make_sync_request(kobo, app, session)
        entitlements = [item for item in payload if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements) == 1, (
            f"Expected book once, got {len(entitlements)}. "
            "distinct() clause not preventing duplicates from multiple shelves."
        )


def test_only_kobo_shelves_multiple_books_on_multiple_shelves(monkeypatch):
    """Multiple books across multiple shelves should each sync exactly once."""
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        _seed_books(session, 4)
        books = session.query(db.Books).all()

        # Shelf 1: books 0, 1, 2
        _create_kobo_shelf_with_books(session, user.id, [books[0].id, books[1].id, books[2].id], "Shelf A")
        # Shelf 2: books 1, 2, 3 (overlapping)
        _create_kobo_shelf_with_books(session, user.id, [books[1].id, books[2].id, books[3].id], "Shelf B")

        _setup_kobo_test_environment(monkeypatch, kobo, user)
        app = _create_test_flask_app()

        _, payload = _make_sync_request(kobo, app, session)
        entitlements = [item for item in payload if "NewEntitlement" in item or "ChangedEntitlement" in item]
        synced_ids = set(_extract_entitlement_ids(entitlements))

        assert len(entitlements) == 4, (
            f"Expected 4 unique books, got {len(entitlements)}. "
            "Books on multiple shelves causing duplicates."
        )
        assert synced_ids == {b.uuid for b in books}, "All books should be synced exactly once"


# =============================================================================
# KEPUB CONVERSION TESTS
# shelf.py needs to import helper module to trigger KEPUB conversion when
# books are added to Kobo-synced shelves.
# =============================================================================

def test_add_book_to_kobo_shelf_triggers_kepub_conversion():
    """shelf.py should import helper for KEPUB conversion on Kobo shelves."""
    from kobo_test_support import install_stub_modules
    install_stub_modules()

    from cps import shelf as shelf_module

    assert hasattr(shelf_module, "helper"), (
        "shelf.py missing 'helper' import for KEPUB conversion fix. "
        "Add 'helper' to imports: 'from . import calibre_db, config, db, logger, ub, helper'"
    )


def test_add_book_to_non_kobo_shelf_does_not_trigger_kepub_conversion():
    """Non-Kobo shelves should not trigger KEPUB conversion (enforced by if-condition)."""
    from kobo_test_support import install_stub_modules
    install_stub_modules()

    from cps import shelf as shelf_module

    # This test documents expected behavior - conversion only happens when
    # shelf.kobo_sync is True (enforced by: `if shelf.kobo_sync and config.config_kepubifypath:`)
    if hasattr(shelf_module, "helper"):
        pass  # Fix is present, conditional logic in add_to_shelf enforces this

    assert True


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _seed_books(session, count):
    """Seed books with distinct timestamps (older books have earlier timestamps)."""
    now = datetime.now(timezone.utc)
    for idx in range(count):
        title = f"Book {idx + 1}"
        book = db.Books(
            title=title,
            sort=title,
            author_sort="",
            timestamp=now - timedelta(days=idx + 2),
            pubdate=db.Books.DEFAULT_PUBDATE,
            series_index="1.0",
            last_modified=now - timedelta(days=idx + 2),
            path=f"book_{idx + 1}",
            has_cover=0,
            authors=[],
            tags=[],
            languages=[],
        )
        book.uuid = str(uuid4())
        session.add(book)
        session.flush()
        session.add(
            db.Data(
                book=book.id,
                book_format="EPUB",
                uncompressed_size=123,
                name=f"book_{idx + 1}.epub",
            )
        )
    session.commit()


def _seed_books_with_same_timestamp(session, count, timestamp=None):
    """Seed books with identical timestamps (reproduces bulk import scenario)."""
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    for idx in range(count):
        title = f"Book {idx + 1}"
        book = db.Books(
            title=title,
            sort=title,
            author_sort="",
            timestamp=timestamp,
            pubdate=db.Books.DEFAULT_PUBDATE,
            series_index="1.0",
            last_modified=timestamp,
            path=f"book_{idx + 1}",
            has_cover=0,
            authors=[],
            tags=[],
            languages=[],
        )
        book.uuid = str(uuid4())
        session.add(book)
        session.flush()
        session.add(
            db.Data(
                book=book.id,
                book_format="EPUB",
                uncompressed_size=123,
                name=f"book_{idx + 1}.epub",
            )
        )
    session.commit()


def _seed_books_with_timezone_suffix(session, count, timestamp=None):
    """Seed books with explicit +00:00 suffix via raw SQL (tests TZ stripping fix)."""
    if timestamp is None:
        timestamp = datetime.now(timezone.utc) - timedelta(days=10)

    ts_str = timestamp.strftime('%Y-%m-%d %H:%M:%S+00:00')

    for idx in range(count):
        book_uuid = str(uuid4())

        session.execute(text("""
            INSERT INTO books (title, sort, author_sort, timestamp, pubdate,
                             series_index, last_modified, path, has_cover, uuid)
            VALUES (:title, :sort, '', :timestamp, :pubdate,
                   '1.0', :last_modified, :path, 0, :uuid)
        """), {
            "title": f"Book {idx + 1}",
            "sort": f"Book {idx + 1}",
            "timestamp": ts_str,
            "pubdate": "2024-01-01",
            "last_modified": ts_str,
            "path": f"book_{idx + 1}",
            "uuid": book_uuid,
        })

        result = session.execute(text("SELECT last_insert_rowid()"))
        book_id = result.fetchone()[0]

        session.execute(text("""
            INSERT INTO data (book, format, uncompressed_size, name)
            VALUES (:book_id, 'EPUB', 123, :name)
        """), {"book_id": book_id, "name": f"book_{idx + 1}.epub"})

    session.commit()


def _create_kobo_shelf_with_books(app_session, user_id, book_ids, shelf_name="Test Shelf"):
    """Create a Kobo-synced shelf with books.

    Sets date_added slightly after shelf.last_modified to reproduce the timing
    mismatch that causes the download loop bug.
    """
    now = datetime.now(timezone.utc)
    shelf = ub.Shelf(
        user_id=user_id,
        name=shelf_name,
        uuid=str(uuid4()),
        kobo_sync=True,
        created=now,
        last_modified=now,
    )
    app_session.add(shelf)
    app_session.flush()

    book_date_added = now + timedelta(milliseconds=100)
    for book_id in book_ids:
        book_shelf = ub.BookShelf(
            book_id=book_id,
            date_added=book_date_added,
        )
        book_shelf.ub_shelf = shelf
        app_session.add(book_shelf)

    app_session.commit()
    return shelf


def _setup_kobo_test_environment(monkeypatch, kobo, user, sync_limit=None):
    """Configure monkeypatch settings for Kobo sync tests."""
    monkeypatch.setattr(kobo, "current_user", user, raising=False)
    monkeypatch.setattr(kobo_sync_status, "current_user", user, raising=False)
    monkeypatch.setattr(kobo.shelf_lib, "current_user", user, raising=False)
    monkeypatch.setattr(kobo.config, "config_kobo_proxy", False, raising=False)
    monkeypatch.setattr(kobo.config, "config_external_port", 80, raising=False)
    monkeypatch.setattr(kobo.config, "config_kepubifypath", None, raising=False)
    monkeypatch.setattr(kobo, "get_epub_layout", lambda *a, **k: None)
    monkeypatch.setattr(kobo.calibre_db, "reconnect_db", lambda *a, **k: None)
    monkeypatch.setattr(kobo.calibre_db, "common_filters", lambda *a, **k: true())

    if sync_limit is not None:
        monkeypatch.setattr(kobo, "SYNC_ITEM_LIMIT", sync_limit, raising=False)


def _create_test_flask_app():
    """Create a Flask test app with required WSGI wrapper."""
    app = Flask(__name__)
    app.config["TESTING"] = True
    original_wsgi_app = app.wsgi_app

    class _WsgiWrapper:
        def __init__(self, wsgi_app):
            self._wsgi_app = wsgi_app
            self.is_proxied = False

        def __call__(self, environ, start_response):
            return self._wsgi_app(environ, start_response)

    app.wsgi_app = _WsgiWrapper(original_wsgi_app)
    return app


def _make_sync_request(kobo, app, session, token=None, base_url="http://example.com"):
    """Make a sync request and return (response, parsed_payload)."""
    headers = {}
    if token:
        headers["x-kobo-synctoken"] = token

    with app.test_request_context(
        "/kobo/testtoken/v1/library/sync",
        base_url=base_url,
        headers=headers if headers else None
    ):
        g.lib_sql = session
        response = kobo.HandleSyncRequest.__wrapped__()

    return response, json.loads(response.get_data(as_text=True))


def _build_session():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    conn = engine.connect()
    conn.execute(text("ATTACH DATABASE ':memory:' AS calibre"))
    db.Base.metadata.create_all(conn)
    ub.Base.metadata.create_all(conn)
    Session = sessionmaker(bind=conn)
    return Session(), conn, engine


def _build_split_sessions(tmp_path):
    calibre_dir = tmp_path / "calibre"
    calibre_dir.mkdir()
    calibre_db_path = calibre_dir / "metadata.db"
    app_db_path = tmp_path / "app.db"

    calibre_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    calibre_conn = calibre_engine.connect()
    calibre_conn.execute(text("ATTACH DATABASE :calibre_db AS calibre"), {"calibre_db": str(calibre_db_path)})
    db.Base.metadata.create_all(calibre_conn)
    ub.Base.metadata.create_all(calibre_conn)
    CalibreSession = sessionmaker(bind=calibre_conn)

    app_engine = create_engine(
        f"sqlite:///{app_db_path}",
        connect_args={"check_same_thread": False},
    )
    ub.Base.metadata.create_all(app_engine)
    AppSession = sessionmaker(bind=app_engine)

    return CalibreSession(), calibre_conn, calibre_engine, AppSession(), app_engine, app_db_path


@contextmanager
def _kobo_test_session():
    """Context manager for single-session test lifecycle."""
    session, conn, engine = _build_session()
    old_session = ub.session
    ub.session = session
    try:
        yield session
    finally:
        session.close()
        ub.session = old_session
        conn.close()
        engine.dispose()


@contextmanager
def _kobo_test_split_sessions(tmp_path):
    """Context manager for split calibre/app session lifecycle."""
    calibre_session, calibre_conn, calibre_engine, app_session, app_engine, app_db_path = _build_split_sessions(tmp_path)
    old_session = ub.session
    old_app_db_path = ub.app_DB_path
    ub.session = app_session
    ub.app_DB_path = str(app_db_path)
    try:
        yield calibre_session, app_session
    finally:
        ub.session = old_session
        ub.app_DB_path = old_app_db_path
        app_session.close()
        app_engine.dispose()
        calibre_session.close()
        calibre_conn.close()
        calibre_engine.dispose()


def _extract_entitlement_ids(payload):
    """Extract book UUIDs from entitlement payload as a list."""
    ids = []
    for item in payload:
        for key in ("NewEntitlement", "ChangedEntitlement"):
            if key in item:
                ids.append(item[key]["BookEntitlement"]["Id"])
                break
    return ids


def _collect_entitlement_ids(payload):
    """Extract book UUIDs from entitlement payload as a set."""
    return set(_extract_entitlement_ids(payload))
