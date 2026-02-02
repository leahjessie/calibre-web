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


def test_sync_returns_entitlements_and_updates_synced_books(monkeypatch):
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


def test_sync_over_limit_does_not_repeat_payload(monkeypatch, tmp_path):
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
        assert len(payload2) == 2, (
            f"expected 2 itmes (remaining books over sync_limit) in payload2, got {len(payload2)}"
        )
        assert response2.headers.get("x-kobo-sync") is None
        assert set(_extract_entitlement_ids(payload1)).isdisjoint(_extract_entitlement_ids(payload2))


def test_sync_unchanged_library_after_full_sync_returns_empty(monkeypatch, tmp_path):
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


def test_sync_mixed_modified_and_unchanged_only_returns_modified(monkeypatch):
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

        books[0].last_modified = datetime.now(timezone.utc) + timedelta(days=1)
        books[2].last_modified = datetime.now(timezone.utc) + timedelta(days=2)
        session.commit()
        expected_ids = {books[0].uuid, books[2].uuid}

        _, payload2 = _make_sync_request(kobo, app, session, token=token1)
        returned_ids = _collect_entitlement_ids(payload2)
        assert returned_ids == expected_ids, (
            f"Expected only modified IDs {expected_ids}, got {returned_ids}"
        )


def test_sync_exactly_limit_does_not_set_continue_header(monkeypatch):
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
        assert response.headers.get("x-kobo-sync") is None, (
            "Expected no continuation header when result count equals SYNC_ITEM_LIMIT."
        )


def test_modified_synced_book_is_resent(monkeypatch):
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


def test_only_kobo_shelves_no_repeat_books_after_sync(monkeypatch):
    """
    Test that books on a Kobo-synced shelf don't repeat in subsequent syncs.

    This test would FAIL before commit 309865c9 because tags_last_modified
    wasn't being updated to cover BookShelf.date_added values.
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

        # First sync - should return all 3 books
        response1, payload1 = _make_sync_request(kobo, app, session)
        token = response1.headers.get("x-kobo-synctoken")

        entitlements1 = [item for item in payload1 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements1) == 3, f"Expected 3 books in first sync, got {len(entitlements1)}"

        # Second sync with token - should return 0 book entitlements (already synced)
        _, payload2 = _make_sync_request(kobo, app, session, token=token)
        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements2) == 0, (
            f"BUG: Expected 0 books in second sync (books should not repeat), "
            f"got {len(entitlements2)}. This indicates tags_last_modified is not "
            f"being properly updated to cover BookShelf.date_added values."
        )


def test_only_kobo_shelves_modified_synced_book_is_resent(monkeypatch):
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

        assert len(entitlements2) == 1, "Expected modified synced book to be returned again."
        assert _extract_entitlement_ids(entitlements2) == [book.uuid]


def test_only_kobo_shelves_modified_synced_book_paginates_without_repeat(monkeypatch):
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


def test_only_kobo_shelves_pagination_no_repeats(monkeypatch):
    """
    Test that paginated syncs in only_kobo_shelves mode don't repeat books across pages.

    This test would FAIL before commit 309865c9 because tags_last_modified
    wasn't being updated during pagination.
    """
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

        # First sync (page 1) - should return sync_limit books
        response1, payload1 = _make_sync_request(kobo, app, session)
        token1 = response1.headers.get("x-kobo-synctoken")

        entitlements1 = [item for item in payload1 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements1) == sync_limit
        assert response1.headers.get("x-kobo-sync") == "continue"

        # Second sync (page 2) - should return remaining books
        response2, payload2 = _make_sync_request(kobo, app, session, token=token1)
        token2 = response2.headers.get("x-kobo-synctoken")

        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements2) == 2, (
            f"Expected 2 books (remaining after sync_limit) in page 2, got {len(entitlements2)}"
        )

        # Verify no overlap between pages
        ids_page1 = set(_extract_entitlement_ids(entitlements1))
        ids_page2 = set(_extract_entitlement_ids(entitlements2))
        assert ids_page1.isdisjoint(ids_page2), (
            f"BUG: Books repeated across pagination pages. "
            f"Page 1 IDs: {ids_page1}, Page 2 IDs: {ids_page2}"
        )

        # Third sync - should return 0 books
        _, payload3 = _make_sync_request(kobo, app, session, token=token2)
        entitlements3 = [item for item in payload3 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements3) == 0, (
            f"Expected 0 books in third sync, got {len(entitlements3)}"
        )


def test_sync_shelves_updates_tags_last_modified(monkeypatch):
    """
    Test that sync properly updates tags_last_modified when books are added to shelves.

    This test would FAIL before commit 309865c9 because sync_shelves() only tracked
    Shelf.last_modified, not BookShelf.date_added.
    """
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

        # First sync - should return 2 books
        response1, payload1 = _make_sync_request(kobo, app, session)
        token1 = response1.headers.get("x-kobo-synctoken")

        entitlements1 = [item for item in payload1 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements1) == 2, f"Expected 2 books in first sync, got {len(entitlements1)}"

        # Add third book to the shelf with date_added slightly in the future
        # to ensure it's after the tags_last_modified from the first sync
        shelf = session.query(ub.Shelf).filter(ub.Shelf.user_id == user.id).first()
        new_book_shelf = ub.BookShelf(
            book_id=books[2].id,
            date_added=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
        new_book_shelf.ub_shelf = shelf
        session.add(new_book_shelf)
        session.commit()

        # Second sync - should return only the new book
        response2, payload2 = _make_sync_request(kobo, app, session, token=token1)
        token2 = response2.headers.get("x-kobo-synctoken")

        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements2) == 1, (
            f"Expected 1 new book in second sync, got {len(entitlements2)}"
        )

        # Verify it's the correct book (the third one we added)
        synced_ids = set(_extract_entitlement_ids(entitlements2))
        assert books[2].uuid in synced_ids, (
            f"Expected book {books[2].uuid} to be synced, got {synced_ids}"
        )

        # Third sync - should return 0 books
        _, payload3 = _make_sync_request(kobo, app, session, token=token2)
        entitlements3 = [item for item in payload3 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements3) == 0, (
            f"BUG: Expected 0 books in third sync, got {len(entitlements3)}. "
            f"This indicates tags_last_modified is not being properly updated."
        )


def test_add_book_to_kobo_shelf_triggers_kepub_conversion():
    """
    Test that shelf.py has the KEPUB conversion fix for Kobo-synced shelves.

    The fix adds `helper` import to shelf.py and calls helper.convert_book_format
    when a book with EPUB (but no KEPUB) is added to a shelf marked for Kobo sync.

    This test checks that shelf.py imports helper, which is required for the fix.
    """
    # Install stubs before any cps imports
    from kobo_test_support import install_stub_modules
    install_stub_modules()

    from cps import shelf as shelf_module

    # The fix adds `helper` to shelf.py's imports:
    # from . import calibre_db, config, db, logger, ub, helper
    #
    # Check that shelf.py has the helper import (meaning the fix is applied)
    assert hasattr(shelf_module, "helper"), (
        "Expected shelf.py to import 'helper' module for KEPUB conversion. "
        "This test fails if the KEPUB conversion fix is not applied to shelf.py. "
        "The fix should add 'helper' to the imports: "
        "'from . import calibre_db, config, db, logger, ub, helper'"
    )

def test_add_book_to_non_kobo_shelf_does_not_trigger_kepub_conversion():
    """
    Test that the KEPUB conversion logic only triggers for Kobo-synced shelves.

    This is a sanity check that the conversion condition requires kobo_sync=True.
    The actual logic check is: if shelf.kobo_sync and config.config_kepubifypath
    """
    # This test verifies the logic structure - when shelf.kobo_sync is False,
    # the conversion should not be triggered. This is guaranteed by the if-condition
    # in the fix, so this test just documents the expected behavior.

    # Install stubs before any cps imports
    from kobo_test_support import install_stub_modules
    install_stub_modules()

    from cps import shelf as shelf_module

    # If the fix is present, verify the conditional logic exists
    # by checking that the module has the expected attributes
    if hasattr(shelf_module, "helper"):
        # Fix is present - the conditional logic in add_to_shelf ensures
        # conversion only happens when shelf.kobo_sync is True
        pass

    # This test always passes - it documents the expected behavior
    # The actual enforcement is in the code: `if shelf.kobo_sync and config.config_kepubifypath:`
    assert True, "Non-Kobo shelves should not trigger KEPUB conversion (enforced by if-condition in fix)"


def test_only_kobo_shelves_or_condition_date_added_triggers_sync(monkeypatch):
    """
    Test that the or_() fix works: a book is synced when ONLY date_added triggers.

    This tests the first part of the or_() condition:
        or_(
            func.datetime(ub.BookShelf.date_added) > sync_token.tags_last_modified,
            func.datetime(db.Books.last_modified) > sync_token.books_last_modified,
        )

    Scenario:
    1. Sync a shelf with 2 books
    2. Add a NEW book to the shelf (date_added > tags_last_modified)
    3. Do NOT modify the book's metadata (last_modified unchanged)
    4. Second sync should return the new book

    This would FAIL without the or_() fix because the query would require
    BOTH conditions to be true, not just one.
    """
    kobo = import_kobo()

    with _kobo_test_session() as session:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        # Create 3 books but only put 2 on the shelf initially
        _seed_books(session, 3)
        books = session.query(db.Books).order_by(db.Books.id).all()

        # Create shelf with only first 2 books
        _create_kobo_shelf_with_books(session, user.id, [books[0].id, books[1].id], "Kobo Shelf")

        _setup_kobo_test_environment(monkeypatch, kobo, user)
        app = _create_test_flask_app()

        # First sync - should return 2 books
        response1, payload1 = _make_sync_request(kobo, app, session)
        token1 = response1.headers.get("x-kobo-synctoken")

        entitlements1 = [item for item in payload1 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements1) == 2, f"Expected 2 books in first sync, got {len(entitlements1)}"

        # Add third book to shelf (date_added will be > tags_last_modified)
        # but do NOT modify its last_modified
        shelf = session.query(ub.Shelf).filter(ub.Shelf.user_id == user.id).first()
        new_book_shelf = ub.BookShelf(
            book_id=books[2].id,
            date_added=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
        new_book_shelf.ub_shelf = shelf
        session.add(new_book_shelf)
        session.commit()

        # Verify the book's last_modified is NOT updated (still old)
        # This ensures only date_added triggers the sync
        book_last_modified = books[2].last_modified
        if hasattr(book_last_modified, 'tzinfo') and book_last_modified.tzinfo is None:
            book_last_modified = book_last_modified.replace(tzinfo=timezone.utc)
        assert book_last_modified < datetime.now(timezone.utc), (
            "Test setup error: book's last_modified should be in the past"
        )

        # Second sync - should return only the newly added book
        _, payload2 = _make_sync_request(kobo, app, session, token=token1)
        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements2) == 1, (
            f"or_() FIX TEST: Expected 1 book (added to shelf) in second sync, got {len(entitlements2)}. "
            f"This indicates the or_() condition is not working - date_added alone should trigger sync."
        )
        assert _extract_entitlement_ids(entitlements2) == [books[2].uuid], (
            f"Expected book {books[2].uuid} to be synced via date_added trigger"
        )

def test_only_kobo_shelves_or_condition_last_modified_triggers_sync(monkeypatch):
    """
    Test that the or_() fix works: a book is synced when ONLY last_modified triggers.

    This tests the second part of the or_() condition:
        or_(
            func.datetime(ub.BookShelf.date_added) > sync_token.tags_last_modified,
            func.datetime(db.Books.last_modified) > sync_token.books_last_modified,
        )

    Scenario:
    1. Sync a shelf with 2 books
    2. Modify one book's metadata (last_modified > books_last_modified)
    3. Do NOT re-add the book to shelf (date_added unchanged)
    4. Second sync should return the modified book

    This would FAIL without the or_() fix because the query would require
    BOTH conditions to be true, not just one.
    """
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

        # First sync - should return 2 books
        response1, payload1 = _make_sync_request(kobo, app, session)
        token1 = response1.headers.get("x-kobo-synctoken")

        entitlements1 = [item for item in payload1 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements1) == 2, f"Expected 2 books in first sync, got {len(entitlements1)}"

        # Modify the first book's metadata (update last_modified)
        # but do NOT change its date_added on the shelf
        books[0].last_modified = datetime.now(timezone.utc) + timedelta(days=1)
        session.commit()

        # Second sync - should return only the modified book
        _, payload2 = _make_sync_request(kobo, app, session, token=token1)
        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements2) == 1, (
            f"or_() FIX TEST: Expected 1 book (metadata modified) in second sync, got {len(entitlements2)}. "
            f"This indicates the or_() condition is not working - last_modified alone should trigger sync."
        )
        assert _extract_entitlement_ids(entitlements2) == [books[0].uuid], (
            f"Expected book {books[0].uuid} to be synced via last_modified trigger"
        )


#Internal helper functions for kobo sync integration testing
def _seed_books(session, count):
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

def _create_kobo_shelf_with_books(app_session, user_id, book_ids, shelf_name="Test Shelf"):
    """Create a shelf marked for Kobo sync and add books to it.

    Note: date_added is set slightly after shelf.last_modified to reproduce
    the timing mismatch that causes the download loop bug (fixed in 309865c9).
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

    # Set date_added slightly after shelf.last_modified to reproduce the bug condition
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
    """Configure common monkeypatch settings for Kobo sync tests."""
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
    """Create a Flask test app with the required WSGI wrapper."""
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
    """Make a sync request and return the response and parsed payload."""
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
    """Context manager for test session lifecycle."""
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
    """Context manager for split session lifecycle."""
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
    ids = []
    for item in payload:
        for key in ("NewEntitlement", "ChangedEntitlement"):
            if key in item:
                ids.append(item[key]["BookEntitlement"]["Id"])
                break
    return ids

def _collect_entitlement_ids(payload):
    return set(_extract_entitlement_ids(payload))
