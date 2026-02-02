import json
import pytest
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from flask import Flask, g
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.sql.expression import true

from kobo_test_support import import_kobo

from cps import db, ub, constants, kobo_sync_status


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


def test_sync_returns_entitlements_and_updates_synced_books(monkeypatch):
    kobo = import_kobo()
    session, conn, engine = _build_session()
    old_session = ub.session
    ub.session = session
    try:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        session.add(user)
        session.commit()

        _seed_books(session, 2)

        monkeypatch.setattr(kobo, "current_user", user, raising=False)
        monkeypatch.setattr(kobo_sync_status, "current_user", user, raising=False)
        monkeypatch.setattr(kobo.config, "config_kobo_proxy", False, raising=False)
        monkeypatch.setattr(kobo.config, "config_external_port", 80, raising=False)
        monkeypatch.setattr(kobo.config, "config_kepubifypath", None, raising=False)
        monkeypatch.setattr(kobo, "get_epub_layout", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "reconnect_db", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "common_filters", lambda *a, **k: true())

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

        with app.test_request_context("/kobo/testtoken/v1/library/sync", base_url="http://example.com"):
            g.lib_sql = session
            response = kobo.HandleSyncRequest.__wrapped__()

        payload = json.loads(response.get_data(as_text=True))

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
    finally:
        session.close()
        ub.session = old_session
        conn.close()
        engine.dispose()


def _extract_entitlement_ids(payload):
    ids = []
    for item in payload:
        for key in ("NewEntitlement", "ChangedEntitlement"):
            if key in item:
                ids.append(item[key]["BookEntitlement"]["Id"])
                break
    return ids

def test_sync_over_limit_does_not_repeat_payload(monkeypatch, tmp_path):
    kobo = import_kobo()
    calibre_session, calibre_conn, calibre_engine, app_session, app_engine, app_db_path = _build_split_sessions(
        tmp_path
    )
    old_session = ub.session
    old_app_db_path = ub.app_DB_path
    ub.session = app_session
    ub.app_DB_path = str(app_db_path)
    try:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        app_session.add(user)
        app_session.commit()

        sync_limit = 3
        _seed_books(calibre_session, sync_limit + 2)

        monkeypatch.setattr(kobo, "current_user", user, raising=False)
        monkeypatch.setattr(kobo_sync_status, "current_user", user, raising=False)
        monkeypatch.setattr(kobo.shelf_lib, "current_user", user, raising=False)
        monkeypatch.setattr(kobo, "SYNC_ITEM_LIMIT", sync_limit, raising=False)
        monkeypatch.setattr(kobo.config, "config_kobo_proxy", False, raising=False)
        monkeypatch.setattr(kobo.config, "config_external_port", 80, raising=False)
        monkeypatch.setattr(kobo.config, "config_kepubifypath", None, raising=False)
        monkeypatch.setattr(kobo, "get_epub_layout", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "reconnect_db", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "common_filters", lambda *a, **k: true())

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

        with app.test_request_context("/kobo/testtoken/v1/library/sync", base_url="http://example.com"):
            g.lib_sql = calibre_session
            response1 = kobo.HandleSyncRequest.__wrapped__()

        payload1 = json.loads(response1.get_data(as_text=True))
        token = response1.headers.get("x-kobo-synctoken")

        with app.test_request_context(
            "/kobo/testtoken/v1/library/sync",
            base_url="http://example.com",
            headers={"x-kobo-synctoken": token},
        ):
            g.lib_sql = calibre_session
            response2 = kobo.HandleSyncRequest.__wrapped__()

        payload2 = json.loads(response2.get_data(as_text=True))

        assert len(payload1) == sync_limit
        assert response1.headers.get("x-kobo-sync") == "continue"
        assert len(payload2) == 2, (
            f"expected 2 itmes (remaining books over sync_limit) in payload2, got {len(payload2)}"
        )
        assert response2.headers.get("x-kobo-sync") is None
        assert set(_extract_entitlement_ids(payload1)).isdisjoint(_extract_entitlement_ids(payload2))
    finally:
        ub.session = old_session
        ub.app_DB_path = old_app_db_path
        app_session.close()
        app_engine.dispose()
        calibre_session.close()
        calibre_conn.close()
        calibre_engine.dispose()


def test_sync_exactly_limit_does_not_set_continue_header(monkeypatch):
    kobo = import_kobo()
    session, conn, engine = _build_session()
    old_session = ub.session
    ub.session = session
    try:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        session.add(user)
        session.commit()

        sync_limit = 3
        _seed_books(session, sync_limit)

        monkeypatch.setattr(kobo, "current_user", user, raising=False)
        monkeypatch.setattr(kobo_sync_status, "current_user", user, raising=False)
        monkeypatch.setattr(kobo.shelf_lib, "current_user", user, raising=False)
        monkeypatch.setattr(kobo, "SYNC_ITEM_LIMIT", sync_limit, raising=False)
        monkeypatch.setattr(kobo.config, "config_kobo_proxy", False, raising=False)
        monkeypatch.setattr(kobo.config, "config_external_port", 80, raising=False)
        monkeypatch.setattr(kobo.config, "config_kepubifypath", None, raising=False)
        monkeypatch.setattr(kobo, "get_epub_layout", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "reconnect_db", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "common_filters", lambda *a, **k: true())

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

        with app.test_request_context("/kobo/testtoken/v1/library/sync", base_url="http://example.com"):
            g.lib_sql = session
            response = kobo.HandleSyncRequest.__wrapped__()

        payload = json.loads(response.get_data(as_text=True))
        entitlements = [item for item in payload if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements) == sync_limit
        assert response.headers.get("x-kobo-sync") is None, (
            "Expected no continuation header when result count equals SYNC_ITEM_LIMIT."
        )
    finally:
        session.close()
        ub.session = old_session
        conn.close()
        engine.dispose()


def test_modified_synced_book_is_resent(monkeypatch):
    kobo = import_kobo()
    session, conn, engine = _build_session()
    old_session = ub.session
    ub.session = session
    try:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        session.add(user)
        session.commit()

        _seed_books(session, 1)
        book = session.query(db.Books).first()

        monkeypatch.setattr(kobo, "current_user", user, raising=False)
        monkeypatch.setattr(kobo_sync_status, "current_user", user, raising=False)
        monkeypatch.setattr(kobo.shelf_lib, "current_user", user, raising=False)
        monkeypatch.setattr(kobo.config, "config_kobo_proxy", False, raising=False)
        monkeypatch.setattr(kobo.config, "config_external_port", 80, raising=False)
        monkeypatch.setattr(kobo.config, "config_kepubifypath", None, raising=False)
        monkeypatch.setattr(kobo, "get_epub_layout", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "reconnect_db", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "common_filters", lambda *a, **k: true())

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

        with app.test_request_context("/kobo/testtoken/v1/library/sync", base_url="http://example.com"):
            g.lib_sql = session
            response1 = kobo.HandleSyncRequest.__wrapped__()

        token1 = response1.headers.get("x-kobo-synctoken")
        book.last_modified = datetime.now(timezone.utc) + timedelta(days=1)
        session.commit()

        with app.test_request_context(
            "/kobo/testtoken/v1/library/sync",
            base_url="http://example.com",
            headers={"x-kobo-synctoken": token1},
        ):
            g.lib_sql = session
            response2 = kobo.HandleSyncRequest.__wrapped__()

        payload2 = json.loads(response2.get_data(as_text=True))
        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements2) == 1, "Expected modified synced book to be returned again."
        assert _extract_entitlement_ids(entitlements2) == [book.uuid]
    finally:
        session.close()
        ub.session = old_session
        conn.close()
        engine.dispose()


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


def test_only_kobo_shelves_no_repeat_books_after_sync(monkeypatch):
    """
    Test that books on a Kobo-synced shelf don't repeat in subsequent syncs.

    This test would FAIL before commit 309865c9 because tags_last_modified
    wasn't being updated to cover BookShelf.date_added values.
    """
    kobo = import_kobo()
    session, conn, engine = _build_session()
    old_session = ub.session
    ub.session = session
    try:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        _seed_books(session, 3)
        books = session.query(db.Books).all()

        _create_kobo_shelf_with_books(session, user.id, [b.id for b in books], "Kobo Shelf")

        monkeypatch.setattr(kobo, "current_user", user, raising=False)
        monkeypatch.setattr(kobo_sync_status, "current_user", user, raising=False)
        monkeypatch.setattr(kobo.shelf_lib, "current_user", user, raising=False)
        monkeypatch.setattr(kobo.config, "config_kobo_proxy", False, raising=False)
        monkeypatch.setattr(kobo.config, "config_external_port", 80, raising=False)
        monkeypatch.setattr(kobo.config, "config_kepubifypath", None, raising=False)
        monkeypatch.setattr(kobo, "get_epub_layout", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "reconnect_db", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "common_filters", lambda *a, **k: true())

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

        # First sync - should return all 3 books
        with app.test_request_context("/kobo/testtoken/v1/library/sync", base_url="http://example.com"):
            g.lib_sql = session
            response1 = kobo.HandleSyncRequest.__wrapped__()

        payload1 = json.loads(response1.get_data(as_text=True))
        token = response1.headers.get("x-kobo-synctoken")

        entitlements1 = [item for item in payload1 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements1) == 3, f"Expected 3 books in first sync, got {len(entitlements1)}"

        # Second sync with token - should return 0 book entitlements (already synced)
        with app.test_request_context(
            "/kobo/testtoken/v1/library/sync",
            base_url="http://example.com",
            headers={"x-kobo-synctoken": token},
        ):
            g.lib_sql = session
            response2 = kobo.HandleSyncRequest.__wrapped__()

        payload2 = json.loads(response2.get_data(as_text=True))
        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements2) == 0, (
            f"BUG: Expected 0 books in second sync (books should not repeat), "
            f"got {len(entitlements2)}. This indicates tags_last_modified is not "
            f"being properly updated to cover BookShelf.date_added values."
        )
    finally:
        session.close()
        ub.session = old_session
        conn.close()
        engine.dispose()


def test_only_kobo_shelves_modified_synced_book_is_resent(monkeypatch):
    kobo = import_kobo()
    session, conn, engine = _build_session()
    old_session = ub.session
    ub.session = session
    try:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        _seed_books(session, 1)
        book = session.query(db.Books).first()
        _create_kobo_shelf_with_books(session, user.id, [book.id], "Kobo Shelf")

        monkeypatch.setattr(kobo, "current_user", user, raising=False)
        monkeypatch.setattr(kobo_sync_status, "current_user", user, raising=False)
        monkeypatch.setattr(kobo.shelf_lib, "current_user", user, raising=False)
        monkeypatch.setattr(kobo.config, "config_kobo_proxy", False, raising=False)
        monkeypatch.setattr(kobo.config, "config_external_port", 80, raising=False)
        monkeypatch.setattr(kobo.config, "config_kepubifypath", None, raising=False)
        monkeypatch.setattr(kobo, "get_epub_layout", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "reconnect_db", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "common_filters", lambda *a, **k: true())

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

        with app.test_request_context("/kobo/testtoken/v1/library/sync", base_url="http://example.com"):
            g.lib_sql = session
            response1 = kobo.HandleSyncRequest.__wrapped__()

        token1 = response1.headers.get("x-kobo-synctoken")
        book.last_modified = datetime.now(timezone.utc) + timedelta(days=1)
        session.commit()

        with app.test_request_context(
            "/kobo/testtoken/v1/library/sync",
            base_url="http://example.com",
            headers={"x-kobo-synctoken": token1},
        ):
            g.lib_sql = session
            response2 = kobo.HandleSyncRequest.__wrapped__()

        payload2 = json.loads(response2.get_data(as_text=True))
        entitlements2 = [item for item in payload2 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements2) == 1, "Expected modified synced book to be returned again."
        assert _extract_entitlement_ids(entitlements2) == [book.uuid]
    finally:
        session.close()
        ub.session = old_session
        conn.close()
        engine.dispose()


def test_only_kobo_shelves_pagination_no_repeats(monkeypatch):
    """
    Test that paginated syncs in only_kobo_shelves mode don't repeat books across pages.

    This test would FAIL before commit 309865c9 because tags_last_modified
    wasn't being updated during pagination.
    """
    kobo = import_kobo()
    session, conn, engine = _build_session()
    old_session = ub.session
    ub.session = session
    try:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        sync_limit = 3
        _seed_books(session, sync_limit + 2)
        books = session.query(db.Books).all()

        _create_kobo_shelf_with_books(session, user.id, [b.id for b in books], "Kobo Shelf")

        monkeypatch.setattr(kobo, "current_user", user, raising=False)
        monkeypatch.setattr(kobo_sync_status, "current_user", user, raising=False)
        monkeypatch.setattr(kobo.shelf_lib, "current_user", user, raising=False)
        monkeypatch.setattr(kobo, "SYNC_ITEM_LIMIT", sync_limit, raising=False)
        monkeypatch.setattr(kobo.config, "config_kobo_proxy", False, raising=False)
        monkeypatch.setattr(kobo.config, "config_external_port", 80, raising=False)
        monkeypatch.setattr(kobo.config, "config_kepubifypath", None, raising=False)
        monkeypatch.setattr(kobo, "get_epub_layout", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "reconnect_db", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "common_filters", lambda *a, **k: true())

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

        # First sync (page 1) - should return sync_limit books
        with app.test_request_context("/kobo/testtoken/v1/library/sync", base_url="http://example.com"):
            g.lib_sql = session
            response1 = kobo.HandleSyncRequest.__wrapped__()

        payload1 = json.loads(response1.get_data(as_text=True))
        token1 = response1.headers.get("x-kobo-synctoken")

        entitlements1 = [item for item in payload1 if "NewEntitlement" in item or "ChangedEntitlement" in item]
        assert len(entitlements1) == sync_limit
        assert response1.headers.get("x-kobo-sync") == "continue"

        # Second sync (page 2) - should return remaining books
        with app.test_request_context(
            "/kobo/testtoken/v1/library/sync",
            base_url="http://example.com",
            headers={"x-kobo-synctoken": token1},
        ):
            g.lib_sql = session
            response2 = kobo.HandleSyncRequest.__wrapped__()

        payload2 = json.loads(response2.get_data(as_text=True))
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
        with app.test_request_context(
            "/kobo/testtoken/v1/library/sync",
            base_url="http://example.com",
            headers={"x-kobo-synctoken": token2},
        ):
            g.lib_sql = session
            response3 = kobo.HandleSyncRequest.__wrapped__()

        payload3 = json.loads(response3.get_data(as_text=True))
        entitlements3 = [item for item in payload3 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements3) == 0, (
            f"Expected 0 books in third sync, got {len(entitlements3)}"
        )
    finally:
        session.close()
        ub.session = old_session
        conn.close()
        engine.dispose()


def test_sync_shelves_updates_tags_last_modified(monkeypatch):
    """
    Test that sync properly updates tags_last_modified when books are added to shelves.

    This test would FAIL before commit 309865c9 because sync_shelves() only tracked
    Shelf.last_modified, not BookShelf.date_added.
    """
    kobo = import_kobo()
    session, conn, engine = _build_session()
    old_session = ub.session
    ub.session = session
    try:
        user = ub.User(name="test", email="test@example.org", role=constants.ROLE_DOWNLOAD)
        user.kobo_only_shelves_sync = 1
        session.add(user)
        session.commit()

        _seed_books(session, 3)
        books = session.query(db.Books).all()

        # Create shelf with only 2 books initially
        _create_kobo_shelf_with_books(session, user.id, [books[0].id, books[1].id], "Kobo Shelf")

        monkeypatch.setattr(kobo, "current_user", user, raising=False)
        monkeypatch.setattr(kobo_sync_status, "current_user", user, raising=False)
        monkeypatch.setattr(kobo.shelf_lib, "current_user", user, raising=False)
        monkeypatch.setattr(kobo.config, "config_kobo_proxy", False, raising=False)
        monkeypatch.setattr(kobo.config, "config_external_port", 80, raising=False)
        monkeypatch.setattr(kobo.config, "config_kepubifypath", None, raising=False)
        monkeypatch.setattr(kobo, "get_epub_layout", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "reconnect_db", lambda *a, **k: None)
        monkeypatch.setattr(kobo.calibre_db, "common_filters", lambda *a, **k: true())

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

        # First sync - should return 2 books
        with app.test_request_context("/kobo/testtoken/v1/library/sync", base_url="http://example.com"):
            g.lib_sql = session
            response1 = kobo.HandleSyncRequest.__wrapped__()

        payload1 = json.loads(response1.get_data(as_text=True))
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
        with app.test_request_context(
            "/kobo/testtoken/v1/library/sync",
            base_url="http://example.com",
            headers={"x-kobo-synctoken": token1},
        ):
            g.lib_sql = session
            response2 = kobo.HandleSyncRequest.__wrapped__()

        payload2 = json.loads(response2.get_data(as_text=True))
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
        with app.test_request_context(
            "/kobo/testtoken/v1/library/sync",
            base_url="http://example.com",
            headers={"x-kobo-synctoken": token2},
        ):
            g.lib_sql = session
            response3 = kobo.HandleSyncRequest.__wrapped__()

        payload3 = json.loads(response3.get_data(as_text=True))
        entitlements3 = [item for item in payload3 if "NewEntitlement" in item or "ChangedEntitlement" in item]

        assert len(entitlements3) == 0, (
            f"BUG: Expected 0 books in third sync, got {len(entitlements3)}. "
            f"This indicates tags_last_modified is not being properly updated."
        )
    finally:
        session.close()
        ub.session = old_session
        conn.close()
        engine.dispose()
