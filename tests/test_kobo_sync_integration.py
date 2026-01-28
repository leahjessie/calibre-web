import json
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
