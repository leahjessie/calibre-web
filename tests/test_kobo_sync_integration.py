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
