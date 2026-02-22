import json
import sys
import types
from base64 import b64decode, b64encode
from contextlib import contextmanager
from datetime import datetime
from importlib import import_module

from flask import Flask, g
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from cps import db, ub


def _to_epoch_timestamp(datetime_object):
    return (datetime_object - datetime(1970, 1, 1)).total_seconds()


def _get_datetime_from_json(json_object, field_name):
    try:
        # Convert epoch to UTC datetime (naive, matching kobo.py behavior)
        from datetime import timezone
        return datetime.fromtimestamp(json_object[field_name], tz=timezone.utc).replace(tzinfo=None)
    except (KeyError, OSError, OverflowError):
        return datetime.min


class StubSyncToken:
    SYNC_TOKEN_HEADER = "x-kobo-synctoken"
    VERSION = "1-2-0"

    def __init__(self):
        self.raw_kobo_store_token = ""
        self.books_last_created = datetime.min
        self.books_last_modified = datetime.min
        self.books_last_id = -1
        self.archive_last_modified = datetime.min
        self.reading_state_last_modified = datetime.min
        self.tags_last_modified = datetime.min

    @classmethod
    def from_headers(cls, headers):
        sync_token_header = headers.get(cls.SYNC_TOKEN_HEADER, "")
        if sync_token_header == "" or sync_token_header == "stub":
            return cls()

        try:
            sync_token_json = json.loads(
                b64decode(sync_token_header + "=" * (-len(sync_token_header) % 4))
            )
            data_json = sync_token_json.get("data", {})

            token = cls()
            token.raw_kobo_store_token = data_json.get("raw_kobo_store_token", "")
            token.books_last_created = _get_datetime_from_json(data_json, "books_last_created")
            token.books_last_modified = _get_datetime_from_json(data_json, "books_last_modified")
            token.books_last_id = data_json.get("books_last_id", -1)
            token.archive_last_modified = _get_datetime_from_json(data_json, "archive_last_modified")
            token.reading_state_last_modified = _get_datetime_from_json(data_json, "reading_state_last_modified")
            token.tags_last_modified = _get_datetime_from_json(data_json, "tags_last_modified")
            return token
        except Exception:
            return cls()

    def to_headers(self, headers):
        token = {
            "version": self.VERSION,
            "data": {
                "raw_kobo_store_token": self.raw_kobo_store_token,
                "books_last_created": _to_epoch_timestamp(self.books_last_created),
                "books_last_modified": _to_epoch_timestamp(self.books_last_modified),
                "books_last_id": self.books_last_id,
                "archive_last_modified": _to_epoch_timestamp(self.archive_last_modified),
                "reading_state_last_modified": _to_epoch_timestamp(self.reading_state_last_modified),
                "tags_last_modified": _to_epoch_timestamp(self.tags_last_modified),
            },
        }
        headers[self.SYNC_TOKEN_HEADER] = b64encode(json.dumps(token).encode()).decode("utf-8")


def install_stub_modules():
    if "cps.gdriveutils" not in sys.modules:
        gdrive_stub = types.ModuleType("cps.gdriveutils")
        gdrive_stub.getFileFromEbooksFolder = lambda *args, **kwargs: None
        gdrive_stub.do_gdrive_download = lambda *args, **kwargs: None
        sys.modules["cps.gdriveutils"] = gdrive_stub

    if "cps.services.SyncToken" not in sys.modules:
        sync_token_stub = types.ModuleType("cps.services.SyncToken")
        sync_token_stub.SyncToken = StubSyncToken
        sync_token_stub.b64encode_json = lambda json_data: b64encode(json.dumps(json_data).encode()).decode("utf-8")
        sys.modules["cps.services.SyncToken"] = sync_token_stub


def import_kobo():
    install_stub_modules()
    return import_module("cps.kobo")


# =============================================================================
# Flask app factory and WSGI wrapper
# =============================================================================

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


# =============================================================================
# Session builders and context managers
# =============================================================================

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
