import json
from datetime import datetime, timezone
from types import SimpleNamespace

from flask import Flask

from kobo_test_support import import_kobo


def _make_app():
    app = Flask(__name__)
    app.config["TESTING"] = True
    return app


def _import_kobo():
    return import_kobo()


def test_generate_sync_response_sets_headers_and_body(monkeypatch):
    kobo = _import_kobo()
    app = _make_app()
    sync_results = [{"NewEntitlement": {"BookEntitlement": {"Id": "book-1"}}}]
    sync_token = SimpleNamespace(
        to_headers=lambda headers: headers.__setitem__("x-kobo-synctoken", "stub"),
    )

    monkeypatch.setattr(kobo.config, "config_kobo_proxy", False, raising=False)

    with app.app_context():
        response = kobo.generate_sync_response(sync_token, sync_results, set_cont=False)

    assert response.headers["Content-Type"].startswith("application/json")
    assert "x-kobo-synctoken" in response.headers
    assert response.get_data(as_text=True) == json.dumps(sync_results)


def test_generate_sync_response_sets_continue_header(monkeypatch):
    kobo = _import_kobo()
    app = _make_app()
    sync_token = SimpleNamespace(
        to_headers=lambda headers: headers.__setitem__("x-kobo-synctoken", "stub"),
    )

    monkeypatch.setattr(kobo.config, "config_kobo_proxy", False, raising=False)

    with app.app_context():
        response = kobo.generate_sync_response(sync_token, [], set_cont=True)

    assert response.headers.get("x-kobo-sync") == "continue"


def test_create_book_entitlement_reflects_archived_state():
    kobo = _import_kobo()
    book = SimpleNamespace(
        uuid="123e4567-e89b-12d3-a456-426614174000",
        timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
        last_modified=datetime(2021, 2, 3, tzinfo=timezone.utc),
    )

    entitlement = kobo.create_book_entitlement(book, archived=True)

    assert entitlement["Id"] == book.uuid
    assert entitlement["IsRemoved"] is True
