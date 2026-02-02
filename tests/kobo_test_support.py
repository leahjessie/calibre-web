import json
import sys
import types
from base64 import b64decode, b64encode
from datetime import datetime
from importlib import import_module


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
    VERSION = "1-1-0"

    def __init__(self):
        self.raw_kobo_store_token = ""
        self.books_last_created = datetime.min
        self.books_last_modified = datetime.min
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
        sys.modules["cps.services.SyncToken"] = sync_token_stub


def import_kobo():
    install_stub_modules()
    return import_module("cps.kobo")
