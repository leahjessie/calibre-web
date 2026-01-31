import sys
import types
from datetime import datetime
from importlib import import_module


class StubSyncToken:
    SYNC_TOKEN_HEADER = "x-kobo-synctoken"

    def __init__(self):
        self.books_last_created = datetime.min
        self.books_last_modified = datetime.min
        self.archive_last_modified = datetime.min
        self.reading_state_last_modified = datetime.min
        self.tags_last_modified = datetime.min

    @classmethod
    def from_headers(cls, headers):
        return cls()

    def to_headers(self, headers):
        headers[self.SYNC_TOKEN_HEADER] = "stub"


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
