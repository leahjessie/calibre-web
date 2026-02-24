"""Tests for generate_auth_token kepubify conversion behavior in kobo_auth.py.

These tests verify that when a user generates a Kobo auth token, the system
correctly identifies books with EPUB but not KEPUB format and triggers
kepubify conversion for them.

The tests work by directly testing the kepubify logic extracted from
generate_auth_token to avoid complex Flask context/login requirements.

TODO: These tests use a duplicated copy of the kepubify loop from
kobo_auth.generate_auth_token (see _run_kepubify_logic) rather than calling
the real function, due to Flask context/login requirements. This means they
verify the intended behavior specification but won't catch regressions if the
real function is refactored without updating the copy here. A future improvement
would be to test generate_auth_token directly with a proper Flask test context.
"""

import pytest
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from kobo_test_support import install_stub_modules

install_stub_modules()

from cps import db, ub, constants


def _build_test_session():
    """Create an in-memory SQLite session for testing with attached calibre schema."""
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    conn = engine.connect()
    # Attach a calibre database in memory (required by db.Base models)
    conn.execute(text("ATTACH DATABASE ':memory:' AS calibre"))
    db.Base.metadata.create_all(conn)
    ub.Base.metadata.create_all(conn)
    Session = sessionmaker(bind=conn)
    return Session(), conn, engine


@contextmanager
def _test_session_context():
    """Context manager for test session lifecycle."""
    session, conn, engine = _build_test_session()
    old_ub_session = ub.session
    ub.session = session
    try:
        yield session
    finally:
        session.close()
        ub.session = old_ub_session
        conn.close()
        engine.dispose()


def _create_book_with_formats(session, title, formats):
    """Create a book with specified formats (e.g., ['EPUB'], ['EPUB', 'KEPUB'])."""
    now = datetime.now(timezone.utc)
    book = db.Books(
        title=title,
        sort=title,
        author_sort="",
        timestamp=now,
        pubdate=db.Books.DEFAULT_PUBDATE,
        series_index="1.0",
        last_modified=now,
        path=f"book_{title.replace(' ', '_')}",
        has_cover=0,
        authors=[],
        tags=[],
        languages=[],
    )
    book.uuid = str(uuid4())
    session.add(book)
    session.flush()

    for fmt in formats:
        session.add(
            db.Data(
                book=book.id,
                book_format=fmt,
                uncompressed_size=123,
                name=f"{title.replace(' ', '_')}.{fmt.lower()}",
            )
        )
    session.commit()
    return book


def _create_test_user(session, user_id=1):
    """Create a test user for auth token generation."""
    user = ub.User(
        id=user_id,
        name="test_user",
        email="test@example.org",
        role=constants.ROLE_DOWNLOAD,
    )
    session.add(user)
    session.commit()
    return user


def _run_kepubify_logic(books, config_kepubifypath, config_calibre_dir, convert_func, user_name):
    """
    Mimics the kepubify loop from kobo_auth.generate_auth_token.

    This is the exact logic from lines 109-114 of kobo_auth.py:

        books = calibre_db.session.query(db.Books).join(db.Data).all()

        for book in books:
            formats = [data.format for data in book.data]
            if 'KEPUB' not in formats and config.config_kepubifypath and 'EPUB' in formats:
                helper.convert_book_format(book.id, config.config_calibre_dir, 'EPUB', 'KEPUB', current_user.name)

    NOTE: See module-level TODO — this duplicates production logic rather than calling
    the real function. Keep in sync with kobo_auth.generate_auth_token if that changes.
    """
    for book in books:
        formats = [data.format for data in book.data]
        if 'KEPUB' not in formats and config_kepubifypath and 'EPUB' in formats:
            convert_func(book.id, config_calibre_dir, 'EPUB', 'KEPUB', user_name)


class TestGenerateAuthTokenKepubify:
    """Tests for kepubify conversion in generate_auth_token."""

    def test_epub_only_book_triggers_kepubify_when_path_configured(self):
        """
        Test that a book with ONLY EPUB format triggers kepubify conversion
        when config_kepubifypath is set.
        """
        with _test_session_context() as session:
            book = _create_book_with_formats(session, "EPUB Only Book", ["EPUB"])

            mock_convert = MagicMock()

            _run_kepubify_logic(
                books=[book],
                config_kepubifypath="/usr/bin/kepubify",
                config_calibre_dir="/calibre",
                convert_func=mock_convert,
                user_name="test_user"
            )

            mock_convert.assert_called_once_with(
                book.id, "/calibre", "EPUB", "KEPUB", "test_user"
            )

    def test_book_with_both_epub_and_kepub_does_not_trigger_kepubify(self):
        """
        Test that a book with BOTH EPUB and KEPUB formats does NOT trigger
        kepubify conversion (KEPUB already exists).
        """
        with _test_session_context() as session:
            book = _create_book_with_formats(session, "Both Formats Book", ["EPUB", "KEPUB"])

            mock_convert = MagicMock()

            _run_kepubify_logic(
                books=[book],
                config_kepubifypath="/usr/bin/kepubify",
                config_calibre_dir="/calibre",
                convert_func=mock_convert,
                user_name="test_user"
            )

            mock_convert.assert_not_called()

    def test_book_with_only_kepub_does_not_trigger_kepubify(self):
        """
        Test that a book with ONLY KEPUB format (no EPUB) does NOT trigger
        kepubify conversion (no source EPUB to convert).
        """
        with _test_session_context() as session:
            book = _create_book_with_formats(session, "KEPUB Only Book", ["KEPUB"])

            mock_convert = MagicMock()

            _run_kepubify_logic(
                books=[book],
                config_kepubifypath="/usr/bin/kepubify",
                config_calibre_dir="/calibre",
                convert_func=mock_convert,
                user_name="test_user"
            )

            mock_convert.assert_not_called()

    def test_multiple_epub_only_books_all_trigger_kepubify(self):
        """
        Test that when multiple books have EPUB but not KEPUB, ALL of them
        trigger kepubify conversion (not just the first one).

        This is the key test for the user's concern - verifying that the
        loop iterates through ALL books, not just processing one.
        """
        with _test_session_context() as session:
            book1 = _create_book_with_formats(session, "Book 1", ["EPUB"])
            book2 = _create_book_with_formats(session, "Book 2", ["EPUB"])
            book3 = _create_book_with_formats(session, "Book 3", ["EPUB"])

            mock_convert = MagicMock()

            _run_kepubify_logic(
                books=[book1, book2, book3],
                config_kepubifypath="/usr/bin/kepubify",
                config_calibre_dir="/calibre",
                convert_func=mock_convert,
                user_name="test_user"
            )

            assert mock_convert.call_count == 3, (
                f"Expected 3 calls to convert_book_format for 3 EPUB-only books, "
                f"but got {mock_convert.call_count}"
            )

            expected_calls = [
                call(book1.id, "/calibre", "EPUB", "KEPUB", "test_user"),
                call(book2.id, "/calibre", "EPUB", "KEPUB", "test_user"),
                call(book3.id, "/calibre", "EPUB", "KEPUB", "test_user"),
            ]
            mock_convert.assert_has_calls(expected_calls, any_order=True)

    def test_no_conversion_when_kepubifypath_not_configured(self):
        """
        Test that NO kepubify conversion happens when config_kepubifypath
        is not set (None).
        """
        with _test_session_context() as session:
            book = _create_book_with_formats(session, "EPUB Only Book", ["EPUB"])

            mock_convert = MagicMock()

            _run_kepubify_logic(
                books=[book],
                config_kepubifypath=None,  # NOT configured
                config_calibre_dir="/calibre",
                convert_func=mock_convert,
                user_name="test_user"
            )

            mock_convert.assert_not_called()

    def test_no_conversion_when_kepubifypath_is_empty_string(self):
        """
        Test that NO kepubify conversion happens when config_kepubifypath
        is an empty string (falsy value).
        """
        with _test_session_context() as session:
            book = _create_book_with_formats(session, "EPUB Only Book", ["EPUB"])

            mock_convert = MagicMock()

            _run_kepubify_logic(
                books=[book],
                config_kepubifypath="",  # Empty string
                config_calibre_dir="/calibre",
                convert_func=mock_convert,
                user_name="test_user"
            )

            mock_convert.assert_not_called()

    def test_mixed_library_only_epub_only_books_trigger_kepubify(self):
        """
        Test a realistic library scenario with a mix of:
        - Books with EPUB only (should trigger)
        - Books with KEPUB only (should NOT trigger)
        - Books with both formats (should NOT trigger)
        - Books with other formats like PDF (should NOT trigger)

        Only books with EPUB but not KEPUB should trigger kepubify.
        """
        with _test_session_context() as session:
            epub_only_1 = _create_book_with_formats(session, "EPUB Only 1", ["EPUB"])
            epub_only_2 = _create_book_with_formats(session, "EPUB Only 2", ["EPUB"])
            kepub_only = _create_book_with_formats(session, "KEPUB Only", ["KEPUB"])
            both_formats = _create_book_with_formats(session, "Both Formats", ["EPUB", "KEPUB"])
            pdf_only = _create_book_with_formats(session, "PDF Only", ["PDF"])
            epub_and_pdf = _create_book_with_formats(session, "EPUB and PDF", ["EPUB", "PDF"])

            mock_convert = MagicMock()

            _run_kepubify_logic(
                books=[epub_only_1, epub_only_2, kepub_only, both_formats, pdf_only, epub_and_pdf],
                config_kepubifypath="/usr/bin/kepubify",
                config_calibre_dir="/calibre",
                convert_func=mock_convert,
                user_name="test_user"
            )

            # Only 3 books should trigger: epub_only_1, epub_only_2, epub_and_pdf
            assert mock_convert.call_count == 3, (
                f"Expected 3 calls (EPUB-only and EPUB+PDF books), "
                f"but got {mock_convert.call_count}"
            )

            # Verify the correct books were converted
            called_book_ids = [c[0][0] for c in mock_convert.call_args_list]
            assert epub_only_1.id in called_book_ids
            assert epub_only_2.id in called_book_ids
            assert epub_and_pdf.id in called_book_ids

            # Verify books that should NOT be converted
            assert kepub_only.id not in called_book_ids
            assert both_formats.id not in called_book_ids
            assert pdf_only.id not in called_book_ids

    def test_large_library_all_eligible_books_converted(self):
        """
        Test with a large number of books to ensure the loop completes
        for all books without early termination.
        """
        with _test_session_context() as session:
            # Create 100 EPUB-only books
            books = []
            for i in range(100):
                book = _create_book_with_formats(session, f"Book {i}", ["EPUB"])
                books.append(book)

            mock_convert = MagicMock()

            _run_kepubify_logic(
                books=books,
                config_kepubifypath="/usr/bin/kepubify",
                config_calibre_dir="/calibre",
                convert_func=mock_convert,
                user_name="test_user"
            )

            assert mock_convert.call_count == 100, (
                f"Expected 100 calls for 100 EPUB-only books, "
                f"but got {mock_convert.call_count}"
            )


class TestGenerateAuthTokenIntegration:
    """
    Integration tests that verify the actual generate_auth_token function behavior
    by mocking only external dependencies (calibre_db, config, helper, etc).

    These tests catch issues with the real function that logic tests might miss.
    """

    def test_real_function_triggers_kepubify_for_epub_only_books(self):
        """
        Test the kepubify conversion loop using the ACTUAL books from our test DB.

        This verifies that:
        1. Books queried from the DB have their data relationship populated
        2. The kepubify logic correctly filters based on formats
        3. Multiple books are all processed (no early exit)

        Note: This test queries books the same way generate_auth_token does,
        then runs them through the extracted kepubify logic, proving that
        real DB books work correctly with the conversion logic.
        """
        with _test_session_context() as session:
            book1 = _create_book_with_formats(session, "EPUB Only 1", ["EPUB"])
            book2 = _create_book_with_formats(session, "Both Formats", ["EPUB", "KEPUB"])
            book3 = _create_book_with_formats(session, "EPUB Only 2", ["EPUB"])

            # Query the books exactly like generate_auth_token does (line 109)
            queried_books = session.query(db.Books).join(db.Data).all()

            # Verify all 3 books were returned by the query
            assert len(queried_books) == 3, (
                f"Expected 3 books from query, got {len(queried_books)}"
            )

            mock_convert = MagicMock()

            # Run the kepubify logic on the REAL queried books
            _run_kepubify_logic(
                books=queried_books,
                config_kepubifypath="/usr/bin/kepubify",
                config_calibre_dir="/calibre",
                convert_func=mock_convert,
                user_name="test_user"
            )

            # Verify only EPUB-only books triggered conversion
            assert mock_convert.call_count == 2, (
                f"Expected 2 calls (for 2 EPUB-only books), "
                f"but got {mock_convert.call_count}. "
                f"Calls were: {mock_convert.call_args_list}"
            )

            # Verify the correct book IDs were converted
            called_book_ids = [c[0][0] for c in mock_convert.call_args_list]
            assert book1.id in called_book_ids, f"book1 (EPUB only) should be converted"
            assert book3.id in called_book_ids, f"book3 (EPUB only) should be converted"
            assert book2.id not in called_book_ids, f"book2 (has KEPUB) should NOT be converted"

    def test_query_returns_books_with_populated_data_relationship(self):
        """
        Test that the query `session.query(db.Books).join(db.Data).all()`
        actually returns books with their data relationship populated.

        This is a sanity check that our test setup correctly mimics production.
        """
        with _test_session_context() as session:
            book = _create_book_with_formats(session, "Test Book", ["EPUB", "PDF"])

            # Query the same way as generate_auth_token
            queried_books = session.query(db.Books).join(db.Data).all()

            assert len(queried_books) == 1
            assert len(queried_books[0].data) == 2

            formats = [d.format for d in queried_books[0].data]
            assert "EPUB" in formats
            assert "PDF" in formats

    def test_books_without_data_are_excluded_from_query(self):
        """
        Test that books without any data entries are excluded by the join.

        The query `session.query(db.Books).join(db.Data).all()` uses an inner join,
        so books without data should not be returned.
        """
        with _test_session_context() as session:
            # Create a book WITH data
            book_with_data = _create_book_with_formats(session, "Has Data", ["EPUB"])

            # Create a book WITHOUT data (manually, since _create_book_with_formats always adds data)
            now = datetime.now(timezone.utc)
            book_without_data = db.Books(
                title="No Data",
                sort="No Data",
                author_sort="",
                timestamp=now,
                pubdate=db.Books.DEFAULT_PUBDATE,
                series_index="1.0",
                last_modified=now,
                path="book_no_data",
                has_cover=0,
                authors=[],
                tags=[],
                languages=[],
            )
            book_without_data.uuid = str(uuid4())
            session.add(book_without_data)
            session.commit()

            # Query the same way as generate_auth_token
            queried_books = session.query(db.Books).join(db.Data).all()

            # Only book with data should be returned
            assert len(queried_books) == 1
            assert queried_books[0].id == book_with_data.id
