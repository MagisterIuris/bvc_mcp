"""
Unit tests for watchlist.py.

All tests use temporary SQLite files on disk (not ':memory:') because
watchlist.py needs init_db() to create the tables first, and each
sqlite3.connect(':memory:') creates a separate, independent database.
"""

from __future__ import annotations

import os
import tempfile

import pytest

from bvc_mcp.database import init_db
from bvc_mcp.watchlist import (
    add_to_watchlist,
    create_watchlist,
    delete_watchlist,
    get_watchlist,
    get_watchlist_symbols,
    list_watchlists,
    remove_from_watchlist,
)

USER_A = "test_user_a"
USER_B = "test_user_b"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def make_temp_db() -> str:
    """Create a temporary database file with schema initialised."""
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    f.close()
    init_db(f.name)
    return f.name


# ---------------------------------------------------------------------------
# create_watchlist
# ---------------------------------------------------------------------------


class TestCreateWatchlist:
    def test_returns_dict_with_expected_keys(self):
        db = make_temp_db()
        try:
            result = create_watchlist("tech", ["ATW", "IAM"], db, owner=USER_A)
            assert {"id", "name", "created_at", "symbols", "count"} == set(result.keys())
        finally:
            os.unlink(db)

    def test_symbols_are_uppercased(self):
        db = make_temp_db()
        try:
            result = create_watchlist("mix", ["atw", "iam"], db, owner=USER_A)
            assert result["symbols"] == ["ATW", "IAM"]
        finally:
            os.unlink(db)

    def test_count_matches_symbol_length(self):
        db = make_temp_db()
        try:
            result = create_watchlist("trio", ["ATW", "IAM", "BCP"], db, owner=USER_A)
            assert result["count"] == 3
        finally:
            os.unlink(db)

    def test_empty_symbols_list(self):
        db = make_temp_db()
        try:
            result = create_watchlist("empty", [], db, owner=USER_A)
            assert result["count"] == 0
        finally:
            os.unlink(db)

    def test_duplicate_name_same_owner_raises_value_error(self):
        db = make_temp_db()
        try:
            create_watchlist("dup", ["ATW"], db, owner=USER_A)
            with pytest.raises(ValueError, match="already exists"):
                create_watchlist("dup", ["IAM"], db, owner=USER_A)
        finally:
            os.unlink(db)

    def test_whitespace_symbols_stripped(self):
        db = make_temp_db()
        try:
            result = create_watchlist("ws", ["  ATW  ", " IAM"], db, owner=USER_A)
            assert "ATW" in result["symbols"]
            assert "IAM" in result["symbols"]
        finally:
            os.unlink(db)

    # --- isolation ---

    def test_same_name_different_owners_succeeds(self):
        """Two users can each have a watchlist with the same name."""
        db = make_temp_db()
        try:
            r_a = create_watchlist("tech", ["ATW"], db, owner=USER_A)
            r_b = create_watchlist("tech", ["BCP"], db, owner=USER_B)
            assert r_a["id"] != r_b["id"]
        finally:
            os.unlink(db)


# ---------------------------------------------------------------------------
# get_watchlist
# ---------------------------------------------------------------------------


class TestGetWatchlist:
    def test_returns_none_for_unknown(self):
        db = make_temp_db()
        try:
            assert get_watchlist("nonexistent", db, owner=USER_A) is None
        finally:
            os.unlink(db)

    def test_returns_correct_watchlist(self):
        db = make_temp_db()
        try:
            create_watchlist("my_list", ["ATW", "IAM"], db, owner=USER_A)
            result = get_watchlist("my_list", db, owner=USER_A)
            assert result is not None
            assert result["name"] == "my_list"
        finally:
            os.unlink(db)

    def test_stocks_list_contains_correct_symbols(self):
        db = make_temp_db()
        try:
            create_watchlist("stocks", ["ATW", "BCP"], db, owner=USER_A)
            result = get_watchlist("stocks", db, owner=USER_A)
            symbols = [s["symbol"] for s in result["stocks"]]
            assert "ATW" in symbols
            assert "BCP" in symbols
        finally:
            os.unlink(db)

    def test_count_matches_stocks_length(self):
        db = make_temp_db()
        try:
            create_watchlist("cnt", ["ATW", "IAM", "BCP"], db, owner=USER_A)
            result = get_watchlist("cnt", db, owner=USER_A)
            assert result["count"] == len(result["stocks"])
        finally:
            os.unlink(db)

    # --- isolation ---

    def test_owner_a_cannot_see_owner_b_watchlist(self):
        db = make_temp_db()
        try:
            create_watchlist("private", ["ATW"], db, owner=USER_B)
            assert get_watchlist("private", db, owner=USER_A) is None
        finally:
            os.unlink(db)

    def test_same_name_returns_correct_owner_data(self):
        db = make_temp_db()
        try:
            create_watchlist("shared_name", ["ATW"], db, owner=USER_A)
            create_watchlist("shared_name", ["BCP"], db, owner=USER_B)
            result_a = get_watchlist("shared_name", db, owner=USER_A)
            result_b = get_watchlist("shared_name", db, owner=USER_B)
            syms_a = [s["symbol"] for s in result_a["stocks"]]
            syms_b = [s["symbol"] for s in result_b["stocks"]]
            assert "ATW" in syms_a and "BCP" not in syms_a
            assert "BCP" in syms_b and "ATW" not in syms_b
        finally:
            os.unlink(db)


# ---------------------------------------------------------------------------
# list_watchlists
# ---------------------------------------------------------------------------


class TestListWatchlists:
    def test_empty_db_returns_empty_list(self):
        db = make_temp_db()
        try:
            assert list_watchlists(db, owner=USER_A) == []
        finally:
            os.unlink(db)

    def test_returns_correct_count(self):
        db = make_temp_db()
        try:
            create_watchlist("a", ["ATW"], db, owner=USER_A)
            create_watchlist("b", ["IAM", "BCP"], db, owner=USER_A)
            result = list_watchlists(db, owner=USER_A)
            assert len(result) == 2
        finally:
            os.unlink(db)

    def test_stock_count_in_result(self):
        db = make_temp_db()
        try:
            create_watchlist("one", ["ATW", "IAM"], db, owner=USER_A)
            result = list_watchlists(db, owner=USER_A)
            assert result[0]["stock_count"] == 2
        finally:
            os.unlink(db)

    def test_row_has_expected_keys(self):
        db = make_temp_db()
        try:
            create_watchlist("keys", ["ATW"], db, owner=USER_A)
            result = list_watchlists(db, owner=USER_A)
            assert {"id", "name", "created_at", "stock_count"} == set(result[0].keys())
        finally:
            os.unlink(db)

    # --- isolation ---

    def test_returns_only_own_watchlists(self):
        db = make_temp_db()
        try:
            create_watchlist("w1", ["ATW"], db, owner=USER_A)
            create_watchlist("w2", ["IAM"], db, owner=USER_B)
            result_a = list_watchlists(db, owner=USER_A)
            result_b = list_watchlists(db, owner=USER_B)
            assert len(result_a) == 1 and result_a[0]["name"] == "w1"
            assert len(result_b) == 1 and result_b[0]["name"] == "w2"
        finally:
            os.unlink(db)


# ---------------------------------------------------------------------------
# add_to_watchlist
# ---------------------------------------------------------------------------


class TestAddToWatchlist:
    def test_adds_symbol_successfully(self):
        db = make_temp_db()
        try:
            create_watchlist("add_test", ["ATW"], db, owner=USER_A)
            result = add_to_watchlist("add_test", "IAM", db, owner=USER_A)
            assert result.get("success") is True
        finally:
            os.unlink(db)

    def test_error_for_unknown_watchlist(self):
        db = make_temp_db()
        try:
            result = add_to_watchlist("nope", "ATW", db, owner=USER_A)
            assert "error" in result
        finally:
            os.unlink(db)

    def test_error_for_duplicate_symbol(self):
        db = make_temp_db()
        try:
            create_watchlist("dup", ["ATW"], db, owner=USER_A)
            result = add_to_watchlist("dup", "ATW", db, owner=USER_A)
            assert "error" in result
        finally:
            os.unlink(db)

    def test_symbol_appears_in_watchlist_after_add(self):
        db = make_temp_db()
        try:
            create_watchlist("grow", ["ATW"], db, owner=USER_A)
            add_to_watchlist("grow", "BCP", db, owner=USER_A)
            symbols = get_watchlist_symbols("grow", db, owner=USER_A)
            assert "BCP" in symbols
        finally:
            os.unlink(db)

    def test_symbol_normalized_to_uppercase(self):
        db = make_temp_db()
        try:
            create_watchlist("norm", ["ATW"], db, owner=USER_A)
            result = add_to_watchlist("norm", "iam", db, owner=USER_A)
            assert result.get("symbol") == "IAM"
        finally:
            os.unlink(db)

    def test_cannot_add_to_another_users_watchlist(self):
        db = make_temp_db()
        try:
            create_watchlist("private", ["ATW"], db, owner=USER_B)
            result = add_to_watchlist("private", "IAM", db, owner=USER_A)
            assert "error" in result
        finally:
            os.unlink(db)


# ---------------------------------------------------------------------------
# remove_from_watchlist
# ---------------------------------------------------------------------------


class TestRemoveFromWatchlist:
    def test_removes_symbol_successfully(self):
        db = make_temp_db()
        try:
            create_watchlist("rm", ["ATW", "IAM"], db, owner=USER_A)
            result = remove_from_watchlist("rm", "IAM", db, owner=USER_A)
            assert result.get("success") is True
        finally:
            os.unlink(db)

    def test_symbol_gone_after_removal(self):
        db = make_temp_db()
        try:
            create_watchlist("gone", ["ATW", "IAM"], db, owner=USER_A)
            remove_from_watchlist("gone", "IAM", db, owner=USER_A)
            symbols = get_watchlist_symbols("gone", db, owner=USER_A)
            assert "IAM" not in symbols
        finally:
            os.unlink(db)

    def test_error_for_unknown_watchlist(self):
        db = make_temp_db()
        try:
            result = remove_from_watchlist("nope", "ATW", db, owner=USER_A)
            assert "error" in result
        finally:
            os.unlink(db)

    def test_error_for_symbol_not_in_watchlist(self):
        db = make_temp_db()
        try:
            create_watchlist("missing", ["ATW"], db, owner=USER_A)
            result = remove_from_watchlist("missing", "XYZ", db, owner=USER_A)
            assert "error" in result
        finally:
            os.unlink(db)

    def test_cannot_remove_from_another_users_watchlist(self):
        db = make_temp_db()
        try:
            create_watchlist("other", ["ATW"], db, owner=USER_B)
            result = remove_from_watchlist("other", "ATW", db, owner=USER_A)
            assert "error" in result
        finally:
            os.unlink(db)


# ---------------------------------------------------------------------------
# delete_watchlist
# ---------------------------------------------------------------------------


class TestDeleteWatchlist:
    def test_deletes_successfully(self):
        db = make_temp_db()
        try:
            create_watchlist("del", ["ATW"], db, owner=USER_A)
            result = delete_watchlist("del", db, owner=USER_A)
            assert result.get("success") is True
        finally:
            os.unlink(db)

    def test_watchlist_gone_after_delete(self):
        db = make_temp_db()
        try:
            create_watchlist("gone2", ["ATW"], db, owner=USER_A)
            delete_watchlist("gone2", db, owner=USER_A)
            assert get_watchlist("gone2", db, owner=USER_A) is None
        finally:
            os.unlink(db)

    def test_symbols_also_deleted(self):
        db = make_temp_db()
        try:
            create_watchlist("cascade", ["ATW", "IAM"], db, owner=USER_A)
            delete_watchlist("cascade", db, owner=USER_A)
            symbols = get_watchlist_symbols("cascade", db, owner=USER_A)
            assert symbols == []
        finally:
            os.unlink(db)

    def test_error_for_unknown_watchlist(self):
        db = make_temp_db()
        try:
            result = delete_watchlist("nope", db, owner=USER_A)
            assert "error" in result
        finally:
            os.unlink(db)

    def test_list_shrinks_after_delete(self):
        db = make_temp_db()
        try:
            create_watchlist("x", ["ATW"], db, owner=USER_A)
            create_watchlist("y", ["IAM"], db, owner=USER_A)
            delete_watchlist("x", db, owner=USER_A)
            remaining = list_watchlists(db, owner=USER_A)
            assert len(remaining) == 1
            assert remaining[0]["name"] == "y"
        finally:
            os.unlink(db)

    def test_cannot_delete_another_users_watchlist(self):
        db = make_temp_db()
        try:
            create_watchlist("secret", ["ATW"], db, owner=USER_B)
            result = delete_watchlist("secret", db, owner=USER_A)
            assert "error" in result
            # Still exists for USER_B
            assert get_watchlist("secret", db, owner=USER_B) is not None
        finally:
            os.unlink(db)

    def test_deleting_one_user_watchlist_does_not_affect_other(self):
        db = make_temp_db()
        try:
            create_watchlist("shared_name", ["ATW"], db, owner=USER_A)
            create_watchlist("shared_name", ["BCP"], db, owner=USER_B)
            delete_watchlist("shared_name", db, owner=USER_A)
            assert get_watchlist("shared_name", db, owner=USER_A) is None
            assert get_watchlist("shared_name", db, owner=USER_B) is not None
        finally:
            os.unlink(db)


# ---------------------------------------------------------------------------
# get_watchlist_symbols
# ---------------------------------------------------------------------------


class TestGetWatchlistSymbols:
    def test_returns_empty_for_unknown(self):
        db = make_temp_db()
        try:
            assert get_watchlist_symbols("nope", db, owner=USER_A) == []
        finally:
            os.unlink(db)

    def test_returns_correct_symbols(self):
        db = make_temp_db()
        try:
            create_watchlist("syms", ["ATW", "IAM", "BCP"], db, owner=USER_A)
            symbols = get_watchlist_symbols("syms", db, owner=USER_A)
            assert set(symbols) == {"ATW", "IAM", "BCP"}
        finally:
            os.unlink(db)

    def test_returns_list_of_strings(self):
        db = make_temp_db()
        try:
            create_watchlist("str_test", ["ATW"], db, owner=USER_A)
            symbols = get_watchlist_symbols("str_test", db, owner=USER_A)
            assert isinstance(symbols, list)
            assert all(isinstance(s, str) for s in symbols)
        finally:
            os.unlink(db)

    def test_returns_empty_for_other_owners_list(self):
        db = make_temp_db()
        try:
            create_watchlist("mylist", ["ATW"], db, owner=USER_B)
            symbols = get_watchlist_symbols("mylist", db, owner=USER_A)
            assert symbols == []
        finally:
            os.unlink(db)
