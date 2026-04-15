"""
Unit tests for auth.py — resolve_owner() and mask_key().
"""

from __future__ import annotations

import os
from unittest import mock

import pytest

from bvc_mcp.auth import mask_key, resolve_owner, resolve_owner_with_source
from bvc_mcp.config import DEFAULT_OWNER, OWNER_ID_LENGTH


# ---------------------------------------------------------------------------
# resolve_owner
# ---------------------------------------------------------------------------


class TestResolveOwner:
    def test_empty_string_returns_default(self):
        assert resolve_owner("") == DEFAULT_OWNER

    def test_none_returns_default(self):
        assert resolve_owner(None) == DEFAULT_OWNER

    def test_api_key_returns_hex_string(self):
        result = resolve_owner("mykey123")
        assert isinstance(result, str)
        assert len(result) == OWNER_ID_LENGTH
        # Must be valid hex
        int(result, 16)

    def test_api_key_is_deterministic(self):
        assert resolve_owner("mykey123") == resolve_owner("mykey123")

    def test_different_keys_produce_different_owners(self):
        assert resolve_owner("key_alpha") != resolve_owner("key_beta")

    def test_api_key_takes_priority_over_env_vars(self):
        with mock.patch.dict(os.environ, {"BVC_USER_ID": "env_user"}):
            result = resolve_owner("explicit_key")
        assert result != "env_user"
        assert len(result) == OWNER_ID_LENGTH

    def test_bvc_user_id_env_used_when_no_key(self):
        with mock.patch.dict(os.environ, {"BVC_USER_ID": "hamza"}, clear=False):
            result = resolve_owner("")
        assert result == "hamza"

    def test_bvc_user_id_whitespace_is_ignored(self):
        with mock.patch.dict(os.environ, {"BVC_USER_ID": "   "}, clear=False):
            os.environ.pop("BVC_API_KEY", None)
            result = resolve_owner("")
        assert result == DEFAULT_OWNER

    def test_bvc_user_id_invalid_format_is_hashed(self):
        raw = "hamza/admin@example.com"
        with mock.patch.dict(os.environ, {"BVC_USER_ID": raw}, clear=False):
            result = resolve_owner("")
        assert isinstance(result, str)
        assert len(result) == OWNER_ID_LENGTH
        int(result, 16)

    def test_whitespace_only_api_key_falls_back_to_default(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("BVC_USER_ID", None)
            os.environ.pop("BVC_API_KEY", None)
            result = resolve_owner("   ")
        assert result == DEFAULT_OWNER

    def test_bvc_api_key_env_hashed_when_no_user_id(self):
        env = {"BVC_API_KEY": "secret_env_key"}
        # Ensure BVC_USER_ID is absent
        with mock.patch.dict(os.environ, env, clear=False):
            os.environ.pop("BVC_USER_ID", None)
            result = resolve_owner("")
        assert isinstance(result, str)
        assert len(result) == OWNER_ID_LENGTH
        # Should match the hash of the env key
        assert result == resolve_owner("secret_env_key")

    def test_fallback_when_no_key_no_env(self):
        # Remove both env vars if present
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("BVC_USER_ID", None)
            os.environ.pop("BVC_API_KEY", None)
            result = resolve_owner(None)
        assert result == DEFAULT_OWNER

    def test_authorization_header_is_used_when_no_api_key(self):
        owner, source = resolve_owner_with_source(authorization="Bearer abc123")
        assert source == "authorization"
        assert len(owner) == OWNER_ID_LENGTH
        int(owner, 16)

    def test_client_id_is_used_when_no_api_key_or_authorization(self):
        owner, source = resolve_owner_with_source(client_id="chatgpt-client-123")
        assert source == "client"
        assert len(owner) == OWNER_ID_LENGTH
        int(owner, 16)

    def test_api_key_takes_priority_over_authorization_header(self):
        owner, source = resolve_owner_with_source(
            api_key="tool-key",
            authorization="Bearer abc123",
        )
        assert source == "api_key"
        assert owner == resolve_owner("tool-key")

    def test_authorization_takes_priority_over_session_id(self):
        owner, source = resolve_owner_with_source(
            authorization="Bearer abc123",
            client_id="chatgpt-client-123",
        )
        assert source == "authorization"
        assert owner == resolve_owner("abc123")

    def test_client_id_takes_priority_over_env_defaults(self):
        owner, source = resolve_owner_with_source(
            client_id="chatgpt-client-123",
        )
        assert source == "client"
        assert owner != DEFAULT_OWNER

# ---------------------------------------------------------------------------
# mask_key
# ---------------------------------------------------------------------------


class TestMaskKey:
    def test_shows_last_four_chars(self):
        assert mask_key("abcdef1234567890") == "****7890"

    def test_short_key_returns_four_stars(self):
        assert mask_key("abc") == "****"

    def test_exactly_four_chars_returns_four_stars(self):
        assert mask_key("1234") == "****"

    def test_five_char_key(self):
        assert mask_key("12345") == "****2345"

    def test_does_not_reveal_full_key(self):
        key = "supersecretkey99"
        masked = mask_key(key)
        assert key not in masked
        assert masked.startswith("****")
