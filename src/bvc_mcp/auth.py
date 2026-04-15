"""
User identity resolution for the BVC MCP server.

Resolves a caller-supplied API key to a short, stable owner identifier that
is safe to store in the database without exposing the raw key.

Priority order for resolve_owner():
  1. api_key argument (non-empty)  → sha256(api_key)[:16]
  2. Authorization token           → sha256(token)[:16]
  3. MCP client id                 → sha256(client_id)[:16]
  4. BVC_USER_ID env var           → value as-is
  5. BVC_API_KEY env var           → sha256(value)[:16]
  6. fallback                      → "default"
"""

from __future__ import annotations

import hashlib
import os
import re

from .config import DEFAULT_OWNER, OWNER_ID_LENGTH

_ENV_USER_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def _hash_key(api_key: str) -> str:
    """Return the first OWNER_ID_LENGTH hex chars of SHA-256(api_key)."""
    return hashlib.sha256(api_key.encode()).hexdigest()[:OWNER_ID_LENGTH]


def _normalize_env_user(env_user: str) -> str:
    """
    Return a safe owner identifier derived from ``BVC_USER_ID``.

    Valid short IDs are kept human-readable for local development.
    Any invalid or overlong value is hashed so that it remains bounded and safe
    to store/log without breaking deployments that already rely on the env var.
    """
    value = env_user.strip()
    if not value:
        return ""
    if _ENV_USER_RE.fullmatch(value):
        return value
    return _hash_key(value)


def _extract_auth_token(authorization: str | None) -> str:
    """Extract a stable token value from an Authorization header."""
    if authorization is None:
        return ""
    value = authorization.strip()
    if not value:
        return ""
    parts = value.split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return value


def resolve_owner_with_source(
    api_key: str | None = None,
    authorization: str | None = None,
    client_id: str | None = None,
) -> tuple[str, str]:
    """
    Resolve the owner identifier and report which identity source was used.

    Returns:
        Tuple ``(owner, source)`` where source is one of:
        ``api_key``, ``authorization``, ``client``, ``env_user``, ``env_api_key``,
        ``default``.
    """
    if api_key is not None:
        cleaned_key = api_key.strip()
        if cleaned_key:
            return _hash_key(cleaned_key), "api_key"

    auth_token = _extract_auth_token(authorization)
    if auth_token:
        return _hash_key(auth_token), "authorization"

    if client_id is not None:
        cleaned_client_id = client_id.strip()
        if cleaned_client_id:
            return _hash_key(cleaned_client_id), "client"

    env_user = _normalize_env_user(os.environ.get("BVC_USER_ID", ""))
    if env_user:
        return env_user, "env_user"

    env_key = os.environ.get("BVC_API_KEY", "").strip()
    if env_key:
        return _hash_key(env_key), "env_api_key"

    return DEFAULT_OWNER, "default"


def resolve_owner(api_key: str | None = None) -> str:
    """
    Resolve the owner identifier from the available context.

    Args:
        api_key: Optional API key passed by the caller (MCP tool parameter).

    Returns:
        A short, stable string suitable for use as ``owner`` in the DB.
        Never returns an empty string — falls back to ``"default"``.
    """
    owner, _source = resolve_owner_with_source(api_key=api_key)
    return owner


def mask_key(api_key: str) -> str:
    """
    Return a masked version of an API key safe for logging.

    Shows only the last 4 characters: ``"****xxxx"``.

    Args:
        api_key: The raw API key string.

    Returns:
        Masked string. If the key is 4 chars or shorter, returns ``"****"``.
    """
    if len(api_key) <= 4:
        return "****"
    return "****" + api_key[-4:]
