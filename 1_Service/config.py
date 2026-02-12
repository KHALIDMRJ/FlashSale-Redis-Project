"""
config.py
=========
Central configuration for Flash Sale Redis project (No API, Redis-only service).

Features:
- Reads configuration from environment variables (best practice)
- Supports Redis ACL auth (username + password) OR password-only
- Provides defaults for local Docker Redis usage
- Validates configuration and prints a safe summary (no secrets)

Environment variables:
- REDIS_HOST        (default: 127.0.0.1)
- REDIS_PORT        (default: 6379)
- REDIS_DB          (default: 0)
- REDIS_USERNAME    (optional, for Redis ACL)
- REDIS_PASSWORD    (optional, for auth)
- RES_TTL           (default: 300)  reservation TTL seconds
- SWEEPER_INTERVAL  (default: 10)   sweeper interval seconds
- PAYMENT_LOCK_TTL  (default: 2592000) 30 days, idempotence lock TTL
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise ValueError(f"Invalid integer for {name}='{raw}'") from e


def _get_str(name: str, default: Optional[str] = None) -> Optional[str]:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    return raw if raw else default


@dataclass(frozen=True)
class Settings:
    # Redis connection
    redis_host: str = _get_str("REDIS_HOST", "127.0.0.1") or "127.0.0.1"
    redis_port: int = _get_int("REDIS_PORT", 6379)
    redis_db: int = _get_int("REDIS_DB", 0)

    # Redis auth (ACL)
    redis_username: Optional[str] = _get_str("REDIS_USERNAME", None)
    redis_password: Optional[str] = _get_str("REDIS_PASSWORD", None)

    # Business timings
    reservation_ttl: int = _get_int("RES_TTL", 300)  # default 5 min
    sweeper_interval: int = _get_int("SWEEPER_INTERVAL", 10)

    # Anti double-payment idempotence lock TTL
    payment_lock_ttl: int = _get_int("PAYMENT_LOCK_TTL", 30 * 24 * 60 * 60)

    def validate(self) -> None:
        """
        Basic sanity checks to fail fast with clear messages.
        """
        if not (1 <= self.redis_port <= 65535):
            raise ValueError(f"REDIS_PORT out of range: {self.redis_port}")

        if self.redis_db < 0:
            raise ValueError(f"REDIS_DB must be >= 0, got {self.redis_db}")

        if self.reservation_ttl <= 0:
            raise ValueError(f"RES_TTL must be > 0, got {self.reservation_ttl}")

        if self.sweeper_interval <= 0:
            raise ValueError(f"SWEEPER_INTERVAL must be > 0, got {self.sweeper_interval}")

        if self.payment_lock_ttl <= 0:
            raise ValueError(f"PAYMENT_LOCK_TTL must be > 0, got {self.payment_lock_ttl}")

        # If username provided but password missing -> usually misconfig
        if self.redis_username and not self.redis_password:
            raise ValueError("REDIS_USERNAME is set but REDIS_PASSWORD is missing.")

        # If password is set but username isn't, that's OK for password-only Redis.
        # If both missing, that's OK for local unsecured Redis.

    def safe_summary(self) -> str:
        """
        Returns a safe summary string for logs (never prints the password).
        """
        auth_mode = "no-auth"
        if self.redis_password and self.redis_username:
            auth_mode = "acl-auth (username+password)"
        elif self.redis_password:
            auth_mode = "password-auth"

        return (
            "Settings("
            f"redis={self.redis_host}:{self.redis_port}/db{self.redis_db}, "
            f"auth={auth_mode}, "
            f"reservation_ttl={self.reservation_ttl}s, "
            f"sweeper_interval={self.sweeper_interval}s, "
            f"payment_lock_ttl={self.payment_lock_ttl}s"
            ")"
        )


def load_settings() -> Settings:
    """
    Load and validate settings once, used by app.py and redis_client.py.
    """
    s = Settings()
    s.validate()
    return s
