"""
mallorcaeat — API key rotation helper

Reads comma-separated key lists from environment variables.
Rotates to the next key on 429 rate-limit responses.

Usage:
    from keys import KeyRotator

    rotator = KeyRotator.from_env("SERPER_API_KEYS", "SERPER_API_KEY")

    # In your request loop:
    params["api_key"] = rotator.current()
    resp = session.get(url, params=params)
    if resp.status_code == 429:
        rotator.rotate()
        if rotator.all_exhausted():
            time.sleep(60)  # full backoff
            rotator.reset()
"""

import logging
import os

logger = logging.getLogger(__name__)


class KeyRotator:
    """Round-robin key pool with 429-triggered rotation."""

    def __init__(self, keys: list[str]):
        if not keys:
            raise ValueError("KeyRotator requires at least one API key")
        self._keys = keys
        self._index = 0
        self._exhausted_count = 0

    @classmethod
    def from_env(cls, plural_var: str, singular_var: str | None = None) -> "KeyRotator":
        """
        Load keys from environment.
        Tries `plural_var` first (comma-separated), falls back to `singular_var`.

        Example:
            KeyRotator.from_env("SERPER_API_KEYS", "SERPER_API_KEY")
        """
        raw = os.environ.get(plural_var, "").strip()
        if raw:
            keys = [k.strip() for k in raw.split(",") if k.strip()]
            if keys:
                logger.info("KeyRotator[%s]: %d key(s) loaded", plural_var, len(keys))
                return cls(keys)

        if singular_var:
            key = os.environ.get(singular_var, "").strip()
            if key:
                logger.info("KeyRotator[%s]: 1 key loaded (singular fallback)", singular_var)
                return cls([key])

        raise EnvironmentError(
            f"No API keys found. Set {plural_var} (comma-separated) "
            + (f"or {singular_var}" if singular_var else "")
        )

    def current(self) -> str:
        """Return the currently active key."""
        return self._keys[self._index]

    def rotate(self) -> bool:
        """
        Advance to the next key. Returns True if a fresh key is available,
        False if we've wrapped back to the start (all keys tried once).
        """
        next_index = (self._index + 1) % len(self._keys)
        self._exhausted_count += 1

        if self._exhausted_count >= len(self._keys):
            # All keys tried — do NOT advance further, signal exhaustion
            return False

        self._index = next_index
        logger.warning(
            "KeyRotator: rotated to key %d/%d after 429",
            self._index + 1, len(self._keys),
        )
        return True

    def all_exhausted(self) -> bool:
        """True when every key has seen a 429 in this cycle."""
        return self._exhausted_count >= len(self._keys)

    def reset(self):
        """Reset the exhaustion counter (call after a cooldown sleep)."""
        self._exhausted_count = 0
        logger.debug("KeyRotator: reset exhaustion counter")

    def __len__(self) -> int:
        return len(self._keys)
