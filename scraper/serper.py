import logging
import time

import requests

from config import (
    RESULTS_PER_CALL,
    SEARCH_COUNTRY,
    SEARCH_LANGUAGE,
    SERPER_MAPS_URL,
)
from keys import KeyRotator

logger = logging.getLogger(__name__)

_SESSION = requests.Session()

# Module-level rotator — initialised lazily so tests can mock env vars
_rotator: KeyRotator | None = None


def _get_rotator() -> KeyRotator:
    global _rotator
    if _rotator is None:
        _rotator = KeyRotator.from_env("SERPER_API_KEYS", "SERPER_API_KEY")
    return _rotator


def search_maps(query: str, location: str, retries: int = 5) -> dict:
    """
    Call the Serper /maps endpoint and return the parsed JSON response.
    Rotates API key on 429. Raises on unrecoverable errors after `retries` attempts.
    """
    rotator = _get_rotator()
    payload = {
        "q": f"{query} {location}",
        "gl": SEARCH_COUNTRY,
        "hl": SEARCH_LANGUAGE,
        "num": RESULTS_PER_CALL,
    }

    for attempt in range(1, retries + 1):
        try:
            headers = {
                "X-API-KEY": rotator.current(),
                "Content-Type": "application/json",
            }
            resp = _SESSION.post(
                SERPER_MAPS_URL,
                json=payload,
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            rotator.reset()  # successful call — reset exhaustion counter
            return resp.json()
        except requests.exceptions.HTTPError:
            if resp.status_code == 429:
                # Try next key first
                if rotator.rotate():
                    logger.warning("429 from Serper – rotated to next key (attempt %d/%d)",
                                   attempt, retries)
                    continue  # retry immediately with new key
                else:
                    # All keys exhausted — back off
                    wait = 30
                    logger.warning("429 from Serper – all keys exhausted, waiting %ss", wait)
                    time.sleep(wait)
                    rotator.reset()
            elif attempt < retries:
                wait = 2 ** attempt
                logger.warning("HTTP %s – retry in %ss (%d/%d)",
                               resp.status_code, wait, attempt, retries)
                time.sleep(wait)
            else:
                raise
        except requests.exceptions.RequestException as e:
            if attempt < retries:
                wait = 2 ** attempt
                logger.warning("Request error – retry in %ss: %s", wait, e)
                time.sleep(wait)
            else:
                raise
