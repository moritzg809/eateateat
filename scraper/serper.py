import logging
import os
import time

import requests

from config import (
    RESULTS_PER_CALL,
    SEARCH_COUNTRY,
    SEARCH_LANGUAGE,
    SERPER_MAPS_URL,
)

logger = logging.getLogger(__name__)

_SESSION = requests.Session()


def _headers() -> dict:
    return {
        "X-API-KEY": os.environ["SERPER_API_KEY"],
        "Content-Type": "application/json",
    }


def search_maps(query: str, location: str, retries: int = 3) -> dict:
    """
    Call the Serper /maps endpoint and return the parsed JSON response.
    Raises on unrecoverable errors after `retries` attempts.
    """
    payload = {
        "q": f"{query} {location}",
        "gl": SEARCH_COUNTRY,
        "hl": SEARCH_LANGUAGE,
        "num": RESULTS_PER_CALL,
    }

    for attempt in range(1, retries + 1):
        try:
            resp = _SESSION.post(
                SERPER_MAPS_URL,
                json=payload,
                headers=_headers(),
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            # 429 = rate limit — always retry with backoff
            if resp.status_code == 429 or attempt < retries:
                wait = 2 ** attempt
                logger.warning("HTTP %s – retrying in %ss (attempt %s/%s)",
                               resp.status_code, wait, attempt, retries)
                time.sleep(wait)
            else:
                raise
        except requests.exceptions.RequestException as e:
            if attempt < retries:
                wait = 2 ** attempt
                logger.warning("Request error – retrying in %ss: %s", wait, e)
                time.sleep(wait)
            else:
                raise
