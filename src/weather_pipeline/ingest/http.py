"""Small HTTP client shared by all collectors (retry, backoff, generic user agent)."""

import http.client
import logging
import time
import urllib.error
import urllib.request

log = logging.getLogger(__name__)

USER_AGENT = "weather-bigdata-pipeline/1.0"
RETRYABLE = {408, 429, 500, 502, 503, 504}


def get(url: str, timeout: float = 30, attempts: int = 5) -> bytes:
    for attempt in range(1, attempts + 1):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code not in RETRYABLE or attempt == attempts:
                raise
            reason = f"HTTP {error.code}"
        except (OSError, http.client.HTTPException) as error:
            if attempt == attempts:
                raise
            reason = repr(error)
        delay = min(60, 3 * 2**attempt)
        log.warning("request failed (%s), retrying in %ss", reason, delay)
        time.sleep(delay)
    raise ValueError("attempts must be >= 1")
