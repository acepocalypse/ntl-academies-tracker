from __future__ import annotations

from typing import Callable, Optional, Tuple

import pandas as pd
import requests

# Shared HTTP defaults for lightweight profile checks
_DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
_REQUEST_TIMEOUT = 15  # seconds

# Marker phrases that appear on academy 404 pages
_NAE_MISSING_MARKERS = [
    "page you are looking for might have been removed",
    "resource you are looking for has been removed",
    "page cannot be found",
]
_NAM_MISSING_MARKERS = ["page not found"]
_NAS_MISSING_MARKERS = ["page not found"]

VerifierResult = Tuple[Optional[bool], str]
RemovalCheckOutcome = Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]  # confirmed, still_present, errors


def _http_get(url: str) -> Tuple[Optional[requests.Response], Optional[str]]:
    """Tiny helper to fetch a URL with shared headers and timeout."""
    try:
        resp = requests.get(
            url,
            headers=_DEFAULT_HEADERS,
            timeout=_REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        return resp, None
    except requests.RequestException as exc:  # pragma: no cover - network issues
        return None, str(exc)


def _response_indicates_missing(resp: requests.Response, markers: list[str]) -> bool:
    """Return True when status/HTML indicates the profile page is gone."""
    if resp.status_code == 404:
        return True
    text_lower = resp.text.lower()
    return any(marker in text_lower for marker in markers)


def _generic_verifier(url: str, markers: list[str]) -> VerifierResult:
    resp, error = _http_get(url)
    if resp is None:
        return None, f"request_error={error}"

    if _response_indicates_missing(resp, markers):
        return True, f"status={resp.status_code}"

    if resp.status_code == 200:
        return False, "status=200"

    # Unknown state: surface status for logging / manual follow up
    return None, f"status={resp.status_code}"


_AWARD_VERIFIERS: dict[str, Callable[[str], VerifierResult]] = {
    "3008": lambda url: _generic_verifier(url, _NAE_MISSING_MARKERS),
    "1909": lambda url: _generic_verifier(url, _NAM_MISSING_MARKERS),
    "2023": lambda url: _generic_verifier(url, _NAS_MISSING_MARKERS),
}


def verify_removed_rows(
    award_id: str,
    removed_df: Optional[pd.DataFrame],
) -> RemovalCheckOutcome:
    """
    Double-check flagged removals by re-fetching each profile URL.

    Returns tuple: (confirmed_removed, still_present, check_errors)
      - confirmed_removed: rows that appear to be genuinely missing (kept as removals)
      - still_present: rows that still return a live profile (dropped from removals)
      - check_errors: rows where the verification failed (left flagged, but noted)
    """
    if removed_df is None or removed_df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty

    verifier = _AWARD_VERIFIERS.get(award_id)

    confirmed: list[dict] = []
    still_present: list[dict] = []
    errors: list[dict] = []

    for row in removed_df.to_dict(orient="records"):
        row = dict(row)  # work on a copy
        url = row.get("profile_url", "")

        if not url:
            row["double_check_status"] = "no_url"
            confirmed.append(row)
            continue

        if verifier is None:
            row["double_check_status"] = "not_supported"
            confirmed.append(row)
            continue

        verdict, detail = verifier(url)
        row["double_check_detail"] = detail

        if verdict is True:
            row["double_check_status"] = "confirmed_missing"
            confirmed.append(row)
        elif verdict is False:
            row["double_check_status"] = "still_present"
            still_present.append(row)
        else:
            row["double_check_status"] = "check_error"
            errors.append(row)
            confirmed.append(row)  # keep flagged but mark uncertain

    return (
        pd.DataFrame(confirmed),
        pd.DataFrame(still_present),
        pd.DataFrame(errors),
    )
