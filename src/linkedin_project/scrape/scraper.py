"""
LinkedIn profile scraper.

This module provides functions to collect LinkedIn profile data using the
``linkedin-api`` library (unofficial internal LinkedIn API) with a structured
fallback interface.  Because the library re-uses LinkedIn's own mobile-app
endpoints, **no official LinkedIn API key is required**, but authentication
via username and password is mandatory.

Authentication credentials are read from environment variables:

    LINKEDIN_USERNAME   — LinkedIn account email address
    LINKEDIN_PASSWORD   — LinkedIn account password
    LINKEDIN_2FA_SECRET — (optional) TOTP secret if 2FA is enabled

All public functions return ``pandas.DataFrame`` objects that conform to the
schemas defined in :mod:`linkedin_project.scrape.schema`.  The caller (e.g. a
pipeline script) is responsible for caching raw responses to ``data/raw/`` and
passing the parsed DataFrames to the transform layer.

Delay between API calls is enforced via ``CALL_DELAY_SECONDS`` to reduce the
risk of rate-limiting.  All field accesses on the raw API response are wrapped
with ``_safe_get`` so that missing or renamed fields produce ``None`` rather
than raising ``KeyError``.

Raises
------
AuthError
    LinkedIn login failed (wrong credentials or 2FA challenge without secret).
ScraperError
    An API call failed after the configured number of retries.
SchemaError
    The API response structure deviated from what the parser expected in a way
    that cannot be recovered from (e.g. a required top-level key is absent).
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd

from linkedin_project.scrape.schema import (
    CERTIFICATIONS_SCHEMA,
    EDUCATION_SCHEMA,
    EXPERIENCE_SCHEMA,
    PROFILE_SCHEMA,
    RECOMMENDATIONS_SCHEMA,
    SKILLS_SCHEMA,
    empty_certifications_df,
    empty_education_df,
    empty_experience_df,
    empty_recommendations_df,
    empty_skills_df,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Seconds to sleep between consecutive API calls to avoid rate limiting.
CALL_DELAY_SECONDS: float = 1.5

#: Maximum number of retry attempts for a single API call.
MAX_RETRIES: int = 3

#: Base delay (seconds) for exponential backoff on retries.
RETRY_BASE_DELAY: float = 2.0


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class AuthError(Exception):
    """Raised when LinkedIn authentication fails."""


class ScraperError(Exception):
    """Raised when an API call fails after all retry attempts."""


class SchemaError(Exception):
    """Raised when the API response deviates from the expected structure."""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _now_utc() -> str:
    """Return the current UTC time as an ISO 8601 string.

    Returns
    -------
    str
        e.g. ``"2026-03-08T12:00:00+00:00"``
    """
    return datetime.now(tz=timezone.utc).isoformat()


def _safe_get(obj: Any, *keys: str, default: Optional[str] = None) -> Optional[Any]:
    """Safely traverse a nested dict/list structure.

    Parameters
    ----------
    obj:
        The root object to traverse.
    *keys:
        Sequence of dict keys or list indices (as strings cast to int) to
        traverse in order.
    default:
        Value to return if any key is missing or the object is not a dict/list.

    Returns
    -------
    Any or None
        The value at the end of the key path, or ``default``.
    """
    current = obj
    for key in keys:
        if current is None:
            return default
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list):
            try:
                current = current[int(key)]
            except (IndexError, ValueError):
                return default
        else:
            return default
    return current if current is not None else default


def _parse_date(year: Optional[int], month: Optional[int] = None) -> Optional[str]:
    """Convert year/month integers from the LinkedIn API to an ISO 8601 date string.

    Parameters
    ----------
    year:
        Four-digit year integer, or ``None``.
    month:
        Month integer (1–12), or ``None`` (defaults to ``1``).

    Returns
    -------
    str or None
        ISO date string ``"YYYY-MM-01"`` or ``None`` if ``year`` is ``None``.
    """
    if year is None:
        return None
    m = month if month is not None else 1
    try:
        return f"{year:04d}-{m:02d}-01"
    except (TypeError, ValueError):
        return None


def _retry_call(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """Call ``fn(*args, **kwargs)`` with exponential back-off on failure.

    Parameters
    ----------
    fn:
        Callable to invoke.
    *args:
        Positional arguments forwarded to ``fn``.
    **kwargs:
        Keyword arguments forwarded to ``fn``.

    Returns
    -------
    Any
        The return value of ``fn``.

    Raises
    ------
    ScraperError
        If ``fn`` raises an exception on every attempt.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY**attempt
                logger.warning(
                    "API call %s failed (attempt %d/%d): %s. Retrying in %.1fs.",
                    getattr(fn, "__name__", repr(fn)),
                    attempt,
                    MAX_RETRIES,
                    exc,
                    delay,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "API call %s failed after %d attempts: %s",
                    getattr(fn, "__name__", repr(fn)),
                    MAX_RETRIES,
                    exc,
                )
    raise ScraperError(f"API call failed after {MAX_RETRIES} attempts") from last_exc


# ---------------------------------------------------------------------------
# LinkedIn API client wrapper
# ---------------------------------------------------------------------------


@dataclass
class LinkedInClient:
    """Thin wrapper around the ``linkedin_api.Linkedin`` client.

    The underlying ``linkedin_api`` library is imported lazily inside methods
    so that the module can be imported (and tested) without the library being
    installed in environments where only mock objects are used.

    Parameters
    ----------
    username:
        LinkedIn account email.  Defaults to the ``LINKEDIN_USERNAME``
        environment variable.
    password:
        LinkedIn account password.  Defaults to the ``LINKEDIN_PASSWORD``
        environment variable.
    _client:
        Pre-built client object injected for testing.  When provided,
        ``username`` and ``password`` are ignored.
    """

    username: str = field(
        default_factory=lambda: os.environ.get("LINKEDIN_USERNAME", "")
    )
    password: str = field(
        default_factory=lambda: os.environ.get("LINKEDIN_PASSWORD", "")
    )
    _client: Optional[Any] = field(default=None, repr=False)

    def _get_client(self) -> Any:
        """Return the underlying ``linkedin_api.Linkedin`` instance.

        Returns
        -------
        linkedin_api.Linkedin
            Authenticated client.

        Raises
        ------
        AuthError
            If credentials are missing or login fails.
        """
        if self._client is not None:
            return self._client

        if not self.username or not self.password:
            raise AuthError(
                "LinkedIn credentials not set. "
                "Set LINKEDIN_USERNAME and LINKEDIN_PASSWORD environment variables."
            )

        try:
            from linkedin_api import Linkedin  # type: ignore[import]

            self._client = Linkedin(self.username, self.password)
            logger.info("Successfully authenticated with LinkedIn as %s", self.username)
            return self._client
        except Exception as exc:
            raise AuthError(
                f"LinkedIn login failed for {self.username!r}. "
                "Check credentials or 2FA settings."
            ) from exc

    def get_profile_raw(self, public_id: str) -> dict[str, Any]:
        """Fetch the raw profile dict from the LinkedIn API.

        Parameters
        ----------
        public_id:
            The LinkedIn public profile identifier (the slug in the URL, e.g.
            ``"john-doe-123"``).

        Returns
        -------
        dict
            Raw response dict from ``linkedin_api``.

        Raises
        ------
        ScraperError
            If the API call fails after all retries.
        AuthError
            If authentication fails.
        """
        client = self._get_client()
        return _retry_call(client.get_profile, public_id)

    def get_skills_raw(self, public_id: str) -> list[dict[str, Any]]:
        """Fetch the raw skills list from the LinkedIn API.

        Parameters
        ----------
        public_id:
            LinkedIn public profile identifier.

        Returns
        -------
        list of dict
            Raw skills response.
        """
        client = self._get_client()
        return _retry_call(client.get_profile_skills, public_id)

    def get_certifications_raw(self, public_id: str) -> list[dict[str, Any]]:
        """Fetch the raw certifications list from the LinkedIn API.

        Parameters
        ----------
        public_id:
            LinkedIn public profile identifier.

        Returns
        -------
        list of dict
            Raw certifications response.
        """
        client = self._get_client()
        return _retry_call(client.get_profile_certifications, public_id)

    def get_recommendations_raw(self, public_id: str) -> dict[str, Any]:
        """Fetch the raw recommendations dict from the LinkedIn API.

        Parameters
        ----------
        public_id:
            LinkedIn public profile identifier.

        Returns
        -------
        dict
            Raw recommendations response.
        """
        client = self._get_client()
        return _retry_call(client.get_profile_recommendations, public_id)

    def get_contact_info_raw(self, public_id: str) -> dict[str, Any]:
        """Fetch the raw contact info dict from the LinkedIn API.

        Parameters
        ----------
        public_id:
            LinkedIn public profile identifier.

        Returns
        -------
        dict
            Raw contact info response.
        """
        client = self._get_client()
        return _retry_call(client.get_profile_contact_info, public_id)


# ---------------------------------------------------------------------------
# Parsers — convert raw API dicts to DataFrames
# ---------------------------------------------------------------------------


def parse_profile(raw: dict[str, Any], public_id: str) -> pd.DataFrame:
    """Parse the raw profile API response into a profile DataFrame.

    Parameters
    ----------
    raw:
        Raw dict returned by ``linkedin_api.Linkedin.get_profile``.
    public_id:
        The LinkedIn public profile slug used for the API call.

    Returns
    -------
    pd.DataFrame
        Single-row DataFrame conforming to :data:`~schema.PROFILE_SCHEMA`.
    """
    scraped_at = _now_utc()

    # linkedin-api returns picture root + artifacts; build a usable URL
    picture_root: Optional[str] = _safe_get(raw, "displayPictureUrl")
    picture_suffix: Optional[str] = _safe_get(raw, "img_800_800") or _safe_get(
        raw, "img_400_400"
    )
    if picture_root and picture_suffix:
        profile_picture_url: Optional[str] = picture_root + picture_suffix
    else:
        profile_picture_url = picture_root

    connection_count: Optional[int] = (  # type: ignore[assignment]
        _safe_get(raw, "connections", "total")
    )

    row: dict[str, Any] = {
        "profile_id": public_id,
        "full_name": " ".join(
            filter(
                None,
                [
                    _safe_get(raw, "firstName"),
                    _safe_get(raw, "lastName"),
                ],
            )
        )
        or None,
        "headline": _safe_get(raw, "headline"),
        "summary": _safe_get(raw, "summary"),
        "location": _safe_get(raw, "geoLocationName"),
        "industry": _safe_get(raw, "industryName"),
        "profile_picture_url": profile_picture_url,
        "connection_count": connection_count,
        "scraped_at": scraped_at,
    }

    df = pd.DataFrame([row])
    # Cast to schema dtypes
    for col, dtype in PROFILE_SCHEMA.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    return df[list(PROFILE_SCHEMA.keys())]


def parse_experience(raw: dict[str, Any], public_id: str) -> pd.DataFrame:
    """Parse experience entries from the raw profile API response.

    Parameters
    ----------
    raw:
        Raw dict returned by ``linkedin_api.Linkedin.get_profile``.
        Experience entries live under the ``"experience"`` key.
    public_id:
        LinkedIn public profile slug.

    Returns
    -------
    pd.DataFrame
        DataFrame conforming to :data:`~schema.EXPERIENCE_SCHEMA`.
        Returns an empty DataFrame if no experience entries are present.
    """
    experiences: list[dict[str, Any]] = raw.get("experience", []) or []
    if not experiences:
        logger.info("No experience entries found for %s", public_id)
        return empty_experience_df()

    scraped_at = _now_utc()
    rows: list[dict[str, Any]] = []

    for exp in experiences:
        time_period = exp.get("timePeriod") or {}
        start = time_period.get("startDate") or {}
        end = time_period.get("endDate")

        start_date = _parse_date(start.get("year"), start.get("month"))
        end_date: Optional[str] = None
        is_current = True
        if end:
            end_date = _parse_date(end.get("year"), end.get("month"))
            is_current = False

        company_url: Optional[str] = _safe_get(
            exp, "company", "miniCompany", "universalName"
        )
        if company_url:
            company_url = f"https://www.linkedin.com/company/{company_url}"

        rows.append(
            {
                "profile_id": public_id,
                "company_name": _safe_get(exp, "companyName"),
                "company_linkedin_url": company_url,
                "company_logo_url": _safe_get(
                    exp, "company", "miniCompany", "logo", "rootUrl"
                ),
                "title": _safe_get(exp, "title"),
                "employment_type": _safe_get(exp, "employmentType"),
                "location": _safe_get(exp, "locationName"),
                "start_date": start_date,
                "end_date": end_date,
                "is_current": is_current,
                "description": _safe_get(exp, "description"),
                "scraped_at": scraped_at,
            }
        )

    df = pd.DataFrame(rows)
    for col, dtype in EXPERIENCE_SCHEMA.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    return df[list(EXPERIENCE_SCHEMA.keys())]


def parse_education(raw: dict[str, Any], public_id: str) -> pd.DataFrame:
    """Parse education entries from the raw profile API response.

    Parameters
    ----------
    raw:
        Raw dict returned by ``linkedin_api.Linkedin.get_profile``.
        Education entries live under the ``"education"`` key.
    public_id:
        LinkedIn public profile slug.

    Returns
    -------
    pd.DataFrame
        DataFrame conforming to :data:`~schema.EDUCATION_SCHEMA`.
    """
    education_list: list[dict[str, Any]] = raw.get("education", []) or []
    if not education_list:
        logger.info("No education entries found for %s", public_id)
        return empty_education_df()

    scraped_at = _now_utc()
    rows: list[dict[str, Any]] = []

    for edu in education_list:
        time_period = edu.get("timePeriod") or {}
        start = time_period.get("startDate") or {}
        end = time_period.get("endDate") or {}

        school_url: Optional[str] = _safe_get(edu, "school", "universalName")
        if school_url:
            school_url = f"https://www.linkedin.com/school/{school_url}"

        rows.append(
            {
                "profile_id": public_id,
                "school_name": _safe_get(edu, "schoolName"),
                "school_linkedin_url": school_url,
                "degree": _safe_get(edu, "degreeName"),
                "field_of_study": _safe_get(edu, "fieldOfStudy"),
                "start_date": _parse_date(start.get("year"), start.get("month")),
                "end_date": _parse_date(end.get("year"), end.get("month")),
                "grade": _safe_get(edu, "grade"),
                "activities": _safe_get(edu, "activities"),
                "description": _safe_get(edu, "description"),
                "scraped_at": scraped_at,
            }
        )

    df = pd.DataFrame(rows)
    for col, dtype in EDUCATION_SCHEMA.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    return df[list(EDUCATION_SCHEMA.keys())]


def parse_skills(raw_skills: list[dict[str, Any]], public_id: str) -> pd.DataFrame:
    """Parse the raw skills API response into a skills DataFrame.

    Parameters
    ----------
    raw_skills:
        Raw list returned by ``linkedin_api.Linkedin.get_profile_skills``.
    public_id:
        LinkedIn public profile slug.

    Returns
    -------
    pd.DataFrame
        DataFrame conforming to :data:`~schema.SKILLS_SCHEMA`.
    """
    if not raw_skills:
        logger.info("No skills found for %s", public_id)
        return empty_skills_df()

    scraped_at = _now_utc()
    rows: list[dict[str, Any]] = []

    for skill in raw_skills:
        rows.append(
            {
                "profile_id": public_id,
                "skill_name": _safe_get(skill, "name"),
                "endorsement_count": _safe_get(skill, "endorsementCount"),
                "scraped_at": scraped_at,
            }
        )

    df = pd.DataFrame(rows)
    for col, dtype in SKILLS_SCHEMA.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    return df[list(SKILLS_SCHEMA.keys())]


def parse_certifications(
    raw_certs: list[dict[str, Any]], public_id: str
) -> pd.DataFrame:
    """Parse the raw certifications API response into a certifications DataFrame.

    Parameters
    ----------
    raw_certs:
        Raw list returned by ``linkedin_api.Linkedin.get_profile_certifications``.
    public_id:
        LinkedIn public profile slug.

    Returns
    -------
    pd.DataFrame
        DataFrame conforming to :data:`~schema.CERTIFICATIONS_SCHEMA`.
    """
    if not raw_certs:
        logger.info("No certifications found for %s", public_id)
        return empty_certifications_df()

    scraped_at = _now_utc()
    rows: list[dict[str, Any]] = []

    for cert in raw_certs:
        issued_date_raw = cert.get("timePeriod", {}).get("startDate") or {}
        expiry_date_raw = cert.get("timePeriod", {}).get("endDate") or {}

        rows.append(
            {
                "profile_id": public_id,
                "cert_name": _safe_get(cert, "name"),
                "authority": _safe_get(cert, "authority"),
                "issued_date": _parse_date(
                    issued_date_raw.get("year"), issued_date_raw.get("month")
                ),
                "expiry_date": _parse_date(
                    expiry_date_raw.get("year"), expiry_date_raw.get("month")
                ),
                "credential_id": _safe_get(cert, "licenseNumber"),
                "credential_url": _safe_get(cert, "url"),
                "scraped_at": scraped_at,
            }
        )

    df = pd.DataFrame(rows)
    for col, dtype in CERTIFICATIONS_SCHEMA.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    return df[list(CERTIFICATIONS_SCHEMA.keys())]


def parse_recommendations(raw_recs: dict[str, Any], public_id: str) -> pd.DataFrame:
    """Parse the raw recommendations API response into a recommendations DataFrame.

    Parameters
    ----------
    raw_recs:
        Raw dict returned by ``linkedin_api.Linkedin.get_profile_recommendations``.
        Received recommendations live under the ``"receivedRecommendations"`` key.
    public_id:
        LinkedIn public profile slug.

    Returns
    -------
    pd.DataFrame
        DataFrame conforming to :data:`~schema.RECOMMENDATIONS_SCHEMA`.
    """
    received: list[dict[str, Any]] = raw_recs.get("receivedRecommendations", []) or []
    if not received:
        logger.info("No received recommendations found for %s", public_id)
        return empty_recommendations_df()

    scraped_at = _now_utc()
    rows: list[dict[str, Any]] = []

    for rec in received:
        recommender = rec.get("recommender") or {}
        date_raw = rec.get("creationDate") or {}

        rows.append(
            {
                "profile_id": public_id,
                "recommender_name": " ".join(
                    filter(
                        None,
                        [
                            _safe_get(recommender, "firstName"),
                            _safe_get(recommender, "lastName"),
                        ],
                    )
                )
                or None,
                "recommender_title": _safe_get(recommender, "occupation"),
                "relationship": _safe_get(rec, "relationshipType"),
                "recommendation_text": _safe_get(rec, "recommendationText"),
                "recommendation_date": _parse_date(
                    date_raw.get("year"), date_raw.get("month")
                ),
                "scraped_at": scraped_at,
            }
        )

    df = pd.DataFrame(rows)
    for col, dtype in RECOMMENDATIONS_SCHEMA.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    return df[list(RECOMMENDATIONS_SCHEMA.keys())]


# ---------------------------------------------------------------------------
# High-level scrape function
# ---------------------------------------------------------------------------


@dataclass
class ProfileData:
    """Container for all scraped LinkedIn profile DataFrames.

    Attributes
    ----------
    profile:
        Single-row DataFrame with top-level profile fields.
    experience:
        One row per work experience entry.
    education:
        One row per education entry.
    skills:
        One row per skill.
    certifications:
        One row per certification.
    recommendations:
        One row per received recommendation.
    """

    profile: pd.DataFrame
    experience: pd.DataFrame
    education: pd.DataFrame
    skills: pd.DataFrame
    certifications: pd.DataFrame
    recommendations: pd.DataFrame


def scrape_profile(
    public_id: str,
    client: Optional[LinkedInClient] = None,
) -> ProfileData:
    """Scrape all sections of a LinkedIn profile and return structured DataFrames.

    This is the primary entry point for the scraping layer.  It fetches all
    profile sections, parses them into DataFrames conforming to the schemas in
    :mod:`linkedin_project.scrape.schema`, and returns them as a
    :class:`ProfileData` container.

    A :class:`LinkedInClient` is created automatically if not provided,
    reading credentials from environment variables ``LINKEDIN_USERNAME`` and
    ``LINKEDIN_PASSWORD``.

    Parameters
    ----------
    public_id:
        LinkedIn public profile identifier (the slug in the profile URL, e.g.
        ``"john-doe-123"``).
    client:
        Optional pre-built :class:`LinkedInClient`.  Useful for testing by
        injecting a mock client.

    Returns
    -------
    ProfileData
        Container with DataFrames for each profile section.

    Raises
    ------
    AuthError
        If LinkedIn authentication fails.
    ScraperError
        If any API call fails after all retry attempts.

    Examples
    --------
    >>> from linkedin_project.scrape.scraper import scrape_profile
    >>> data = scrape_profile("john-doe-123")
    >>> data.profile.shape
    (1, 9)
    """
    if client is None:
        client = LinkedInClient()

    logger.info("Starting full profile scrape for public_id=%r", public_id)

    # --- Profile (experience and education are embedded in this response) ---
    raw_profile = client.get_profile_raw(public_id)
    time.sleep(CALL_DELAY_SECONDS)

    profile_df = parse_profile(raw_profile, public_id)
    experience_df = parse_experience(raw_profile, public_id)
    education_df = parse_education(raw_profile, public_id)

    # --- Skills (separate API call) ---
    raw_skills = client.get_skills_raw(public_id)
    time.sleep(CALL_DELAY_SECONDS)
    skills_df = parse_skills(raw_skills, public_id)

    # --- Certifications (separate API call) ---
    raw_certs = client.get_certifications_raw(public_id)
    time.sleep(CALL_DELAY_SECONDS)
    certs_df = parse_certifications(raw_certs, public_id)

    # --- Recommendations (separate API call) ---
    raw_recs = client.get_recommendations_raw(public_id)
    time.sleep(CALL_DELAY_SECONDS)
    recs_df = parse_recommendations(raw_recs, public_id)

    logger.info(
        "Scrape complete for %r: %d experience, %d education, %d skills, "
        "%d certifications, %d recommendations",
        public_id,
        len(experience_df),
        len(education_df),
        len(skills_df),
        len(certs_df),
        len(recs_df),
    )

    return ProfileData(
        profile=profile_df,
        experience=experience_df,
        education=education_df,
        skills=skills_df,
        certifications=certs_df,
        recommendations=recs_df,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    # Exceptions
    "AuthError",
    "ScraperError",
    "SchemaError",
    # Client
    "LinkedInClient",
    # Parsers
    "parse_profile",
    "parse_experience",
    "parse_education",
    "parse_skills",
    "parse_certifications",
    "parse_recommendations",
    # High-level
    "scrape_profile",
    "ProfileData",
    # Constants
    "CALL_DELAY_SECONDS",
    "MAX_RETRIES",
]
