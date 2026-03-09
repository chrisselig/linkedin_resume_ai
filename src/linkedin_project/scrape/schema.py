"""
Schema definitions for LinkedIn profile scraping output.

This module defines the canonical pandas DataFrame schemas for each LinkedIn
profile section. These schemas act as a contract between the scrape layer and
the transform layer — the transform subagent should expect DataFrames with
exactly these column names and dtypes.

All string fields use ``pd.StringDtype()`` (nullable strings backed by
``pandas.NA``). All numeric fields use ``pd.Int64Dtype()`` (nullable integers)
or ``float`` where fractional values are possible. All datetime fields are
``datetime64[ns]`` with UTC timezone stored as object strings in ISO 8601 format
(``pd.StringDtype()``); the transform layer normalises them to proper timestamps.

Nullable types are used throughout because LinkedIn profile fields are optional
— users may leave sections empty or the API may omit fields depending on
account privacy settings.
"""

from __future__ import annotations

import pandas as pd

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _empty_df(columns: dict[str, object]) -> pd.DataFrame:
    """Return an empty DataFrame with specified column dtypes.

    Parameters
    ----------
    columns:
        Mapping of column name to pandas dtype.

    Returns
    -------
    pd.DataFrame
        An empty DataFrame with the given schema applied.
    """
    df = pd.DataFrame({col: pd.array([], dtype=dtype) for col, dtype in columns.items()})
    return df


# ---------------------------------------------------------------------------
# Section schemas
# ---------------------------------------------------------------------------

#: Schema for the top-level profile / summary section.
#:
#: Columns
#: -------
#: profile_id       : LinkedIn public profile ID (slug), e.g. "john-doe-123"
#: full_name        : Full display name on the profile
#: headline         : LinkedIn headline (role / tagline under the name)
#: summary          : About / summary section text (may be multi-paragraph)
#: location         : Free-text location field, e.g. "Calgary, Alberta, Canada"
#: industry         : Industry classification from LinkedIn
#: profile_picture_url : URL to the profile photo (fetched at render time)
#: connection_count : Approximate number of connections (LinkedIn rounds to 500+)
#: scraped_at       : ISO 8601 UTC timestamp of when this record was collected
PROFILE_SCHEMA: dict[str, object] = {
    "profile_id": pd.StringDtype(),
    "full_name": pd.StringDtype(),
    "headline": pd.StringDtype(),
    "summary": pd.StringDtype(),
    "location": pd.StringDtype(),
    "industry": pd.StringDtype(),
    "profile_picture_url": pd.StringDtype(),
    "connection_count": pd.Int64Dtype(),
    "scraped_at": pd.StringDtype(),
}

#: Schema for work experience records.
#:
#: Columns
#: -------
#: profile_id      : FK to profile
#: company_name    : Name of the employer
#: company_linkedin_url : LinkedIn company page URL (may be null)
#: company_logo_url: Logo image URL (may be null)
#: title           : Job title / position title
#: employment_type : e.g. "Full-time", "Contract", "Part-time" (may be null)
#: location        : Role location, e.g. "Remote" or "Calgary, AB"
#: start_date      : ISO 8601 date string, e.g. "2020-01-01"
#: end_date        : ISO 8601 date string; null means current/ongoing role
#: is_current      : True if this is the active role (end_date is null)
#: description     : Role description / bullet points as raw text
#: scraped_at      : ISO 8601 UTC timestamp of when this record was collected
EXPERIENCE_SCHEMA: dict[str, object] = {
    "profile_id": pd.StringDtype(),
    "company_name": pd.StringDtype(),
    "company_linkedin_url": pd.StringDtype(),
    "company_logo_url": pd.StringDtype(),
    "title": pd.StringDtype(),
    "employment_type": pd.StringDtype(),
    "location": pd.StringDtype(),
    "start_date": pd.StringDtype(),
    "end_date": pd.StringDtype(),
    "is_current": pd.BooleanDtype(),
    "description": pd.StringDtype(),
    "scraped_at": pd.StringDtype(),
}

#: Schema for education records.
#:
#: Columns
#: -------
#: profile_id      : FK to profile
#: school_name     : Name of the institution
#: school_linkedin_url : LinkedIn school page URL (may be null)
#: degree          : Degree type, e.g. "Bachelor of Science"
#: field_of_study  : Major / field, e.g. "Computer Science"
#: start_date      : ISO 8601 date string (year-level precision is fine)
#: end_date        : ISO 8601 date string; null if still enrolled
#: grade           : GPA or grade string (may be null)
#: activities      : Clubs, honours, activities description (may be null)
#: description     : Additional free-text description (may be null)
#: scraped_at      : ISO 8601 UTC timestamp of when this record was collected
EDUCATION_SCHEMA: dict[str, object] = {
    "profile_id": pd.StringDtype(),
    "school_name": pd.StringDtype(),
    "school_linkedin_url": pd.StringDtype(),
    "degree": pd.StringDtype(),
    "field_of_study": pd.StringDtype(),
    "start_date": pd.StringDtype(),
    "end_date": pd.StringDtype(),
    "grade": pd.StringDtype(),
    "activities": pd.StringDtype(),
    "description": pd.StringDtype(),
    "scraped_at": pd.StringDtype(),
}

#: Schema for skills records.
#:
#: Columns
#: -------
#: profile_id        : FK to profile
#: skill_name        : Canonical skill name as shown on LinkedIn
#: endorsement_count : Number of endorsements (available via API, not CSV export)
#: scraped_at        : ISO 8601 UTC timestamp of when this record was collected
SKILLS_SCHEMA: dict[str, object] = {
    "profile_id": pd.StringDtype(),
    "skill_name": pd.StringDtype(),
    "endorsement_count": pd.Int64Dtype(),
    "scraped_at": pd.StringDtype(),
}

#: Schema for certification records.
#:
#: Columns
#: -------
#: profile_id      : FK to profile
#: cert_name       : Name of the certification
#: authority       : Issuing organisation, e.g. "AWS", "Google"
#: issued_date     : ISO 8601 date string of issue date (may be null)
#: expiry_date     : ISO 8601 date string of expiry; null means no expiry
#: credential_id   : Unique credential ID/number (may be null)
#: credential_url  : Verification URL (may be null)
#: scraped_at      : ISO 8601 UTC timestamp of when this record was collected
CERTIFICATIONS_SCHEMA: dict[str, object] = {
    "profile_id": pd.StringDtype(),
    "cert_name": pd.StringDtype(),
    "authority": pd.StringDtype(),
    "issued_date": pd.StringDtype(),
    "expiry_date": pd.StringDtype(),
    "credential_id": pd.StringDtype(),
    "credential_url": pd.StringDtype(),
    "scraped_at": pd.StringDtype(),
}

#: Schema for recommendation records.
#:
#: Columns
#: -------
#: profile_id         : FK to profile (the person being recommended)
#: recommender_name   : Full name of the person who wrote the recommendation
#: recommender_title  : Job title of the recommender at time of recommendation
#: relationship       : Described relationship, e.g. "Managed directly"
#: recommendation_text: Full text of the recommendation
#: recommendation_date: ISO 8601 date string when the recommendation was given
#: scraped_at         : ISO 8601 UTC timestamp of when this record was collected
RECOMMENDATIONS_SCHEMA: dict[str, object] = {
    "profile_id": pd.StringDtype(),
    "recommender_name": pd.StringDtype(),
    "recommender_title": pd.StringDtype(),
    "relationship": pd.StringDtype(),
    "recommendation_text": pd.StringDtype(),
    "recommendation_date": pd.StringDtype(),
    "scraped_at": pd.StringDtype(),
}

# ---------------------------------------------------------------------------
# Convenience constructors
# ---------------------------------------------------------------------------


def empty_profile_df() -> pd.DataFrame:
    """Return an empty DataFrame conforming to ``PROFILE_SCHEMA``.

    Returns
    -------
    pd.DataFrame
        Zero-row DataFrame with the profile schema applied.
    """
    return _empty_df(PROFILE_SCHEMA)


def empty_experience_df() -> pd.DataFrame:
    """Return an empty DataFrame conforming to ``EXPERIENCE_SCHEMA``.

    Returns
    -------
    pd.DataFrame
        Zero-row DataFrame with the experience schema applied.
    """
    return _empty_df(EXPERIENCE_SCHEMA)


def empty_education_df() -> pd.DataFrame:
    """Return an empty DataFrame conforming to ``EDUCATION_SCHEMA``.

    Returns
    -------
    pd.DataFrame
        Zero-row DataFrame with the education schema applied.
    """
    return _empty_df(EDUCATION_SCHEMA)


def empty_skills_df() -> pd.DataFrame:
    """Return an empty DataFrame conforming to ``SKILLS_SCHEMA``.

    Returns
    -------
    pd.DataFrame
        Zero-row DataFrame with the skills schema applied.
    """
    return _empty_df(SKILLS_SCHEMA)


def empty_certifications_df() -> pd.DataFrame:
    """Return an empty DataFrame conforming to ``CERTIFICATIONS_SCHEMA``.

    Returns
    -------
    pd.DataFrame
        Zero-row DataFrame with the certifications schema applied.
    """
    return _empty_df(CERTIFICATIONS_SCHEMA)


def empty_recommendations_df() -> pd.DataFrame:
    """Return an empty DataFrame conforming to ``RECOMMENDATIONS_SCHEMA``.

    Returns
    -------
    pd.DataFrame
        Zero-row DataFrame with the recommendations schema applied.
    """
    return _empty_df(RECOMMENDATIONS_SCHEMA)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    # Schema dicts
    "PROFILE_SCHEMA",
    "EXPERIENCE_SCHEMA",
    "EDUCATION_SCHEMA",
    "SKILLS_SCHEMA",
    "CERTIFICATIONS_SCHEMA",
    "RECOMMENDATIONS_SCHEMA",
    # Empty DataFrame constructors
    "empty_profile_df",
    "empty_experience_df",
    "empty_education_df",
    "empty_skills_df",
    "empty_certifications_df",
    "empty_recommendations_df",
]
