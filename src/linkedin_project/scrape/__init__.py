"""
LinkedIn scrape subpackage.

Public API surface for the scraping layer.  Import from here to keep
downstream code insulated from internal module layout changes.

Quick-start
-----------
::

    from linkedin_project.scrape import scrape_profile, ProfileData

    # Requires LINKEDIN_USERNAME and LINKEDIN_PASSWORD env vars to be set.
    data: ProfileData = scrape_profile("john-doe-123")

    print(data.profile)
    print(data.experience)
    print(data.skills)

Schema contracts
----------------
Each section returns a pandas DataFrame.  The column names and dtypes are
defined in :mod:`linkedin_project.scrape.schema` and documented there in full.
"""

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
    empty_profile_df,
    empty_recommendations_df,
    empty_skills_df,
)
from linkedin_project.scrape.scraper import (
    AuthError,
    LinkedInClient,
    ProfileData,
    ScraperError,
    SchemaError,
    parse_certifications,
    parse_education,
    parse_experience,
    parse_profile,
    parse_recommendations,
    parse_skills,
    scrape_profile,
)

__all__ = [
    # High-level entry point
    "scrape_profile",
    "ProfileData",
    # Client (for injection / testing)
    "LinkedInClient",
    # Parsers
    "parse_profile",
    "parse_experience",
    "parse_education",
    "parse_skills",
    "parse_certifications",
    "parse_recommendations",
    # Exceptions
    "AuthError",
    "ScraperError",
    "SchemaError",
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

