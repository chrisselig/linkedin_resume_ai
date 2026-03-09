"""
LinkedIn scrape subpackage — Playwright-based browser scraper.

Quick-start
-----------
::

    from linkedin_project.scrape import scrape_profile, ProfileData

    # Requires LINKEDIN_USERNAME, LINKEDIN_PASSWORD, LINKEDIN_PROFILE env vars.
    data: ProfileData = scrape_profile("chris-selig")

    print(data.profile)
    print(data.experience)
    print(data.skills)
"""

from linkedin_project.scrape.scraper import (
    AuthError,
    ProfileData,
    ScraperError,
    scrape_profile,
)

__all__ = [
    "scrape_profile",
    "ProfileData",
    "AuthError",
    "ScraperError",
]
