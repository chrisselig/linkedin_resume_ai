"""LinkedIn profile scraper using Playwright (headless Chromium).

Logs in to LinkedIn with your credentials, navigates to a public profile,
and extracts experience, education, skills, and certifications via DOM
scraping.  No unofficial API is used — the scraper drives a real browser,
which is far more reliable against auth blocks than API-based approaches.

Environment variables
---------------------
LINKEDIN_USERNAME   — LinkedIn account email address
LINKEDIN_PASSWORD   — LinkedIn account password
LINKEDIN_PROFILE    — Public profile slug (e.g. "chris-selig")

Raises
------
AuthError
    Login failed or LinkedIn presented a verification challenge.
ScraperError
    A page navigation or DOM extraction step failed unexpectedly.
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
    PROFILE_SCHEMA,
    empty_certifications_df,
    empty_education_df,
    empty_experience_df,
    empty_skills_df,
)

logger = logging.getLogger(__name__)

_LINKEDIN_LOGIN_URL = "https://www.linkedin.com/login"
_LINKEDIN_PROFILE_BASE = "https://www.linkedin.com/in"

# Polite delay between page loads (seconds)
_PAGE_DELAY = 2.5


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class AuthError(Exception):
    """Raised when LinkedIn login fails."""


class ScraperError(Exception):
    """Raised when a scraping step fails unexpectedly."""


# ---------------------------------------------------------------------------
# Data container
# ---------------------------------------------------------------------------


@dataclass
class ProfileData:
    """Container for all scraped LinkedIn profile sections."""

    profile: pd.DataFrame = field(default_factory=empty_experience_df)
    experience: pd.DataFrame = field(default_factory=empty_experience_df)
    education: pd.DataFrame = field(default_factory=empty_education_df)
    skills: pd.DataFrame = field(default_factory=empty_skills_df)
    certifications: pd.DataFrame = field(default_factory=empty_certifications_df)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()


def _safe_text(element: Any) -> str:
    """Extract stripped text from a Playwright element handle, or ''."""
    try:
        text = element.inner_text()
        return text.strip() if text else ""
    except Exception:
        return ""


def _safe_attr(element: Any, attr: str) -> str:
    """Get an attribute from a Playwright element handle, or ''."""
    try:
        val = element.get_attribute(attr)
        return val.strip() if val else ""
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------


def _login(page: Any, username: str, password: str) -> None:
    """Log in to LinkedIn.  Raises AuthError on failure.

    Args:
        page: A Playwright ``Page`` object.
        username: LinkedIn account email.
        password: LinkedIn account password.

    Raises:
        AuthError: If login fails or a challenge page is detected.
    """
    logger.info("Navigating to LinkedIn login page...")
    page.goto(_LINKEDIN_LOGIN_URL, wait_until="domcontentloaded")
    time.sleep(1)

    # Fill credentials
    page.fill("#username", username)
    page.fill("#password", password)
    page.click('[data-litms-control-urn="login-submit"]')

    # Wait for navigation
    page.wait_for_load_state("domcontentloaded")
    time.sleep(_PAGE_DELAY)

    current_url = page.url
    logger.info("Post-login URL: %s", current_url)

    if "checkpoint" in current_url or "challenge" in current_url:
        raise AuthError(
            "LinkedIn requires verification. "
            "Complete the challenge manually in a browser first, "
            "or disable 2-step verification."
        )
    if "login" in current_url:
        raise AuthError(
            "LinkedIn login failed. " "Check LINKEDIN_USERNAME and LINKEDIN_PASSWORD."
        )

    logger.info("Login successful.")


# ---------------------------------------------------------------------------
# Section scrapers
# ---------------------------------------------------------------------------


def _scrape_profile_header(page: Any, public_id: str) -> pd.DataFrame:
    """Scrape the profile header (name, headline, location).

    Args:
        page: A logged-in Playwright ``Page`` on the profile URL.
        public_id: LinkedIn profile slug.

    Returns:
        A one-row DataFrame conforming to ``PROFILE_SCHEMA``.
    """
    scraped_at = _now_iso()
    try:
        name = page.locator("h1.text-heading-xlarge").first.inner_text().strip()
    except Exception:
        name = ""
    try:
        headline = (
            page.locator(".text-body-medium.break-words").first.inner_text().strip()
        )
    except Exception:
        headline = ""
    try:
        location = (
            page.locator(".pv-text-details__left-panel .text-body-small")
            .first.inner_text()
            .strip()
        )
    except Exception:
        location = ""

    row = {k: pd.NA for k in PROFILE_SCHEMA}
    row.update(
        {
            "profile_id": public_id,
            "full_name": name,
            "headline": headline,
            "location": location,
            "scraped_at": scraped_at,
        }
    )
    return pd.DataFrame([row]).astype(
        {k: v for k, v in PROFILE_SCHEMA.items() if k in row}
    )


def _scroll_to_load(page: Any) -> None:
    """Slowly scroll the page to trigger lazy-loaded content.

    Args:
        page: A Playwright ``Page`` object.
    """
    for _ in range(6):
        page.evaluate("window.scrollBy(0, window.innerHeight * 0.8)")
        time.sleep(0.6)
    page.evaluate("window.scrollTo(0, 0)")
    time.sleep(0.5)


def _click_show_all(page: Any, section_id: str) -> None:
    """Click 'Show all' / 'See more' inside a profile section if present.

    Args:
        page: A Playwright ``Page`` object.
        section_id: The ``id`` attribute of the section element.
    """
    try:
        btn = page.locator(
            f"#{section_id} a[href*='detail'], "
            f"#{section_id} button:has-text('Show all'), "
            f"#{section_id} button:has-text('See more')"
        ).first
        if btn.is_visible(timeout=1500):
            btn.click()
            time.sleep(1.5)
    except Exception:
        pass


def _scrape_experience(page: Any, public_id: str) -> pd.DataFrame:
    """Scrape the experience section from the profile page.

    Args:
        page: A logged-in Playwright ``Page`` on the profile URL.
        public_id: LinkedIn profile slug.

    Returns:
        A DataFrame conforming to ``EXPERIENCE_SCHEMA``.
    """
    scraped_at = _now_iso()
    rows: list[dict] = []

    try:
        section = page.locator("#experience").first
        items = section.locator("li.artdeco-list__item").all()
    except Exception:
        return empty_experience_df()

    for item in items:
        try:
            title = (
                item.locator(".t-bold span[aria-hidden='true']")
                .first.inner_text()
                .strip()
            )
            company = (
                item.locator(".t-normal.t-black--light span[aria-hidden='true']")
                .first.inner_text()
                .strip()
            )
            date_range = (
                item.locator(".pvs-entity__caption-wrapper").first.inner_text().strip()
            )
            location_el = item.locator(
                "span.t-black--light:not(.pvs-entity__caption-wrapper)"
            )
            location = (
                location_el.first.inner_text().strip() if location_el.count() else ""
            )
            desc_el = item.locator(".pvs-list__item--no-padding-when-first .t-normal")
            description = desc_el.first.inner_text().strip() if desc_el.count() else ""

            parts = [p.strip() for p in date_range.split("·") if p.strip()]
            start_date, end_date, is_current = "", "", False
            if parts:
                date_part = parts[0]
                if "–" in date_part or "-" in date_part:
                    sep = "–" if "–" in date_part else "-"
                    halves = date_part.split(sep)
                    start_date = halves[0].strip()
                    end_raw = halves[1].strip() if len(halves) > 1 else ""
                    is_current = "present" in end_raw.lower()
                    end_date = "" if is_current else end_raw

            rows.append(
                {
                    "profile_id": public_id,
                    "company_name": company,
                    "company_linkedin_url": pd.NA,
                    "company_logo_url": pd.NA,
                    "title": title,
                    "employment_type": pd.NA,
                    "location": location,
                    "start_date": start_date,
                    "end_date": end_date if end_date else pd.NA,
                    "is_current": is_current,
                    "description": description,
                    "scraped_at": scraped_at,
                }
            )
        except Exception as exc:
            logger.debug("Skipping experience item: %s", exc)
            continue

    if not rows:
        return empty_experience_df()
    return pd.DataFrame(rows)


def _scrape_education(page: Any, public_id: str) -> pd.DataFrame:
    """Scrape the education section from the profile page.

    Args:
        page: A logged-in Playwright ``Page`` on the profile URL.
        public_id: LinkedIn profile slug.

    Returns:
        A DataFrame conforming to ``EDUCATION_SCHEMA``.
    """
    scraped_at = _now_iso()
    rows: list[dict] = []

    try:
        section = page.locator("#education").first
        items = section.locator("li.artdeco-list__item").all()
    except Exception:
        return empty_education_df()

    for item in items:
        try:
            school = (
                item.locator(".t-bold span[aria-hidden='true']")
                .first.inner_text()
                .strip()
            )
            detail_els = item.locator(
                ".t-normal.t-black--light span[aria-hidden='true']"
            ).all()
            degree = detail_els[0].inner_text().strip() if len(detail_els) > 0 else ""
            field_of_study = (
                detail_els[1].inner_text().strip() if len(detail_els) > 1 else ""
            )
            date_text = (
                item.locator(".pvs-entity__caption-wrapper").first.inner_text().strip()
            )
            start_date, end_date = "", ""
            if "–" in date_text or "-" in date_text:
                sep = "–" if "–" in date_text else "-"
                halves = date_text.split(sep)
                start_date = halves[0].strip()
                end_date = halves[1].strip() if len(halves) > 1 else ""

            rows.append(
                {
                    "profile_id": public_id,
                    "school_name": school,
                    "school_linkedin_url": pd.NA,
                    "degree": degree,
                    "field_of_study": field_of_study,
                    "start_date": start_date,
                    "end_date": end_date,
                    "grade": pd.NA,
                    "activities": pd.NA,
                    "description": pd.NA,
                    "scraped_at": scraped_at,
                }
            )
        except Exception as exc:
            logger.debug("Skipping education item: %s", exc)
            continue

    if not rows:
        return empty_education_df()
    return pd.DataFrame(rows)


def _scrape_skills(page: Any, public_id: str) -> pd.DataFrame:
    """Scrape the skills section from the profile page.

    Args:
        page: A logged-in Playwright ``Page`` on the profile URL.
        public_id: LinkedIn profile slug.

    Returns:
        A DataFrame conforming to ``SKILLS_SCHEMA``.
    """
    scraped_at = _now_iso()
    rows: list[dict] = []

    try:
        section = page.locator("#skills").first
        items = section.locator("li.artdeco-list__item").all()
    except Exception:
        return empty_skills_df()

    for item in items:
        try:
            name = (
                item.locator(".t-bold span[aria-hidden='true']")
                .first.inner_text()
                .strip()
            )
            endorse_el = item.locator(".pvs-entity__caption-wrapper")
            endorsement_count = pd.NA
            if endorse_el.count():
                text = endorse_el.first.inner_text().strip()
                digits = "".join(c for c in text if c.isdigit())
                if digits:
                    endorsement_count = int(digits)
            if name:
                rows.append(
                    {
                        "profile_id": public_id,
                        "skill_name": name,
                        "endorsement_count": endorsement_count,
                        "scraped_at": scraped_at,
                    }
                )
        except Exception as exc:
            logger.debug("Skipping skill item: %s", exc)
            continue

    if not rows:
        return empty_skills_df()
    return pd.DataFrame(rows)


def _scrape_certifications(page: Any, public_id: str) -> pd.DataFrame:
    """Scrape the certifications section from the profile page.

    Args:
        page: A logged-in Playwright ``Page`` on the profile URL.
        public_id: LinkedIn profile slug.

    Returns:
        A DataFrame conforming to ``CERTIFICATIONS_SCHEMA``.
    """
    scraped_at = _now_iso()
    rows: list[dict] = []

    try:
        section = page.locator("#licenses_and_certifications").first
        items = section.locator("li.artdeco-list__item").all()
    except Exception:
        return empty_certifications_df()

    for item in items:
        try:
            name = (
                item.locator(".t-bold span[aria-hidden='true']")
                .first.inner_text()
                .strip()
            )
            authority_el = item.locator(
                ".t-normal.t-black--light span[aria-hidden='true']"
            )
            authority = (
                authority_el.first.inner_text().strip() if authority_el.count() else ""
            )
            date_el = item.locator(".pvs-entity__caption-wrapper")
            issued_date = date_el.first.inner_text().strip() if date_el.count() else ""
            if name:
                rows.append(
                    {
                        "profile_id": public_id,
                        "cert_name": name,
                        "authority": authority,
                        "issued_date": issued_date,
                        "expiry_date": pd.NA,
                        "credential_id": pd.NA,
                        "credential_url": pd.NA,
                        "scraped_at": scraped_at,
                    }
                )
        except Exception as exc:
            logger.debug("Skipping cert item: %s", exc)
            continue

    if not rows:
        return empty_certifications_df()
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def scrape_profile(
    public_id: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    headless: bool = True,
) -> ProfileData:
    """Scrape a LinkedIn profile using a headless Chromium browser.

    Credentials and the target profile slug are read from environment
    variables if not passed directly.

    Args:
        public_id: LinkedIn profile slug (e.g. ``"chris-selig"``).
                   Falls back to ``LINKEDIN_PROFILE`` env var.
        username: LinkedIn login email. Falls back to ``LINKEDIN_USERNAME``.
        password: LinkedIn login password. Falls back to ``LINKEDIN_PASSWORD``.
        headless: Run the browser in headless mode (default ``True``).
                  Set to ``False`` to watch the browser during debugging.

    Returns:
        A :class:`ProfileData` container with DataFrames for all sections.

    Raises:
        AuthError: If login fails or a verification challenge is detected.
        ScraperError: If a critical scraping step fails.
    """
    from playwright.sync_api import sync_playwright

    public_id = public_id or os.environ.get("LINKEDIN_PROFILE", "")
    username = username or os.environ.get("LINKEDIN_USERNAME", "")
    password = password or os.environ.get("LINKEDIN_PASSWORD", "")

    if not username or not password:
        raise AuthError(
            "LINKEDIN_USERNAME and LINKEDIN_PASSWORD environment variables must be set."
        )
    if not public_id:
        raise ScraperError("No LinkedIn profile slug provided.")

    profile_url = f"{_LINKEDIN_PROFILE_BASE}/{public_id}/"

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()

        try:
            _login(page, username, password)

            logger.info("Navigating to profile: %s", profile_url)
            page.goto(profile_url, wait_until="domcontentloaded")
            time.sleep(_PAGE_DELAY)
            _scroll_to_load(page)

            logger.info("Scraping profile header...")
            profile_df = _scrape_profile_header(page, public_id)

            logger.info("Scraping experience...")
            _click_show_all(page, "experience")
            experience_df = _scrape_experience(page, public_id)

            logger.info("Scraping education...")
            _click_show_all(page, "education")
            education_df = _scrape_education(page, public_id)

            logger.info("Scraping skills...")
            _click_show_all(page, "skills")
            skills_df = _scrape_skills(page, public_id)

            logger.info("Scraping certifications...")
            certifications_df = _scrape_certifications(page, public_id)

            return ProfileData(
                profile=profile_df,
                experience=experience_df,
                education=education_df,
                skills=skills_df,
                certifications=certifications_df,
            )

        except AuthError:
            raise
        except Exception as exc:
            raise ScraperError(f"Scraping failed: {exc}") from exc
        finally:
            browser.close()
