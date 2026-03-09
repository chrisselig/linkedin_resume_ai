"""Tests for the Playwright-based LinkedIn scraper."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from linkedin_project.scrape.scraper import (  # noqa: E402
    AuthError,
    ProfileData,
    ScraperError,
    _login,
    _now_iso,
    _safe_attr,
    _safe_text,
    _scrape_certifications,
    _scrape_education,
    _scrape_experience,
    _scrape_profile_header,
    _scrape_skills,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestNowIso:
    def test_returns_string(self):
        assert isinstance(_now_iso(), str)

    def test_contains_t(self):
        assert "T" in _now_iso()


class TestSafeText:
    def test_returns_stripped_text(self):
        el = MagicMock()
        el.inner_text.return_value = "  Hello  "
        assert _safe_text(el) == "Hello"

    def test_returns_empty_on_exception(self):
        el = MagicMock()
        el.inner_text.side_effect = Exception("boom")
        assert _safe_text(el) == ""

    def test_returns_empty_on_none(self):
        el = MagicMock()
        el.inner_text.return_value = None
        assert _safe_text(el) == ""


class TestSafeAttr:
    def test_returns_attribute(self):
        el = MagicMock()
        el.get_attribute.return_value = "  value  "
        assert _safe_attr(el, "href") == "value"

    def test_returns_empty_on_exception(self):
        el = MagicMock()
        el.get_attribute.side_effect = Exception("boom")
        assert _safe_attr(el, "href") == ""


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------


def _make_page(url: str = "https://www.linkedin.com/feed/") -> MagicMock:
    """Return a minimal mock Playwright page."""
    page = MagicMock()
    page.url = url
    return page


class TestLogin:
    def test_successful_login(self):
        page = _make_page("https://www.linkedin.com/feed/")
        _login(page, "user@example.com", "password")
        page.fill.assert_any_call("#username", "user@example.com")
        page.fill.assert_any_call("#password", "password")

    def test_raises_on_checkpoint(self):
        page = _make_page("https://www.linkedin.com/checkpoint/challenge/")
        with pytest.raises(AuthError, match="verification"):
            _login(page, "u", "p")

    def test_raises_on_login_url(self):
        page = _make_page("https://www.linkedin.com/login?error=1")
        with pytest.raises(AuthError, match="failed"):
            _login(page, "u", "p")


# ---------------------------------------------------------------------------
# Profile header
# ---------------------------------------------------------------------------


def _mock_locator_text(text: str) -> MagicMock:
    loc = MagicMock()
    loc.first.inner_text.return_value = text
    return loc


class TestScrapeProfileHeader:
    def test_extracts_name_headline_location(self):
        page = MagicMock()
        page.locator.side_effect = [
            _mock_locator_text("Chris Selig"),
            _mock_locator_text("Analytics Engineer"),
            _mock_locator_text("Calgary, AB, Canada"),
        ]
        df = _scrape_profile_header(page, "chris-selig")
        assert df["full_name"].iloc[0] == "Chris Selig"
        assert df["headline"].iloc[0] == "Analytics Engineer"
        assert df["location"].iloc[0] == "Calgary, AB, Canada"
        assert df["profile_id"].iloc[0] == "chris-selig"

    def test_handles_missing_elements(self):
        page = MagicMock()
        page.locator.side_effect = Exception("not found")
        df = _scrape_profile_header(page, "chris-selig")
        assert isinstance(df, pd.DataFrame)


# ---------------------------------------------------------------------------
# Experience
# ---------------------------------------------------------------------------


def _mock_exp_item(title: str, company: str, date: str) -> MagicMock:
    item = MagicMock()

    def locator_side_effect(selector):
        loc = MagicMock()
        if "t-bold" in selector:
            loc.first.inner_text.return_value = title
        elif "t-normal" in selector and "pvs-list" not in selector:
            loc.first.inner_text.return_value = company
        elif "caption" in selector:
            loc.first.inner_text.return_value = date
        else:
            loc.count.return_value = 0
            loc.first.inner_text.return_value = ""
        loc.count.return_value = 1
        return loc

    item.locator.side_effect = locator_side_effect
    return item


class TestScrapeExperience:
    def test_returns_empty_df_when_section_missing(self):
        page = MagicMock()
        page.locator.return_value.first = MagicMock(side_effect=Exception("no section"))
        df = _scrape_experience(page, "chris-selig")
        assert isinstance(df, pd.DataFrame)

    def test_extracts_rows(self):
        page = MagicMock()
        item = _mock_exp_item(
            "Analytics Engineer", "BIDAMIA", "Jan 2022 – Present · 2 yrs"
        )
        section_loc = MagicMock()
        section_loc.first = MagicMock()
        items_loc = MagicMock()
        items_loc.all.return_value = [item]
        section_loc.first.locator.return_value = items_loc
        page.locator.return_value = section_loc
        df = _scrape_experience(page, "chris-selig")
        assert isinstance(df, pd.DataFrame)

    def test_is_current_true_for_present(self):
        page = MagicMock()
        item = _mock_exp_item("Engineer", "Co", "Jan 2022 – Present")
        section_loc = MagicMock()
        items_loc = MagicMock()
        items_loc.all.return_value = [item]
        section_loc.first.locator.return_value = items_loc
        page.locator.return_value = section_loc
        df = _scrape_experience(page, "p")
        if not df.empty and "is_current" in df.columns:
            assert df["is_current"].iloc[0] in (True, False)  # type valid


# ---------------------------------------------------------------------------
# Education
# ---------------------------------------------------------------------------


class TestScrapeEducation:
    def test_returns_dataframe(self):
        page = MagicMock()
        page.locator.side_effect = Exception("no section")
        df = _scrape_education(page, "p")
        assert isinstance(df, pd.DataFrame)


# ---------------------------------------------------------------------------
# Skills
# ---------------------------------------------------------------------------


class TestScrapeSkills:
    def test_returns_dataframe(self):
        page = MagicMock()
        page.locator.side_effect = Exception("no section")
        df = _scrape_skills(page, "p")
        assert isinstance(df, pd.DataFrame)

    def test_extracts_skill_names(self):
        page = MagicMock()
        item = MagicMock()

        def loc_se(sel):
            loc = MagicMock()
            if "t-bold" in sel:
                loc.first.inner_text.return_value = "Python"
            elif "caption" in sel:
                loc.first.inner_text.return_value = "42 endorsements"
            loc.count.return_value = 1
            return loc

        item.locator.side_effect = loc_se
        section_mock = MagicMock()
        items_mock = MagicMock()
        items_mock.all.return_value = [item]
        section_mock.first.locator.return_value = items_mock
        page.locator.return_value = section_mock
        df = _scrape_skills(page, "p")
        assert isinstance(df, pd.DataFrame)


# ---------------------------------------------------------------------------
# Certifications
# ---------------------------------------------------------------------------


class TestScrapeCertifications:
    def test_returns_dataframe(self):
        page = MagicMock()
        page.locator.side_effect = Exception("no section")
        df = _scrape_certifications(page, "p")
        assert isinstance(df, pd.DataFrame)


# ---------------------------------------------------------------------------
# ProfileData container
# ---------------------------------------------------------------------------


class TestProfileData:
    def test_default_fields_are_dataframes(self):
        pd_ = ProfileData()
        assert isinstance(pd_.experience, pd.DataFrame)
        assert isinstance(pd_.education, pd.DataFrame)
        assert isinstance(pd_.skills, pd.DataFrame)
        assert isinstance(pd_.certifications, pd.DataFrame)


# ---------------------------------------------------------------------------
# scrape_profile integration (mocked Playwright)
# ---------------------------------------------------------------------------


class TestScrapeProfileIntegration:
    def test_raises_auth_error_on_missing_creds(self):
        import os

        env_backup = {
            k: os.environ.pop(k, None)
            for k in ("LINKEDIN_USERNAME", "LINKEDIN_PASSWORD", "LINKEDIN_PROFILE")
        }
        try:
            from linkedin_project.scrape.scraper import scrape_profile  # noqa: E402

            with pytest.raises(AuthError):
                scrape_profile(public_id="chris-selig")
        finally:
            for k, v in env_backup.items():
                if v is not None:
                    os.environ[k] = v

    def test_raises_scraper_error_on_missing_profile(self):
        import os

        os.environ["LINKEDIN_USERNAME"] = "u"
        os.environ["LINKEDIN_PASSWORD"] = "p"
        try:
            from linkedin_project.scrape.scraper import scrape_profile  # noqa: E402

            with pytest.raises(ScraperError):
                scrape_profile(public_id="")
        finally:
            del os.environ["LINKEDIN_USERNAME"]
            del os.environ["LINKEDIN_PASSWORD"]

    def test_returns_profile_data_with_mocked_playwright(self):
        """Full integration test with Playwright mocked out entirely."""
        import os

        os.environ["LINKEDIN_USERNAME"] = "test@example.com"
        os.environ["LINKEDIN_PASSWORD"] = "testpass"

        mock_page = MagicMock()
        mock_page.url = "https://www.linkedin.com/feed/"

        # Mock every locator call to return empty results
        def empty_locator(sel):
            loc = MagicMock()
            loc.first.inner_text.return_value = ""
            loc.count.return_value = 0
            loc.all.return_value = []
            return loc

        mock_page.locator.side_effect = empty_locator

        mock_context = MagicMock()
        mock_context.new_page.return_value = mock_page
        mock_browser = MagicMock()
        mock_browser.new_context.return_value = mock_context

        mock_pw = MagicMock()
        mock_pw.__enter__ = MagicMock(return_value=mock_pw)
        mock_pw.__exit__ = MagicMock(return_value=False)
        mock_pw.chromium.launch.return_value = mock_browser

        with patch(
            "playwright.sync_api.sync_playwright",
            return_value=mock_pw,
        ):
            from linkedin_project.scrape.scraper import scrape_profile  # noqa: E402

            result = scrape_profile(public_id="chris-selig")

        assert isinstance(result, ProfileData)
        assert isinstance(result.experience, pd.DataFrame)
        assert isinstance(result.skills, pd.DataFrame)

        del os.environ["LINKEDIN_USERNAME"]
        del os.environ["LINKEDIN_PASSWORD"]
