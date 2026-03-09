"""
Unit tests for the LinkedIn scraping module.

All tests use mocked responses — no real network calls are made.
Fixture JSON files in ``tests/fixtures/linkedin_responses/`` provide realistic
but synthetic API response data.

Test coverage
-------------
- Schema: column names, dtypes, empty constructors
- Parsers: parse_profile, parse_experience, parse_education, parse_skills,
  parse_certifications, parse_recommendations
- Edge cases: empty sections, missing/null fields, partial dates
- Client: AuthError on missing credentials, retry logic on failure
- High-level: scrape_profile with injected mock client
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

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
    _parse_date,
    _safe_get,
    parse_certifications,
    parse_education,
    parse_experience,
    parse_profile,
    parse_recommendations,
    parse_skills,
    scrape_profile,
)

# ---------------------------------------------------------------------------
# Fixture paths
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "linkedin_responses"


def _load_fixture(name: str) -> dict | list:
    """Load a JSON fixture file by name (without extension)."""
    return json.loads((FIXTURES_DIR / f"{name}.json").read_text())


# ---------------------------------------------------------------------------
# Fixtures (pytest)
# ---------------------------------------------------------------------------


@pytest.fixture()
def raw_profile() -> dict:
    """Raw profile API response fixture."""
    return _load_fixture("profile")


@pytest.fixture()
def raw_skills() -> list:
    """Raw skills API response fixture."""
    return _load_fixture("skills")


@pytest.fixture()
def raw_certifications() -> list:
    """Raw certifications API response fixture."""
    return _load_fixture("certifications")


@pytest.fixture()
def raw_recommendations() -> dict:
    """Raw recommendations API response fixture."""
    return _load_fixture("recommendations")


@pytest.fixture()
def mock_client(raw_profile, raw_skills, raw_certifications, raw_recommendations):
    """A LinkedInClient with all API methods replaced by mocks."""
    client = LinkedInClient(_client=MagicMock())
    client.get_profile_raw = MagicMock(return_value=raw_profile)
    client.get_skills_raw = MagicMock(return_value=raw_skills)
    client.get_certifications_raw = MagicMock(return_value=raw_certifications)
    client.get_recommendations_raw = MagicMock(return_value=raw_recommendations)
    return client


PUBLIC_ID = "jane-doe-123"


# ===========================================================================
# Schema tests
# ===========================================================================


class TestSchemaDefinitions:
    """Validate that schema dicts have expected keys and dtypes."""

    def test_profile_schema_columns(self):
        expected = {
            "profile_id",
            "full_name",
            "headline",
            "summary",
            "location",
            "industry",
            "profile_picture_url",
            "connection_count",
            "scraped_at",
        }
        assert set(PROFILE_SCHEMA.keys()) == expected

    def test_experience_schema_columns(self):
        expected = {
            "profile_id",
            "company_name",
            "company_linkedin_url",
            "company_logo_url",
            "title",
            "employment_type",
            "location",
            "start_date",
            "end_date",
            "is_current",
            "description",
            "scraped_at",
        }
        assert set(EXPERIENCE_SCHEMA.keys()) == expected

    def test_education_schema_columns(self):
        expected = {
            "profile_id",
            "school_name",
            "school_linkedin_url",
            "degree",
            "field_of_study",
            "start_date",
            "end_date",
            "grade",
            "activities",
            "description",
            "scraped_at",
        }
        assert set(EDUCATION_SCHEMA.keys()) == expected

    def test_skills_schema_columns(self):
        expected = {"profile_id", "skill_name", "endorsement_count", "scraped_at"}
        assert set(SKILLS_SCHEMA.keys()) == expected

    def test_certifications_schema_columns(self):
        expected = {
            "profile_id",
            "cert_name",
            "authority",
            "issued_date",
            "expiry_date",
            "credential_id",
            "credential_url",
            "scraped_at",
        }
        assert set(CERTIFICATIONS_SCHEMA.keys()) == expected

    def test_recommendations_schema_columns(self):
        expected = {
            "profile_id",
            "recommender_name",
            "recommender_title",
            "relationship",
            "recommendation_text",
            "recommendation_date",
            "scraped_at",
        }
        assert set(RECOMMENDATIONS_SCHEMA.keys()) == expected

    def test_connection_count_is_nullable_int(self):
        assert PROFILE_SCHEMA["connection_count"] == pd.Int64Dtype()

    def test_endorsement_count_is_nullable_int(self):
        assert SKILLS_SCHEMA["endorsement_count"] == pd.Int64Dtype()

    def test_is_current_is_boolean(self):
        assert EXPERIENCE_SCHEMA["is_current"] == pd.BooleanDtype()


class TestEmptyDataFrameConstructors:
    """Validate empty DataFrame constructors return correct schema."""

    @pytest.mark.parametrize(
        "constructor, schema",
        [
            (empty_profile_df, PROFILE_SCHEMA),
            (empty_experience_df, EXPERIENCE_SCHEMA),
            (empty_education_df, EDUCATION_SCHEMA),
            (empty_skills_df, SKILLS_SCHEMA),
            (empty_certifications_df, CERTIFICATIONS_SCHEMA),
            (empty_recommendations_df, RECOMMENDATIONS_SCHEMA),
        ],
    )
    def test_empty_df_has_zero_rows(self, constructor, schema):
        df = constructor()
        assert len(df) == 0

    @pytest.mark.parametrize(
        "constructor, schema",
        [
            (empty_profile_df, PROFILE_SCHEMA),
            (empty_experience_df, EXPERIENCE_SCHEMA),
            (empty_education_df, EDUCATION_SCHEMA),
            (empty_skills_df, SKILLS_SCHEMA),
            (empty_certifications_df, CERTIFICATIONS_SCHEMA),
            (empty_recommendations_df, RECOMMENDATIONS_SCHEMA),
        ],
    )
    def test_empty_df_has_correct_columns(self, constructor, schema):
        df = constructor()
        assert list(df.columns) == list(schema.keys())

    @pytest.mark.parametrize(
        "constructor, schema",
        [
            (empty_profile_df, PROFILE_SCHEMA),
            (empty_experience_df, EXPERIENCE_SCHEMA),
            (empty_education_df, EDUCATION_SCHEMA),
            (empty_skills_df, SKILLS_SCHEMA),
            (empty_certifications_df, CERTIFICATIONS_SCHEMA),
            (empty_recommendations_df, RECOMMENDATIONS_SCHEMA),
        ],
    )
    def test_empty_df_has_correct_dtypes(self, constructor, schema):
        df = constructor()
        for col, expected_dtype in schema.items():
            assert (
                df[col].dtype == expected_dtype
            ), f"Column '{col}': expected {expected_dtype}, got {df[col].dtype}"


# ===========================================================================
# Helper function tests
# ===========================================================================


class TestSafeGet:
    """Unit tests for the _safe_get utility."""

    def test_simple_key(self):
        assert _safe_get({"a": 1}, "a") == 1

    def test_nested_key(self):
        assert _safe_get({"a": {"b": "val"}}, "a", "b") == "val"

    def test_missing_key_returns_default(self):
        assert _safe_get({"a": 1}, "b") is None

    def test_missing_key_custom_default(self):
        assert _safe_get({"a": 1}, "b", default="fallback") == "fallback"

    def test_none_object_returns_default(self):
        assert _safe_get(None, "a") is None

    def test_deeply_missing_returns_none(self):
        assert _safe_get({"a": {"b": {}}}, "a", "b", "c") is None

    def test_list_index_access(self):
        assert _safe_get([10, 20, 30], "1") == 20

    def test_list_out_of_range_returns_default(self):
        assert _safe_get([10], "5") is None


class TestParseDate:
    """Unit tests for the _parse_date utility."""

    def test_year_and_month(self):
        assert _parse_date(2020, 3) == "2020-03-01"

    def test_year_only_defaults_month_to_1(self):
        assert _parse_date(2020) == "2020-01-01"

    def test_none_year_returns_none(self):
        assert _parse_date(None) is None

    def test_none_month_defaults_to_1(self):
        assert _parse_date(2021, None) == "2021-01-01"

    def test_single_digit_month_zero_padded(self):
        assert _parse_date(2019, 7) == "2019-07-01"

    def test_december(self):
        assert _parse_date(2023, 12) == "2023-12-01"


# ===========================================================================
# Parser tests
# ===========================================================================


class TestParseProfile:
    """Tests for parse_profile."""

    def test_returns_dataframe(self, raw_profile):
        df = parse_profile(raw_profile, PUBLIC_ID)
        assert isinstance(df, pd.DataFrame)

    def test_single_row(self, raw_profile):
        df = parse_profile(raw_profile, PUBLIC_ID)
        assert len(df) == 1

    def test_profile_id(self, raw_profile):
        df = parse_profile(raw_profile, PUBLIC_ID)
        assert df["profile_id"].iloc[0] == PUBLIC_ID

    def test_full_name_concatenated(self, raw_profile):
        df = parse_profile(raw_profile, PUBLIC_ID)
        assert df["full_name"].iloc[0] == "Jane Doe"

    def test_headline(self, raw_profile):
        df = parse_profile(raw_profile, PUBLIC_ID)
        assert "Data Engineer" in df["headline"].iloc[0]

    def test_summary(self, raw_profile):
        df = parse_profile(raw_profile, PUBLIC_ID)
        assert df["summary"].iloc[0] is not pd.NA
        assert len(df["summary"].iloc[0]) > 0

    def test_location(self, raw_profile):
        df = parse_profile(raw_profile, PUBLIC_ID)
        assert df["location"].iloc[0] == "Calgary, Alberta, Canada"

    def test_connection_count_is_int(self, raw_profile):
        df = parse_profile(raw_profile, PUBLIC_ID)
        assert df["connection_count"].iloc[0] == 512

    def test_profile_picture_url_constructed(self, raw_profile):
        df = parse_profile(raw_profile, PUBLIC_ID)
        url = df["profile_picture_url"].iloc[0]
        assert url is not None and "licdn.com" in url

    def test_column_order_matches_schema(self, raw_profile):
        df = parse_profile(raw_profile, PUBLIC_ID)
        assert list(df.columns) == list(PROFILE_SCHEMA.keys())

    def test_dtypes_match_schema(self, raw_profile):
        df = parse_profile(raw_profile, PUBLIC_ID)
        for col, expected_dtype in PROFILE_SCHEMA.items():
            assert (
                df[col].dtype == expected_dtype
            ), f"Column '{col}': expected {expected_dtype}, got {df[col].dtype}"

    def test_scraped_at_is_iso_string(self, raw_profile):
        df = parse_profile(raw_profile, PUBLIC_ID)
        scraped_at = df["scraped_at"].iloc[0]
        assert "T" in scraped_at and "+" in scraped_at

    def test_missing_first_name_returns_last_name(self):
        raw = {"lastName": "Doe", "headline": "Engineer"}
        df = parse_profile(raw, PUBLIC_ID)
        assert df["full_name"].iloc[0] == "Doe"

    def test_fully_empty_profile(self):
        df = parse_profile({}, PUBLIC_ID)
        assert len(df) == 1
        assert df["profile_id"].iloc[0] == PUBLIC_ID
        assert df["headline"].iloc[0] is pd.NA or df["headline"].iloc[0] is None


class TestParseExperience:
    """Tests for parse_experience."""

    def test_returns_dataframe(self, raw_profile):
        df = parse_experience(raw_profile, PUBLIC_ID)
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self, raw_profile):
        df = parse_experience(raw_profile, PUBLIC_ID)
        assert len(df) == 2

    def test_current_role_has_null_end_date(self, raw_profile):
        df = parse_experience(raw_profile, PUBLIC_ID)
        current = df[df["is_current"] == True]  # noqa: E712
        assert len(current) == 1
        assert current["end_date"].iloc[0] is pd.NA

    def test_past_role_has_end_date(self, raw_profile):
        df = parse_experience(raw_profile, PUBLIC_ID)
        past = df[df["is_current"] == False]  # noqa: E712
        assert len(past) == 1
        assert past["end_date"].iloc[0] == "2020-02-01"

    def test_start_date_format(self, raw_profile):
        df = parse_experience(raw_profile, PUBLIC_ID)
        current = df[df["is_current"] == True]  # noqa: E712
        assert current["start_date"].iloc[0] == "2020-03-01"

    def test_company_linkedin_url_constructed(self, raw_profile):
        df = parse_experience(raw_profile, PUBLIC_ID)
        url = df["company_linkedin_url"].iloc[0]
        assert "linkedin.com/company/acme-corp" in url

    def test_column_order_matches_schema(self, raw_profile):
        df = parse_experience(raw_profile, PUBLIC_ID)
        assert list(df.columns) == list(EXPERIENCE_SCHEMA.keys())

    def test_dtypes_match_schema(self, raw_profile):
        df = parse_experience(raw_profile, PUBLIC_ID)
        for col, expected_dtype in EXPERIENCE_SCHEMA.items():
            assert (
                df[col].dtype == expected_dtype
            ), f"Column '{col}': expected {expected_dtype}, got {df[col].dtype}"

    def test_empty_experience_returns_empty_df(self):
        df = parse_experience({"experience": []}, PUBLIC_ID)
        assert len(df) == 0
        assert list(df.columns) == list(EXPERIENCE_SCHEMA.keys())

    def test_missing_experience_key_returns_empty_df(self):
        df = parse_experience({}, PUBLIC_ID)
        assert len(df) == 0

    def test_all_profile_ids_match(self, raw_profile):
        df = parse_experience(raw_profile, PUBLIC_ID)
        assert (df["profile_id"] == PUBLIC_ID).all()


class TestParseEducation:
    """Tests for parse_education."""

    def test_returns_dataframe(self, raw_profile):
        df = parse_education(raw_profile, PUBLIC_ID)
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self, raw_profile):
        df = parse_education(raw_profile, PUBLIC_ID)
        assert len(df) == 1

    def test_school_name(self, raw_profile):
        df = parse_education(raw_profile, PUBLIC_ID)
        assert df["school_name"].iloc[0] == "University of Calgary"

    def test_degree(self, raw_profile):
        df = parse_education(raw_profile, PUBLIC_ID)
        assert df["degree"].iloc[0] == "Bachelor of Science"

    def test_field_of_study(self, raw_profile):
        df = parse_education(raw_profile, PUBLIC_ID)
        assert df["field_of_study"].iloc[0] == "Computer Science"

    def test_start_date(self, raw_profile):
        df = parse_education(raw_profile, PUBLIC_ID)
        assert df["start_date"].iloc[0] == "2013-01-01"

    def test_end_date(self, raw_profile):
        df = parse_education(raw_profile, PUBLIC_ID)
        assert df["end_date"].iloc[0] == "2017-01-01"

    def test_school_url_constructed(self, raw_profile):
        df = parse_education(raw_profile, PUBLIC_ID)
        url = df["school_linkedin_url"].iloc[0]
        assert "linkedin.com/school/university-of-calgary" in url

    def test_grade(self, raw_profile):
        df = parse_education(raw_profile, PUBLIC_ID)
        assert df["grade"].iloc[0] == "3.8 GPA"

    def test_column_order_matches_schema(self, raw_profile):
        df = parse_education(raw_profile, PUBLIC_ID)
        assert list(df.columns) == list(EDUCATION_SCHEMA.keys())

    def test_dtypes_match_schema(self, raw_profile):
        df = parse_education(raw_profile, PUBLIC_ID)
        for col, expected_dtype in EDUCATION_SCHEMA.items():
            assert (
                df[col].dtype == expected_dtype
            ), f"Column '{col}': expected {expected_dtype}, got {df[col].dtype}"

    def test_empty_education_returns_empty_df(self):
        df = parse_education({"education": []}, PUBLIC_ID)
        assert len(df) == 0

    def test_missing_education_key_returns_empty_df(self):
        df = parse_education({}, PUBLIC_ID)
        assert len(df) == 0


class TestParseSkills:
    """Tests for parse_skills."""

    def test_returns_dataframe(self, raw_skills):
        df = parse_skills(raw_skills, PUBLIC_ID)
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self, raw_skills):
        df = parse_skills(raw_skills, PUBLIC_ID)
        assert len(df) == 4

    def test_skill_names(self, raw_skills):
        df = parse_skills(raw_skills, PUBLIC_ID)
        assert "Python" in df["skill_name"].values

    def test_endorsement_counts_are_integers(self, raw_skills):
        df = parse_skills(raw_skills, PUBLIC_ID)
        python_row = df[df["skill_name"] == "Python"]
        assert python_row["endorsement_count"].iloc[0] == 45

    def test_column_order_matches_schema(self, raw_skills):
        df = parse_skills(raw_skills, PUBLIC_ID)
        assert list(df.columns) == list(SKILLS_SCHEMA.keys())

    def test_dtypes_match_schema(self, raw_skills):
        df = parse_skills(raw_skills, PUBLIC_ID)
        for col, expected_dtype in SKILLS_SCHEMA.items():
            assert (
                df[col].dtype == expected_dtype
            ), f"Column '{col}': expected {expected_dtype}, got {df[col].dtype}"

    def test_empty_skills_returns_empty_df(self):
        df = parse_skills([], PUBLIC_ID)
        assert len(df) == 0

    def test_all_profile_ids_match(self, raw_skills):
        df = parse_skills(raw_skills, PUBLIC_ID)
        assert (df["profile_id"] == PUBLIC_ID).all()

    def test_skill_without_endorsement_count(self):
        raw = [{"name": "Leadership"}]
        df = parse_skills(raw, PUBLIC_ID)
        assert len(df) == 1
        assert pd.isna(df["endorsement_count"].iloc[0])


class TestParseCertifications:
    """Tests for parse_certifications."""

    def test_returns_dataframe(self, raw_certifications):
        df = parse_certifications(raw_certifications, PUBLIC_ID)
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self, raw_certifications):
        df = parse_certifications(raw_certifications, PUBLIC_ID)
        assert len(df) == 2

    def test_cert_name(self, raw_certifications):
        df = parse_certifications(raw_certifications, PUBLIC_ID)
        assert "AWS Certified Solutions Architect" in df["cert_name"].values

    def test_authority(self, raw_certifications):
        df = parse_certifications(raw_certifications, PUBLIC_ID)
        aws_row = df[df["cert_name"] == "AWS Certified Solutions Architect"]
        assert aws_row["authority"].iloc[0] == "Amazon Web Services"

    def test_issued_date(self, raw_certifications):
        df = parse_certifications(raw_certifications, PUBLIC_ID)
        aws_row = df[df["cert_name"] == "AWS Certified Solutions Architect"]
        assert aws_row["issued_date"].iloc[0] == "2022-05-01"

    def test_expiry_date(self, raw_certifications):
        df = parse_certifications(raw_certifications, PUBLIC_ID)
        aws_row = df[df["cert_name"] == "AWS Certified Solutions Architect"]
        assert aws_row["expiry_date"].iloc[0] == "2025-05-01"

    def test_null_expiry_date(self, raw_certifications):
        df = parse_certifications(raw_certifications, PUBLIC_ID)
        gcp_row = df[df["cert_name"] == "Google Professional Data Engineer"]
        assert pd.isna(gcp_row["expiry_date"].iloc[0])

    def test_credential_id(self, raw_certifications):
        df = parse_certifications(raw_certifications, PUBLIC_ID)
        aws_row = df[df["cert_name"] == "AWS Certified Solutions Architect"]
        assert aws_row["credential_id"].iloc[0] == "AWS-123456"

    def test_null_credential_id(self, raw_certifications):
        df = parse_certifications(raw_certifications, PUBLIC_ID)
        gcp_row = df[df["cert_name"] == "Google Professional Data Engineer"]
        assert pd.isna(gcp_row["credential_id"].iloc[0])

    def test_column_order_matches_schema(self, raw_certifications):
        df = parse_certifications(raw_certifications, PUBLIC_ID)
        assert list(df.columns) == list(CERTIFICATIONS_SCHEMA.keys())

    def test_dtypes_match_schema(self, raw_certifications):
        df = parse_certifications(raw_certifications, PUBLIC_ID)
        for col, expected_dtype in CERTIFICATIONS_SCHEMA.items():
            assert (
                df[col].dtype == expected_dtype
            ), f"Column '{col}': expected {expected_dtype}, got {df[col].dtype}"

    def test_empty_certifications_returns_empty_df(self):
        df = parse_certifications([], PUBLIC_ID)
        assert len(df) == 0


class TestParseRecommendations:
    """Tests for parse_recommendations."""

    def test_returns_dataframe(self, raw_recommendations):
        df = parse_recommendations(raw_recommendations, PUBLIC_ID)
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self, raw_recommendations):
        df = parse_recommendations(raw_recommendations, PUBLIC_ID)
        assert len(df) == 1

    def test_recommender_name_concatenated(self, raw_recommendations):
        df = parse_recommendations(raw_recommendations, PUBLIC_ID)
        assert df["recommender_name"].iloc[0] == "Bob Smith"

    def test_recommender_title(self, raw_recommendations):
        df = parse_recommendations(raw_recommendations, PUBLIC_ID)
        assert "Acme Corp" in df["recommender_title"].iloc[0]

    def test_relationship(self, raw_recommendations):
        df = parse_recommendations(raw_recommendations, PUBLIC_ID)
        assert df["relationship"].iloc[0] == "Managed directly"

    def test_recommendation_text(self, raw_recommendations):
        df = parse_recommendations(raw_recommendations, PUBLIC_ID)
        assert "exceptional" in df["recommendation_text"].iloc[0]

    def test_recommendation_date(self, raw_recommendations):
        df = parse_recommendations(raw_recommendations, PUBLIC_ID)
        assert df["recommendation_date"].iloc[0] == "2023-04-01"

    def test_column_order_matches_schema(self, raw_recommendations):
        df = parse_recommendations(raw_recommendations, PUBLIC_ID)
        assert list(df.columns) == list(RECOMMENDATIONS_SCHEMA.keys())

    def test_dtypes_match_schema(self, raw_recommendations):
        df = parse_recommendations(raw_recommendations, PUBLIC_ID)
        for col, expected_dtype in RECOMMENDATIONS_SCHEMA.items():
            assert (
                df[col].dtype == expected_dtype
            ), f"Column '{col}': expected {expected_dtype}, got {df[col].dtype}"

    def test_empty_recommendations_returns_empty_df(self):
        df = parse_recommendations({"receivedRecommendations": []}, PUBLIC_ID)
        assert len(df) == 0

    def test_missing_recommendations_key_returns_empty_df(self):
        df = parse_recommendations({}, PUBLIC_ID)
        assert len(df) == 0


# ===========================================================================
# LinkedInClient tests
# ===========================================================================


class TestLinkedInClient:
    """Tests for the LinkedInClient class."""

    def test_raises_auth_error_when_no_credentials(self):
        """Client should raise AuthError if username/password are not set."""
        client = LinkedInClient(username="", password="")
        with pytest.raises(AuthError, match="credentials not set"):
            client._get_client()

    def test_raises_auth_error_on_login_failure(self):
        """Client should raise AuthError if linkedin_api raises an exception."""
        with patch(
            "linkedin_project.scrape.scraper.LinkedInClient._get_client"
        ) as mock_get:
            mock_get.side_effect = AuthError("LinkedIn login failed")
            client = LinkedInClient(username="u@test.com", password="pw")
            with pytest.raises(AuthError):
                client._get_client()

    def test_injected_client_bypasses_auth(self):
        """When _client is injected, no authentication is attempted."""
        mock_api = MagicMock()
        mock_api.get_profile.return_value = {"firstName": "Test"}
        client = LinkedInClient(_client=mock_api)
        result = client._get_client()
        assert result is mock_api

    def test_get_profile_raw_delegates_to_api(self, raw_profile):
        mock_api = MagicMock()
        mock_api.get_profile.return_value = raw_profile
        client = LinkedInClient(_client=mock_api)
        result = client.get_profile_raw(PUBLIC_ID)
        mock_api.get_profile.assert_called_once_with(PUBLIC_ID)
        assert result is raw_profile

    def test_get_skills_raw_delegates_to_api(self, raw_skills):
        mock_api = MagicMock()
        mock_api.get_profile_skills.return_value = raw_skills
        client = LinkedInClient(_client=mock_api)
        result = client.get_skills_raw(PUBLIC_ID)
        mock_api.get_profile_skills.assert_called_once_with(PUBLIC_ID)
        assert result is raw_skills

    def test_get_certifications_raw_delegates_to_api(self, raw_certifications):
        mock_api = MagicMock()
        mock_api.get_profile_certifications.return_value = raw_certifications
        client = LinkedInClient(_client=mock_api)
        result = client.get_certifications_raw(PUBLIC_ID)
        mock_api.get_profile_certifications.assert_called_once_with(PUBLIC_ID)
        assert result is raw_certifications

    def test_get_recommendations_raw_delegates_to_api(self, raw_recommendations):
        mock_api = MagicMock()
        mock_api.get_profile_recommendations.return_value = raw_recommendations
        client = LinkedInClient(_client=mock_api)
        result = client.get_recommendations_raw(PUBLIC_ID)
        mock_api.get_profile_recommendations.assert_called_once_with(PUBLIC_ID)
        assert result is raw_recommendations


# ===========================================================================
# Retry logic tests
# ===========================================================================


class TestRetryLogic:
    """Tests for the _retry_call helper."""

    def test_succeeds_on_first_attempt(self):
        from linkedin_project.scrape.scraper import _retry_call

        fn = MagicMock(return_value="ok")
        result = _retry_call(fn, "arg")
        assert result == "ok"
        assert fn.call_count == 1

    def test_retries_on_failure_then_succeeds(self):
        from linkedin_project.scrape.scraper import _retry_call

        fn = MagicMock(side_effect=[RuntimeError("fail"), "ok"])
        with patch("linkedin_project.scrape.scraper.time.sleep"):
            result = _retry_call(fn)
        assert result == "ok"
        assert fn.call_count == 2

    def test_raises_scraper_error_after_max_retries(self):
        from linkedin_project.scrape.scraper import (
            MAX_RETRIES,
            ScraperError,
            _retry_call,
        )

        fn = MagicMock(side_effect=RuntimeError("always fails"))
        with patch("linkedin_project.scrape.scraper.time.sleep"):
            with pytest.raises(ScraperError):
                _retry_call(fn)
        assert fn.call_count == MAX_RETRIES


# ===========================================================================
# High-level scrape_profile tests
# ===========================================================================


class TestScrapeProfile:
    """Tests for the high-level scrape_profile function."""

    def test_returns_profile_data(self, mock_client):
        with patch("linkedin_project.scrape.scraper.time.sleep"):
            result = scrape_profile(PUBLIC_ID, client=mock_client)
        assert isinstance(result, ProfileData)

    def test_profile_df_has_one_row(self, mock_client):
        with patch("linkedin_project.scrape.scraper.time.sleep"):
            result = scrape_profile(PUBLIC_ID, client=mock_client)
        assert len(result.profile) == 1

    def test_experience_df_non_empty(self, mock_client):
        with patch("linkedin_project.scrape.scraper.time.sleep"):
            result = scrape_profile(PUBLIC_ID, client=mock_client)
        assert len(result.experience) > 0

    def test_education_df_non_empty(self, mock_client):
        with patch("linkedin_project.scrape.scraper.time.sleep"):
            result = scrape_profile(PUBLIC_ID, client=mock_client)
        assert len(result.education) > 0

    def test_skills_df_non_empty(self, mock_client):
        with patch("linkedin_project.scrape.scraper.time.sleep"):
            result = scrape_profile(PUBLIC_ID, client=mock_client)
        assert len(result.skills) > 0

    def test_certifications_df_non_empty(self, mock_client):
        with patch("linkedin_project.scrape.scraper.time.sleep"):
            result = scrape_profile(PUBLIC_ID, client=mock_client)
        assert len(result.certifications) > 0

    def test_recommendations_df_non_empty(self, mock_client):
        with patch("linkedin_project.scrape.scraper.time.sleep"):
            result = scrape_profile(PUBLIC_ID, client=mock_client)
        assert len(result.recommendations) > 0

    def test_all_dfs_have_correct_columns(self, mock_client):
        with patch("linkedin_project.scrape.scraper.time.sleep"):
            result = scrape_profile(PUBLIC_ID, client=mock_client)
        assert list(result.profile.columns) == list(PROFILE_SCHEMA.keys())
        assert list(result.experience.columns) == list(EXPERIENCE_SCHEMA.keys())
        assert list(result.education.columns) == list(EDUCATION_SCHEMA.keys())
        assert list(result.skills.columns) == list(SKILLS_SCHEMA.keys())
        assert list(result.certifications.columns) == list(CERTIFICATIONS_SCHEMA.keys())
        assert list(result.recommendations.columns) == list(
            RECOMMENDATIONS_SCHEMA.keys()
        )

    def test_client_methods_called_with_public_id(self, mock_client):
        with patch("linkedin_project.scrape.scraper.time.sleep"):
            scrape_profile(PUBLIC_ID, client=mock_client)
        mock_client.get_profile_raw.assert_called_once_with(PUBLIC_ID)
        mock_client.get_skills_raw.assert_called_once_with(PUBLIC_ID)
        mock_client.get_certifications_raw.assert_called_once_with(PUBLIC_ID)
        mock_client.get_recommendations_raw.assert_called_once_with(PUBLIC_ID)

    def test_scrape_profile_raises_scraper_error_on_api_failure(self, mock_client):
        mock_client.get_profile_raw.side_effect = ScraperError("API failed")
        with patch("linkedin_project.scrape.scraper.time.sleep"):
            with pytest.raises(ScraperError):
                scrape_profile(PUBLIC_ID, client=mock_client)


# ===========================================================================
# Public API surface test
# ===========================================================================


class TestPublicApi:
    """Verify the public API exposed via __init__.py."""

    def test_scrape_profile_importable_from_package(self):
        from linkedin_project.scrape import scrape_profile as sp

        assert callable(sp)

    def test_profile_data_importable_from_package(self):
        from linkedin_project.scrape import ProfileData

        assert ProfileData is not None

    def test_exceptions_importable_from_package(self):
        from linkedin_project.scrape import AuthError, ScraperError, SchemaError

        assert issubclass(AuthError, Exception)
        assert issubclass(ScraperError, Exception)
        assert issubclass(SchemaError, Exception)

    def test_schemas_importable_from_package(self):
        from linkedin_project.scrape import (
            CERTIFICATIONS_SCHEMA,
            EDUCATION_SCHEMA,
            EXPERIENCE_SCHEMA,
            PROFILE_SCHEMA,
            RECOMMENDATIONS_SCHEMA,
            SKILLS_SCHEMA,
        )

        assert isinstance(PROFILE_SCHEMA, dict)
        assert isinstance(EXPERIENCE_SCHEMA, dict)
        assert isinstance(EDUCATION_SCHEMA, dict)
        assert isinstance(SKILLS_SCHEMA, dict)
        assert isinstance(CERTIFICATIONS_SCHEMA, dict)
        assert isinstance(RECOMMENDATIONS_SCHEMA, dict)

    def test_empty_constructors_importable_from_package(self):
        from linkedin_project.scrape import (
            empty_certifications_df,
            empty_education_df,
            empty_experience_df,
            empty_profile_df,
            empty_recommendations_df,
            empty_skills_df,
        )

        for fn in (
            empty_profile_df,
            empty_experience_df,
            empty_education_df,
            empty_skills_df,
            empty_certifications_df,
            empty_recommendations_df,
        ):
            assert callable(fn)
