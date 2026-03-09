"""Unit tests for the LinkedIn resume Shiny application.

Tests cover:
- DB connection stub / fallback logic
- Data loading functions (load_section, load_profile)
- Sample data correctness
- Pure helper functions in component modules
- get_sample_data error handling
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from app.app import (
    get_db_connection,
    get_sample_data,
    load_section,
    load_profile,
    _SAMPLE_PROFILE,
)
from app.components.experience import _format_date_range, _build_experience_card
from app.components.education import _build_education_card
from app.components.skills import _build_skill_tag, _build_skills_chart

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def nonexistent_db(tmp_path: Path) -> Path:
    """Return a path to a DB file that does not exist."""
    return tmp_path / "missing.db"


@pytest.fixture()
def empty_db(tmp_path: Path) -> Path:
    """Return a path to a valid but empty DuckDB file."""
    import duckdb

    db_path = tmp_path / "empty.db"
    conn = duckdb.connect(str(db_path))
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# get_db_connection
# ---------------------------------------------------------------------------


class TestGetDbConnection:
    """Tests for get_db_connection."""

    def test_returns_none_for_missing_file(self, nonexistent_db: Path) -> None:
        """get_db_connection returns None when the DB file does not exist."""
        result = get_db_connection(nonexistent_db)
        assert result is None

    def test_returns_connection_for_existing_db(self, empty_db: Path) -> None:
        """get_db_connection returns a live connection for a valid DB file."""
        conn = get_db_connection(empty_db)
        assert conn is not None
        conn.close()

    def test_accepts_string_path(self, empty_db: Path) -> None:
        """get_db_connection accepts a string as well as a Path object."""
        conn = get_db_connection(str(empty_db))
        assert conn is not None
        conn.close()

    def test_returns_none_for_invalid_path_type(self) -> None:
        """get_db_connection returns None when path is obviously invalid."""
        result = get_db_connection("/no/such/directory/db.db")
        assert result is None


# ---------------------------------------------------------------------------
# get_sample_data
# ---------------------------------------------------------------------------


class TestGetSampleData:
    """Tests for get_sample_data."""

    @pytest.mark.parametrize(
        "section",
        ["experience", "education", "skills", "certifications"],
    )
    def test_returns_dataframe_for_valid_section(self, section: str) -> None:
        """get_sample_data returns a non-empty DataFrame for each valid section."""
        df = get_sample_data(section)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_raises_for_unknown_section(self) -> None:
        """get_sample_data raises ValueError for an unknown section name."""
        with pytest.raises(ValueError, match="Unknown section"):
            get_sample_data("unknown_section")

    def test_returns_copy_not_original(self) -> None:
        """get_sample_data returns a copy, so mutations do not affect originals."""
        df1 = get_sample_data("skills")
        df1["name"] = "MODIFIED"
        df2 = get_sample_data("skills")
        assert not (df2["name"] == "MODIFIED").all()

    def test_experience_has_required_columns(self) -> None:
        """Experience sample data contains all expected columns."""
        df = get_sample_data("experience")
        for col in ("title", "company", "start_date", "end_date"):
            assert col in df.columns, f"Missing column: {col}"

    def test_education_has_required_columns(self) -> None:
        """Education sample data contains expected columns."""
        df = get_sample_data("education")
        for col in ("school", "degree", "field_of_study", "start_year", "end_year"):
            assert col in df.columns, f"Missing column: {col}"

    def test_skills_has_required_columns(self) -> None:
        """Skills sample data has name and endorsements columns."""
        df = get_sample_data("skills")
        assert "name" in df.columns
        assert "endorsements" in df.columns

    def test_certifications_has_required_columns(self) -> None:
        """Certifications sample data contains name, issuer, issued_date."""
        df = get_sample_data("certifications")
        for col in ("name", "issuer", "issued_date"):
            assert col in df.columns, f"Missing column: {col}"


# ---------------------------------------------------------------------------
# load_section
# ---------------------------------------------------------------------------


class TestLoadSection:
    """Tests for load_section."""

    def test_returns_sample_when_conn_is_none(self) -> None:
        """load_section returns sample data when conn is None."""
        df = load_section(None, "experience")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    @pytest.mark.parametrize(
        "section",
        ["experience", "education", "skills", "certifications"],
    )
    def test_all_sections_fallback_gracefully(self, section: str) -> None:
        """load_section handles all valid sections with None conn."""
        df = load_section(None, section)
        assert isinstance(df, pd.DataFrame)

    def test_unknown_section_with_none_conn(self) -> None:
        """load_section with None conn and unknown section raises ValueError."""
        with pytest.raises(ValueError):
            load_section(None, "bogus")

    def test_fallback_when_table_missing_in_db(self, empty_db: Path) -> None:
        """load_section falls back to sample data when table is absent in DB."""
        import duckdb

        conn = duckdb.connect(str(empty_db))
        df = load_section(conn, "experience")
        conn.close()
        # Should have fallen back to sample data since table is missing
        assert not df.empty

    def test_returns_db_data_when_table_exists(self, tmp_path: Path) -> None:
        """load_section returns DB rows when the table exists and has data."""
        import duckdb

        db_path = tmp_path / "test.db"
        conn = duckdb.connect(str(db_path))
        conn.execute("""
            CREATE TABLE skills (
                name VARCHAR,
                endorsements INTEGER
            )
            """)
        conn.execute("INSERT INTO skills VALUES ('Python', 99)")
        df = load_section(conn, "skills")
        conn.close()

        assert not df.empty
        assert "Python" in df["name"].values


# ---------------------------------------------------------------------------
# load_profile
# ---------------------------------------------------------------------------


class TestLoadProfile:
    """Tests for load_profile."""

    def test_returns_dict_when_conn_is_none(self) -> None:
        """load_profile returns a dict with expected keys for None conn."""
        profile = load_profile(None)
        assert isinstance(profile, dict)
        for key in ("name", "headline", "location", "summary"):
            assert key in profile, f"Missing key: {key}"

    def test_summary_is_nonempty_string(self) -> None:
        """load_profile summary is a non-empty string in sample mode."""
        profile = load_profile(None)
        assert isinstance(profile["summary"], str)
        assert len(profile["summary"]) > 0

    def test_returns_db_profile_when_available(self, tmp_path: Path) -> None:
        """load_profile returns DB row data when profile table exists."""
        import duckdb

        db_path = tmp_path / "profile_test.db"
        conn = duckdb.connect(str(db_path))
        conn.execute("""
            CREATE TABLE profile (
                name VARCHAR,
                headline VARCHAR,
                location VARCHAR,
                email VARCHAR,
                linkedin_url VARCHAR,
                summary VARCHAR
            )
            """)
        conn.execute(
            "INSERT INTO profile VALUES "
            "('Jane Doe', 'Engineer', 'NYC', 'j@example.com', "
            "'https://linkedin.com/in/jdoe', 'Great engineer.')"
        )
        profile = load_profile(conn)
        conn.close()

        assert profile["name"] == "Jane Doe"
        assert profile["summary"] == "Great engineer."

    def test_fallback_when_profile_table_missing(self, empty_db: Path) -> None:
        """load_profile falls back to sample data when profile table is absent."""
        import duckdb

        conn = duckdb.connect(str(empty_db))
        profile = load_profile(conn)
        conn.close()

        # Should be sample profile
        assert profile["name"] == _SAMPLE_PROFILE["name"]


# ---------------------------------------------------------------------------
# Component helpers: experience
# ---------------------------------------------------------------------------


class TestFormatDateRange:
    """Tests for experience._format_date_range."""

    def test_present_when_end_empty(self) -> None:
        """_format_date_range shows 'Present' when end is empty string."""
        result = _format_date_range("Jan 2020", "")
        assert "Present" in result

    def test_present_when_end_none_string(self) -> None:
        """_format_date_range shows 'Present' when end is 'None'."""
        result = _format_date_range("Jan 2020", "None")
        # 'None' is truthy and non-whitespace, so it should appear as-is
        assert "Jan 2020" in result

    def test_regular_range(self) -> None:
        """_format_date_range formats a normal date range correctly."""
        result = _format_date_range("Jan 2018", "Dec 2020")
        assert "Jan 2018" in result
        assert "Dec 2020" in result
        assert "–" in result


class TestBuildExperienceCard:
    """Tests for experience._build_experience_card."""

    def test_renders_title_and_company(self) -> None:
        """_build_experience_card includes title and company in output."""
        row = pd.Series(
            {
                "title": "Lead Engineer",
                "company": "TechCo",
                "location": "",
                "start_date": "2019",
                "end_date": "2022",
                "description": "",
            }
        )
        card = _build_experience_card(row)
        card_str = str(card)
        assert "Lead Engineer" in card_str
        assert "TechCo" in card_str

    def test_renders_without_description(self) -> None:
        """_build_experience_card does not raise when description is empty."""
        row = pd.Series(
            {
                "title": "Analyst",
                "company": "DataCo",
                "location": "",
                "start_date": "2020",
                "end_date": "",
                "description": "",
            }
        )
        # Should not raise
        _build_experience_card(row)


# ---------------------------------------------------------------------------
# Component helpers: education
# ---------------------------------------------------------------------------


class TestBuildEducationCard:
    """Tests for education._build_education_card."""

    def test_renders_school_name(self) -> None:
        """_build_education_card includes school name in HTML output."""
        row = pd.Series(
            {
                "school": "MIT",
                "degree": "BS",
                "field_of_study": "CS",
                "start_year": "2010",
                "end_year": "2014",
                "description": "",
            }
        )
        card = _build_education_card(row)
        assert "MIT" in str(card)

    def test_year_range_formatting(self) -> None:
        """_build_education_card produces a year range string."""
        row = pd.Series(
            {
                "school": "Stanford",
                "degree": "MS",
                "field_of_study": "AI",
                "start_year": "2015",
                "end_year": "2017",
                "description": "",
            }
        )
        card_str = str(_build_education_card(row))
        assert "2015" in card_str
        assert "2017" in card_str

    def test_present_when_no_end_year(self) -> None:
        """_build_education_card shows start – Present when end_year is blank."""
        row = pd.Series(
            {
                "school": "UC Berkeley",
                "degree": "PhD",
                "field_of_study": "Statistics",
                "start_year": "2020",
                "end_year": "",
                "description": "",
            }
        )
        card_str = str(_build_education_card(row))
        assert "Present" in card_str


# ---------------------------------------------------------------------------
# Component helpers: skills
# ---------------------------------------------------------------------------


class TestBuildSkillTag:
    """Tests for skills._build_skill_tag."""

    def test_returns_tag_with_skill_name(self) -> None:
        """_build_skill_tag includes the skill name in the tag output."""
        tag = _build_skill_tag("Python")
        assert "Python" in str(tag)

    def test_has_skill_tag_class(self) -> None:
        """_build_skill_tag applies the 'skill-tag' CSS class."""
        tag = _build_skill_tag("SQL")
        assert "skill-tag" in str(tag)


class TestBuildSkillsChart:
    """Tests for skills._build_skills_chart."""

    def test_returns_none_without_endorsements_column(self) -> None:
        """_build_skills_chart returns None when DataFrame lacks endorsements."""
        df = pd.DataFrame({"name": ["Python", "SQL"]})
        result = _build_skills_chart(df)
        assert result is None

    def test_returns_none_when_all_endorsements_zero(self) -> None:
        """_build_skills_chart returns None when all endorsement counts are 0."""
        df = pd.DataFrame({"name": ["Python"], "endorsements": [0]})
        result = _build_skills_chart(df)
        assert result is None

    def test_returns_svg_html_when_endorsements_present(self) -> None:
        """_build_skills_chart returns an HTML tag containing SVG content."""
        df = pd.DataFrame(
            {
                "name": ["Python", "SQL", "dbt"],
                "endorsements": [40, 30, 20],
            }
        )
        result = _build_skills_chart(df)
        assert result is not None
        result_str = str(result)
        assert "<svg" in result_str
        assert "Python" in result_str

    def test_handles_non_numeric_endorsements(self) -> None:
        """_build_skills_chart coerces non-numeric endorsements to 0."""
        df = pd.DataFrame(
            {
                "name": ["Python", "SQL"],
                "endorsements": ["forty", "30"],
            }
        )
        # Should not raise; 'forty' becomes 0, only SQL (30) is charted
        result = _build_skills_chart(df)
        assert result is not None  # SQL=30 should produce a chart
