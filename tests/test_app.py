"""Tests for the Nebula Dark Shiny resume app.

Loads app/app.py directly via importlib to avoid package-path resolution
issues when pytest's assertion rewriter runs before sys.path is set up.
"""

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

# Load app/app.py as a module by file path (avoids 'app is not a package' issue)
_APP_FILE = Path(__file__).parent.parent / "app" / "app.py"
_spec = importlib.util.spec_from_file_location("linkedin_resume_app", _APP_FILE)
_app_mod = importlib.util.module_from_spec(_spec)
sys.modules["linkedin_resume_app"] = _app_mod
_spec.loader.exec_module(_app_mod)

# Bind names used in tests
_build_cert_card = _app_mod._build_cert_card
_build_education_card = _app_mod._build_education_card
_build_experience_card = _app_mod._build_experience_card
_build_skill_bars = _app_mod._build_skill_bars
_build_skill_tags = _app_mod._build_skill_tags
_extract_year = _app_mod._extract_year
get_db_connection = _app_mod.get_db_connection
get_sample_data = _app_mod.get_sample_data
load_profile = _app_mod.load_profile
load_section = _app_mod.load_section


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------


class TestGetDbConnection:
    def test_missing_file_returns_none(self, tmp_path):
        assert get_db_connection(tmp_path / "no.db") is None

    def test_string_path_missing_returns_none(self, tmp_path):
        assert get_db_connection(str(tmp_path / "no.db")) is None

    def test_valid_db_returns_connection(self, tmp_path):
        import duckdb

        db = tmp_path / "test.db"
        duckdb.connect(str(db)).close()
        conn = get_db_connection(db)
        assert conn is not None
        conn.close()


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------


class TestGetSampleData:
    @pytest.mark.parametrize(
        "section,expected_col",
        [
            ("experience", "title"),
            ("education", "school"),
            ("skills", "name"),
            ("certifications", "name"),
        ],
    )
    def test_returns_dataframe_with_column(self, section, expected_col):
        df = get_sample_data(section)
        assert isinstance(df, pd.DataFrame)
        assert expected_col in df.columns

    def test_unknown_section_raises(self):
        with pytest.raises(ValueError):
            get_sample_data("foobar")

    def test_returns_copy(self):
        a = get_sample_data("skills")
        b = get_sample_data("skills")
        assert a is not b


# ---------------------------------------------------------------------------
# Extract year
# ---------------------------------------------------------------------------


class TestExtractYear:
    def test_iso_string(self):
        s = pd.Series(["2021-01-01"])
        assert _extract_year(s).iloc[0] == "2021"

    def test_none_value(self):
        s = pd.Series([None])
        assert _extract_year(s).iloc[0] == ""

    def test_bad_value(self):
        s = pd.Series(["not-a-date"])
        assert _extract_year(s).iloc[0] == ""


# ---------------------------------------------------------------------------
# load_section
# ---------------------------------------------------------------------------


class TestLoadSection:
    def test_none_conn_returns_sample(self):
        df = load_section(None, "skills")
        assert not df.empty

    def test_none_conn_unknown_section_raises(self):
        with pytest.raises(ValueError):
            load_section(None, "unknown")

    def test_db_missing_table_falls_back(self, tmp_path):
        import duckdb

        db = tmp_path / "empty.db"
        duckdb.connect(str(db)).close()
        conn2 = get_db_connection(db)
        df = load_section(conn2, "skills")
        assert not df.empty

    def test_education_gets_year_columns(self, tmp_path):
        import duckdb

        db = tmp_path / "edu.db"
        conn = duckdb.connect(str(db))
        conn.execute(
            "CREATE TABLE education (school VARCHAR, degree VARCHAR, "
            "field_of_study VARCHAR, start_date VARCHAR, end_date VARCHAR)"
        )
        conn.execute(
            "INSERT INTO education VALUES "
            "('UofC', 'BSc', 'CS', '2010-01-01', '2014-05-01')"
        )
        conn.close()
        rconn = get_db_connection(db)
        df = load_section(rconn, "education")
        assert "start_year" in df.columns
        assert df["start_year"].iloc[0] == "2010"


# ---------------------------------------------------------------------------
# load_profile
# ---------------------------------------------------------------------------


class TestLoadProfile:
    def test_none_returns_dict(self):
        p = load_profile(None)
        assert isinstance(p, dict)
        assert "name" in p

    def test_db_fallback(self, tmp_path):
        import duckdb

        db = tmp_path / "empty.db"
        duckdb.connect(str(db)).close()
        conn = get_db_connection(db)
        p = load_profile(conn)
        assert "name" in p


# ---------------------------------------------------------------------------
# Component builders
# ---------------------------------------------------------------------------


class TestBuildExperienceCard:
    def test_renders_title(self):
        row = pd.Series(
            {
                "title": "Engineer",
                "company": "ACME",
                "location": "YYC",
                "start_date": "2022-01-01",
                "end_date": None,
                "is_current": True,
                "description": "Did stuff.",
            }
        )
        html = str(_build_experience_card(row))
        assert "Engineer" in html
        assert "ACME" in html
        assert "Present" in html

    def test_past_role_shows_end_date(self):
        row = pd.Series(
            {
                "title": "Analyst",
                "company": "Co",
                "location": "",
                "start_date": "2019-01-01",
                "end_date": "2021-12-01",
                "is_current": False,
                "description": "",
            }
        )
        html = str(_build_experience_card(row))
        assert "Dec 2021" in html

    def test_empty_description_ok(self):
        row = pd.Series(
            {
                "title": "X",
                "company": "Y",
                "location": "",
                "start_date": None,
                "end_date": None,
                "is_current": False,
                "description": "",
            }
        )
        _build_experience_card(row)  # should not raise


class TestBuildEducationCard:
    def test_renders_degree_and_school(self):
        row = pd.Series(
            {
                "degree": "BSc",
                "field_of_study": "CS",
                "school": "UofC",
                "start_year": "2010",
                "end_year": "2014",
            }
        )
        html = str(_build_education_card(row))
        assert "BSc" in html
        assert "UofC" in html

    def test_handles_missing_fields(self):
        row = pd.Series({"degree": "MSc"})
        _build_education_card(row)  # should not raise


class TestBuildSkillBars:
    def test_renders_bars(self):
        df = pd.DataFrame(
            [
                {"name": "Python", "endorsements": 50},
                {"name": "SQL", "endorsements": 40},
            ]
        )
        html = str(_build_skill_bars(df))
        assert "Python" in html
        assert "skill-bar-fill" in html

    def test_empty_df_returns_span(self):
        result = _build_skill_bars(pd.DataFrame())
        assert "span" in str(result)

    def test_zero_endorsements_no_error(self):
        df = pd.DataFrame([{"name": "X", "endorsements": 0}])
        _build_skill_bars(df)

    def test_uses_skill_name_column(self):
        df = pd.DataFrame([{"skill_name": "dbt", "endorsement_count": 30}])
        html = str(_build_skill_bars(df))
        assert "dbt" in html


class TestBuildSkillTags:
    def test_renders_tags(self):
        df = pd.DataFrame([{"name": "Docker"}, {"name": "K8s"}])
        html = str(_build_skill_tags(df))
        assert "Docker" in html
        assert "skill-tag" in html


class TestBuildCertCard:
    def test_renders_cert_name(self):
        row = pd.Series(
            {
                "name": "AWS SAA",
                "authority": "Amazon Web Services",
                "issued_date": "2023-04",
            }
        )
        html = str(_build_cert_card(row))
        assert "AWS SAA" in html
        assert "Amazon Web Services" in html

    def test_aws_icon(self):
        row = pd.Series({"name": "AWS Cert", "authority": "Amazon", "issued_date": ""})
        html = str(_build_cert_card(row))
        assert "☁️" in html

    def test_missing_fields_ok(self):
        _build_cert_card(pd.Series({"name": "Cert"}))
