"""Unit tests for the storage sub-package."""

from __future__ import annotations

import pytest
import pandas as pd

from linkedin_project.storage.database import (
    connect,
    create_tables,
    query_section,
    upsert_profile,
    _validate_section,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mem_conn():
    """Return an in-memory DuckDB connection with all tables created."""
    conn = connect(":memory:")
    create_tables(conn)
    yield conn
    conn.close()


def _experience_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["exp1", "exp2"],
            "company_name": ["Acme", "Globex"],
            "title": ["Engineer", "Analyst"],
            "location": ["NYC", "Remote"],
            "start_date": pd.to_datetime(["2020-01-01", "2018-06-01"]),
            "end_date": pd.to_datetime(["2023-12-31", "2020-01-01"]),
            "description": ["Built stuff", "Analyzed things"],
            "is_current": [False, True],
        }
    )


def _education_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["edu1"],
            "school_name": ["MIT"],
            "degree": ["B.Sc."],
            "field_of_study": ["Computer Science"],
            "start_date": pd.to_datetime(["2014-09-01"]),
            "end_date": pd.to_datetime(["2018-05-31"]),
            "grade": [3.9],
        }
    )


def _skills_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["sk1", "sk2"],
            "skill_name": ["Python", "SQL"],
            "endorsement_count": [15, 8],
        }
    )


def _certifications_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["cert1"],
            "cert_name": ["AWS Solutions Architect"],
            "authority": ["Amazon Web Services"],
            "issued_date": pd.to_datetime(["2022-03-15"]),
            "expiry_date": pd.to_datetime(["2025-03-15"]),
            "credential_id": ["CERT-123"],
        }
    )


def _summary_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["sum1"],
            "text": ["Experienced data scientist."],
            "last_updated": pd.to_datetime(["2024-01-01"]),
        }
    )


# ---------------------------------------------------------------------------
# Tests: connect
# ---------------------------------------------------------------------------


class TestConnect:
    def test_returns_connection(self) -> None:
        conn = connect(":memory:")
        assert conn is not None
        conn.close()

    def test_in_memory_connection(self) -> None:
        conn = connect(":memory:")
        result = conn.execute("SELECT 42 AS answer").df()
        assert result["answer"].iloc[0] == 42
        conn.close()

    def test_file_based_connection(self, tmp_path) -> None:
        db_path = str(tmp_path / "subdir" / "test.duckdb")
        conn = connect(db_path)
        assert conn is not None
        conn.close()


# ---------------------------------------------------------------------------
# Tests: create_tables
# ---------------------------------------------------------------------------


class TestCreateTables:
    def test_creates_experience_table(self, mem_conn) -> None:
        result = mem_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'experience'"
        ).df()
        assert len(result) == 1

    def test_creates_education_table(self, mem_conn) -> None:
        result = mem_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'education'"
        ).df()
        assert len(result) == 1

    def test_creates_skills_table(self, mem_conn) -> None:
        result = mem_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'skills'"
        ).df()
        assert len(result) == 1

    def test_creates_certifications_table(self, mem_conn) -> None:
        result = mem_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'certifications'"
        ).df()
        assert len(result) == 1

    def test_creates_summary_table(self, mem_conn) -> None:
        result = mem_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'summary'"
        ).df()
        assert len(result) == 1

    def test_idempotent(self, mem_conn) -> None:
        # Calling create_tables again should not raise an error
        create_tables(mem_conn)


# ---------------------------------------------------------------------------
# Tests: _validate_section
# ---------------------------------------------------------------------------


class TestValidateSection:
    def test_valid_sections(self) -> None:
        for section in [
            "experience",
            "education",
            "skills",
            "certifications",
            "summary",
        ]:
            _validate_section(section)  # should not raise

    def test_invalid_section_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid section"):
            _validate_section("unknown_table")

    def test_sql_injection_attempt_raises(self) -> None:
        with pytest.raises(ValueError):
            _validate_section("experience; DROP TABLE experience; --")


# ---------------------------------------------------------------------------
# Tests: upsert_profile
# ---------------------------------------------------------------------------


class TestUpsertProfile:
    def test_inserts_experience(self, mem_conn) -> None:
        df = _experience_df()
        upsert_profile(mem_conn, "experience", df)
        result = mem_conn.execute("SELECT COUNT(*) AS n FROM experience").df()
        assert result["n"].iloc[0] == 2

    def test_inserts_education(self, mem_conn) -> None:
        upsert_profile(mem_conn, "education", _education_df())
        result = mem_conn.execute("SELECT COUNT(*) AS n FROM education").df()
        assert result["n"].iloc[0] == 1

    def test_inserts_skills(self, mem_conn) -> None:
        upsert_profile(mem_conn, "skills", _skills_df())
        result = mem_conn.execute("SELECT COUNT(*) AS n FROM skills").df()
        assert result["n"].iloc[0] == 2

    def test_inserts_certifications(self, mem_conn) -> None:
        upsert_profile(mem_conn, "certifications", _certifications_df())
        result = mem_conn.execute("SELECT COUNT(*) AS n FROM certifications").df()
        assert result["n"].iloc[0] == 1

    def test_inserts_summary(self, mem_conn) -> None:
        upsert_profile(mem_conn, "summary", _summary_df())
        result = mem_conn.execute("SELECT COUNT(*) AS n FROM summary").df()
        assert result["n"].iloc[0] == 1

    def test_upsert_replaces_existing(self, mem_conn) -> None:
        df = _experience_df()
        upsert_profile(mem_conn, "experience", df)

        # Update one row
        updated = df.copy()
        updated.loc[updated["id"] == "exp1", "title"] = "Senior Engineer"
        upsert_profile(mem_conn, "experience", updated)

        result = mem_conn.execute(  # noqa: S608
            "SELECT title FROM experience WHERE id = 'exp1'"
        ).df()
        assert result["title"].iloc[0] == "Senior Engineer"
        # Count should still be 2 (not 4)
        count = mem_conn.execute("SELECT COUNT(*) AS n FROM experience").df()
        assert count["n"].iloc[0] == 2

    def test_empty_dataframe_is_noop(self, mem_conn) -> None:
        df = _experience_df().iloc[0:0]  # empty with correct schema
        upsert_profile(mem_conn, "experience", df)
        result = mem_conn.execute("SELECT COUNT(*) AS n FROM experience").df()
        assert result["n"].iloc[0] == 0

    def test_invalid_section_raises(self, mem_conn) -> None:
        df = _experience_df()
        with pytest.raises(ValueError, match="Invalid section"):
            upsert_profile(mem_conn, "bad_table", df)


# ---------------------------------------------------------------------------
# Tests: query_section
# ---------------------------------------------------------------------------


class TestQuerySection:
    def test_returns_all_rows_no_filter(self, mem_conn) -> None:
        upsert_profile(mem_conn, "experience", _experience_df())
        result = query_section(mem_conn, "experience")
        assert len(result) == 2

    def test_returns_dataframe(self, mem_conn) -> None:
        upsert_profile(mem_conn, "skills", _skills_df())
        result = query_section(mem_conn, "skills")
        assert isinstance(result, pd.DataFrame)

    def test_filter_by_company(self, mem_conn) -> None:
        upsert_profile(mem_conn, "experience", _experience_df())
        result = query_section(mem_conn, "experience", filters={"company_name": "Acme"})
        assert len(result) == 1
        assert result["company_name"].iloc[0] == "Acme"

    def test_filter_returns_empty_when_no_match(self, mem_conn) -> None:
        upsert_profile(mem_conn, "experience", _experience_df())
        result = query_section(
            mem_conn, "experience", filters={"company_name": "Nonexistent Co"}
        )
        assert len(result) == 0

    def test_filter_multiple_conditions(self, mem_conn) -> None:
        upsert_profile(mem_conn, "experience", _experience_df())
        result = query_section(
            mem_conn,
            "experience",
            filters={"company_name": "Acme", "title": "Engineer"},
        )
        assert len(result) == 1

    def test_invalid_section_raises(self, mem_conn) -> None:
        with pytest.raises(ValueError, match="Invalid section"):
            query_section(mem_conn, "hax_table")

    def test_unsafe_column_name_raises(self, mem_conn) -> None:
        upsert_profile(mem_conn, "experience", _experience_df())
        with pytest.raises(ValueError, match="Unsafe column name"):
            query_section(
                mem_conn,
                "experience",
                filters={"company; DROP TABLE experience; --": "x"},
            )

    def test_query_education(self, mem_conn) -> None:
        upsert_profile(mem_conn, "education", _education_df())
        result = query_section(mem_conn, "education")
        assert len(result) == 1

    def test_query_certifications_with_filter(self, mem_conn) -> None:
        upsert_profile(mem_conn, "certifications", _certifications_df())
        result = query_section(
            mem_conn, "certifications", filters={"credential_id": "CERT-123"}
        )
        assert len(result) == 1

    def test_query_summary(self, mem_conn) -> None:
        upsert_profile(mem_conn, "summary", _summary_df())
        result = query_section(mem_conn, "summary")
        assert "text" in result.columns
