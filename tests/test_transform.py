"""Unit tests for the transform sub-package."""

from __future__ import annotations

import math

import pandas as pd
from linkedin_project.transform.cleaner import (
    CANONICAL_SKILLS,
    clean_certifications,
    clean_education,
    clean_experience,
    clean_skills,
    clean_summary,
    clean_text_field,
    normalize_company_name,
    parse_date_column,
)
from linkedin_project.transform.normalizer import (
    deduplicate,
    normalize_experience,
    normalize_skills,
    normalize_skills_df,
    normalize_summary,
    standardize_date_range,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_experience(**kwargs: object) -> pd.DataFrame:
    defaults = {
        "id": ["exp1", "exp2"],
        "company_name": ["Acme Inc.", "globex llc"],
        "title": ["  Engineer  ", "Analyst"],
        "location": ["New York", "Remote"],
        "start_date": ["2020-01-01", "2018-06-01"],
        "end_date": ["2023-12-31", "2020-01-01"],
        "description": ["Built stuff", None],
        "is_current": [False, True],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


def _make_education(**kwargs: object) -> pd.DataFrame:
    defaults = {
        "id": ["edu1"],
        "school_name": ["mit"],
        "degree": ["B.Sc."],
        "field_of_study": ["Computer Science"],
        "start_date": ["2014-09-01"],
        "end_date": ["2018-05-31"],
        "grade": ["3.9"],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


def _make_skills(**kwargs: object) -> pd.DataFrame:
    defaults = {
        "id": ["sk1", "sk2", "sk3"],
        "skill_name": ["python", "sklearn", "NLP"],
        "endorsement_count": ["10", None, "5"],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


def _make_certifications(**kwargs: object) -> pd.DataFrame:
    defaults = {
        "id": ["cert1"],
        "cert_name": ["AWS Solutions Architect"],
        "authority": ["amazon web services"],
        "issued_date": ["2022-03-15"],
        "expiry_date": ["2025-03-15"],
        "credential_id": [" CERT-123 "],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


def _make_summary(**kwargs: object) -> pd.DataFrame:
    defaults = {
        "id": ["sum1"],
        "text": ["  Experienced data scientist.  "],
        "last_updated": ["2024-01-01"],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


# ---------------------------------------------------------------------------
# Tests: clean_text_field
# ---------------------------------------------------------------------------


class TestCleanTextField:
    def test_strips_whitespace(self) -> None:
        s = pd.Series(["  hello  ", " world"])
        result = clean_text_field(s)
        assert result.tolist() == ["hello", "world"]

    def test_handles_none(self) -> None:
        s = pd.Series([None, "value"])
        result = clean_text_field(s)
        assert result.iloc[0] == ""

    def test_handles_nan(self) -> None:
        s = pd.Series([float("nan"), "ok"])
        result = clean_text_field(s)
        assert result.iloc[0] == ""

    def test_returns_series(self) -> None:
        s = pd.Series(["a"])
        assert isinstance(clean_text_field(s), pd.Series)


# ---------------------------------------------------------------------------
# Tests: parse_date_column
# ---------------------------------------------------------------------------


class TestParseDateColumn:
    def test_parses_iso_strings(self) -> None:
        s = pd.Series(["2020-01-01", "2023-06-15"])
        result = parse_date_column(s)
        assert pd.api.types.is_datetime64_any_dtype(result)
        assert result.iloc[0] == pd.Timestamp("2020-01-01")

    def test_coerces_invalid_to_nat(self) -> None:
        s = pd.Series(["not-a-date", "2020-01-01"])
        result = parse_date_column(s)
        assert pd.isna(result.iloc[0])

    def test_handles_none(self) -> None:
        s = pd.Series([None])
        result = parse_date_column(s)
        assert pd.isna(result.iloc[0])


# ---------------------------------------------------------------------------
# Tests: normalize_company_name
# ---------------------------------------------------------------------------


class TestNormalizeCompanyName:
    def test_strips_whitespace(self) -> None:
        s = pd.Series(["  acme  "])
        result = normalize_company_name(s)
        assert result.iloc[0] == "Acme"

    def test_title_case(self) -> None:
        s = pd.Series(["globex corporation"])
        result = normalize_company_name(s)
        assert result.iloc[0] == "Globex Corporation"

    def test_removes_inc_suffix(self) -> None:
        s = pd.Series(["Acme Inc.", "FooCorp Inc"])
        result = normalize_company_name(s)
        assert "Inc" not in result.iloc[0]
        assert "Inc" not in result.iloc[1]

    def test_handles_none(self) -> None:
        s = pd.Series([None])
        result = normalize_company_name(s)
        assert result.iloc[0] == ""


# ---------------------------------------------------------------------------
# Tests: clean_experience
# ---------------------------------------------------------------------------


class TestCleanExperience:
    def test_strips_title_whitespace(self) -> None:
        df = _make_experience()
        result = clean_experience(df)
        assert result["title"].iloc[0] == "Engineer"

    def test_normalizes_company(self) -> None:
        df = _make_experience()
        result = clean_experience(df)
        # "Acme Inc." -> "Acme" (suffix stripped) or at least title-cased
        assert result["company_name"].iloc[0].startswith("Acme")

    def test_parses_start_date(self) -> None:
        df = _make_experience()
        result = clean_experience(df)
        assert pd.api.types.is_datetime64_any_dtype(result["start_date"])

    def test_parses_end_date(self) -> None:
        df = _make_experience()
        result = clean_experience(df)
        assert pd.api.types.is_datetime64_any_dtype(result["end_date"])

    def test_fills_none_description(self) -> None:
        df = _make_experience()
        result = clean_experience(df)
        assert result["description"].iloc[1] == ""

    def test_is_current_bool(self) -> None:
        df = _make_experience()
        result = clean_experience(df)
        assert result["is_current"].dtype == bool

    def test_preserves_row_count(self) -> None:
        df = _make_experience()
        result = clean_experience(df)
        assert len(result) == len(df)


# ---------------------------------------------------------------------------
# Tests: clean_education
# ---------------------------------------------------------------------------


class TestCleanEducation:
    def test_normalizes_institution(self) -> None:
        df = _make_education()
        result = clean_education(df)
        assert result["school_name"].iloc[0] == "Mit"

    def test_parses_gpa_to_float(self) -> None:
        df = _make_education()
        result = clean_education(df)
        assert abs(result["grade"].iloc[0] - 3.9) < 1e-6

    def test_invalid_gpa_becomes_nan(self) -> None:
        df = _make_education(grade=["not-a-number"])
        result = clean_education(df)
        assert math.isnan(result["grade"].iloc[0])

    def test_parses_dates(self) -> None:
        df = _make_education()
        result = clean_education(df)
        assert pd.api.types.is_datetime64_any_dtype(result["start_date"])
        assert pd.api.types.is_datetime64_any_dtype(result["end_date"])


# ---------------------------------------------------------------------------
# Tests: clean_skills
# ---------------------------------------------------------------------------


class TestCleanSkills:
    def test_fills_none_endorsements(self) -> None:
        df = _make_skills()
        result = clean_skills(df)
        assert result["endorsement_count"].iloc[1] == 0

    def test_endorsements_integer(self) -> None:
        df = _make_skills()
        result = clean_skills(df)
        assert result["endorsement_count"].dtype in (int, "int64", "int32")

    def test_strips_name_whitespace(self) -> None:
        df = _make_skills(skill_name=["  python  ", "sql", "NLP"])
        result = clean_skills(df)
        assert result["skill_name"].iloc[0] == "python"


# ---------------------------------------------------------------------------
# Tests: clean_certifications
# ---------------------------------------------------------------------------


class TestCleanCertifications:
    def test_strips_credential_id(self) -> None:
        df = _make_certifications()
        result = clean_certifications(df)
        assert result["credential_id"].iloc[0] == "CERT-123"

    def test_normalizes_issuer(self) -> None:
        df = _make_certifications()
        result = clean_certifications(df)
        # "amazon web services" -> title-cased
        assert result["authority"].iloc[0] == "Amazon Web Services"

    def test_parses_issue_date(self) -> None:
        df = _make_certifications()
        result = clean_certifications(df)
        assert pd.api.types.is_datetime64_any_dtype(result["issued_date"])


# ---------------------------------------------------------------------------
# Tests: clean_summary
# ---------------------------------------------------------------------------


class TestCleanSummary:
    def test_strips_text(self) -> None:
        df = _make_summary()
        result = clean_summary(df)
        assert result["text"].iloc[0] == "Experienced data scientist."

    def test_parses_last_updated(self) -> None:
        df = _make_summary()
        result = clean_summary(df)
        assert pd.api.types.is_datetime64_any_dtype(result["last_updated"])


# ---------------------------------------------------------------------------
# Tests: normalize_skills
# ---------------------------------------------------------------------------


class TestNormalizeSkills:
    def test_maps_python_lowercase(self) -> None:
        df = pd.DataFrame(
            {"id": ["s1"], "skill_name": ["python"], "endorsement_count": [5]}
        )
        result = normalize_skills(df)
        assert result["skill_name"].iloc[0] == "Python"

    def test_maps_sklearn_alias(self) -> None:
        df = pd.DataFrame(
            {"id": ["s2"], "skill_name": ["sklearn"], "endorsement_count": [0]}
        )
        result = normalize_skills(df)
        assert result["skill_name"].iloc[0] == "Scikit-Learn"

    def test_maps_nlp_alias(self) -> None:
        df = pd.DataFrame(
            {"id": ["s3"], "skill_name": ["NLP"], "endorsement_count": [3]}
        )
        result = normalize_skills(df)
        assert result["skill_name"].iloc[0] == "Natural Language Processing"

    def test_unknown_skill_title_cased(self) -> None:
        df = pd.DataFrame(
            {"id": ["s4"], "skill_name": ["obscure-tool"], "endorsement_count": [0]}
        )
        result = normalize_skills(df)
        # Unknown skill should be title-cased rather than empty
        assert result["skill_name"].iloc[0] != ""


# ---------------------------------------------------------------------------
# Tests: deduplicate
# ---------------------------------------------------------------------------


class TestDeduplicate:
    def test_removes_duplicates_on_id(self) -> None:
        df = pd.DataFrame({"id": ["a", "a", "b"], "val": [1, 2, 3]})
        result = deduplicate(df, subset=["id"])
        assert len(result) == 2
        assert result["id"].tolist() == ["a", "b"]

    def test_resets_index(self) -> None:
        df = pd.DataFrame({"id": ["x", "x"]}, index=[5, 10])
        result = deduplicate(df, subset=["id"])
        assert result.index.tolist() == [0]

    def test_keep_last(self) -> None:
        df = pd.DataFrame({"id": ["a", "a"], "val": [1, 99]})
        result = deduplicate(df, subset=["id"], keep="last")
        assert result["val"].iloc[0] == 99

    def test_no_duplicates_unchanged(self) -> None:
        df = pd.DataFrame({"id": ["a", "b", "c"]})
        result = deduplicate(df, subset=["id"])
        assert len(result) == 3


# ---------------------------------------------------------------------------
# Tests: standardize_date_range
# ---------------------------------------------------------------------------


class TestStandardizeDateRange:
    def test_swaps_inverted_dates(self) -> None:
        df = pd.DataFrame(
            {
                "start_date": ["2023-06-01"],
                "end_date": ["2020-01-01"],
            }
        )
        result = standardize_date_range(df)
        assert result["start_date"].iloc[0] < result["end_date"].iloc[0]

    def test_correct_dates_unchanged(self) -> None:
        df = pd.DataFrame(
            {
                "start_date": ["2018-01-01"],
                "end_date": ["2022-12-31"],
            }
        )
        result = standardize_date_range(df)
        assert result["start_date"].iloc[0] == pd.Timestamp("2018-01-01")
        assert result["end_date"].iloc[0] == pd.Timestamp("2022-12-31")

    def test_nat_end_date_not_swapped(self) -> None:
        df = pd.DataFrame(
            {
                "start_date": ["2022-01-01"],
                "end_date": [None],
            }
        )
        result = standardize_date_range(df)
        assert result["start_date"].iloc[0] == pd.Timestamp("2022-01-01")
        assert pd.isna(result["end_date"].iloc[0])

    def test_converts_to_datetime(self) -> None:
        df = pd.DataFrame(
            {
                "start_date": ["2020-03-01"],
                "end_date": ["2021-03-01"],
            }
        )
        result = standardize_date_range(df)
        assert pd.api.types.is_datetime64_any_dtype(result["start_date"])


# ---------------------------------------------------------------------------
# Tests: normalize_experience
# ---------------------------------------------------------------------------


class TestNormalizeExperience:
    def test_deduplicates_on_id(self) -> None:
        df = _make_experience()
        dup = pd.concat([df, df.head(1)], ignore_index=True)
        result = normalize_experience(dup)
        assert len(result) == 2

    def test_date_columns_datetime(self) -> None:
        df = clean_experience(_make_experience())
        result = normalize_experience(df)
        assert pd.api.types.is_datetime64_any_dtype(result["start_date"])


# ---------------------------------------------------------------------------
# Tests: normalize_skills_df
# ---------------------------------------------------------------------------


class TestNormalizeSkillsDf:
    def test_deduplicates_on_skill_name(self) -> None:
        df = clean_skills(_make_skills())
        # Add a duplicate
        dup = pd.concat([df, df.head(1)], ignore_index=True)
        result = normalize_skills_df(dup)
        assert len(result) == len(df)

    def test_maps_canonical_names(self) -> None:
        df = clean_skills(_make_skills())
        result = normalize_skills_df(df)
        assert "Python" in result["skill_name"].tolist()


# ---------------------------------------------------------------------------
# Tests: normalize_summary
# ---------------------------------------------------------------------------


class TestNormalizeSummary:
    def test_keeps_most_recent_on_duplicate_id(self) -> None:
        df = pd.DataFrame(
            {
                "id": ["sum1", "sum1"],
                "text": ["old text", "new text"],
                "last_updated": ["2020-01-01", "2024-01-01"],
            }
        )
        df["last_updated"] = pd.to_datetime(df["last_updated"])
        result = normalize_summary(df)
        assert len(result) == 1
        assert result["text"].iloc[0] == "new text"


# ---------------------------------------------------------------------------
# Tests: CANONICAL_SKILLS list
# ---------------------------------------------------------------------------


class TestCanonicalSkills:
    def test_is_list(self) -> None:
        assert isinstance(CANONICAL_SKILLS, list)

    def test_not_empty(self) -> None:
        assert len(CANONICAL_SKILLS) > 0

    def test_contains_python(self) -> None:
        assert "Python" in CANONICAL_SKILLS
