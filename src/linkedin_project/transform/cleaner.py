"""Data cleaning functions for LinkedIn profile data.

All operations use vectorized pandas operations — no row-level loops.
"""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd


def clean_text_field(series: pd.Series) -> pd.Series:
    """Strip whitespace and normalize None/NaN values to empty strings.

    Parameters
    ----------
    series:
        A pandas Series of string (or mixed) values.

    Returns
    -------
    pd.Series
        Series with whitespace stripped and NaN/None replaced by empty string.
    """
    return series.fillna("").astype(str).str.strip()


def parse_date_column(
    series: pd.Series,
    fmt: Optional[str] = None,
    utc: bool = False,
) -> pd.Series:
    """Parse a Series of date strings into datetime objects.

    Parameters
    ----------
    series:
        A pandas Series containing date strings or existing datetimes.
    fmt:
        Optional strftime format string (e.g. ``"%Y-%m-%d"``).  When
        ``None``, pandas infers the format automatically.
    utc:
        When ``True``, parse with UTC timezone awareness.

    Returns
    -------
    pd.Series
        Series of ``datetime64[ns]`` (or timezone-aware equivalent).
    """
    return pd.to_datetime(series, format=fmt, utc=utc, errors="coerce")


def normalize_company_name(series: pd.Series) -> pd.Series:
    """Normalize company or institution names.

    Applies the following transformations via vectorized string operations:

    * Strip leading/trailing whitespace.
    * Collapse runs of internal whitespace to a single space.
    * Title-case the result.
    * Remove common legal suffixes (``Inc``, ``LLC``, ``Ltd``, etc.)
      followed by punctuation or end-of-string.

    Parameters
    ----------
    series:
        A pandas Series of company/institution name strings.

    Returns
    -------
    pd.Series
        Normalized Series of strings.
    """
    _LEGAL_SUFFIXES = (
        r",?\s*\b(?:Inc\.?|LLC\.?|Ltd\.?|L\.L\.C\.?|Corp\.?|"
        r"Co\.?|PLC\.?|GmbH\.?|S\.A\.?|B\.V\.?)\.?$"
    )

    cleaned = (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.title()
        .str.replace(_LEGAL_SUFFIXES, "", regex=True)
        .str.strip()
    )
    return cleaned


def clean_experience(df: pd.DataFrame) -> pd.DataFrame:
    """Clean a raw experience DataFrame.

    Parameters
    ----------
    df:
        DataFrame with columns:
        ``[id, company_name, title, location, start_date, end_date,
        description, is_current]``.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with the same column set.
    """
    out = df.copy()

    for col in ("id", "title", "location", "description"):
        out[col] = clean_text_field(out[col])

    out["company_name"] = normalize_company_name(out["company_name"])
    out["start_date"] = parse_date_column(out["start_date"])
    out["end_date"] = parse_date_column(out["end_date"])
    out["is_current"] = out["is_current"].fillna(False).astype(bool)

    return out


def clean_education(df: pd.DataFrame) -> pd.DataFrame:
    """Clean a raw education DataFrame.

    Parameters
    ----------
    df:
        DataFrame with columns:
        ``[id, school_name, degree, field_of_study, start_date, end_date,
        grade]``.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with the same column set.
    """
    out = df.copy()

    for col in ("id", "degree", "field_of_study"):
        out[col] = clean_text_field(out[col])

    out["school_name"] = normalize_company_name(out["school_name"])
    out["start_date"] = parse_date_column(out["start_date"])
    out["end_date"] = parse_date_column(out["end_date"])
    out["grade"] = pd.to_numeric(out["grade"], errors="coerce")

    return out


def clean_skills(df: pd.DataFrame) -> pd.DataFrame:
    """Clean a raw skills DataFrame.

    Parameters
    ----------
    df:
        DataFrame with columns: ``[id, skill_name, endorsement_count]``.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with the same column set.
    """
    out = df.copy()

    for col in ("id", "skill_name"):
        out[col] = clean_text_field(out[col])

    out["endorsement_count"] = (
        pd.to_numeric(out["endorsement_count"], errors="coerce").fillna(0).astype(int)
    )

    return out


def clean_certifications(df: pd.DataFrame) -> pd.DataFrame:
    """Clean a raw certifications DataFrame.

    Parameters
    ----------
    df:
        DataFrame with columns:
        ``[id, cert_name, authority, issued_date, expiry_date,
        credential_id]``.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with the same column set.
    """
    out = df.copy()

    for col in ("id", "cert_name", "credential_id"):
        out[col] = clean_text_field(out[col])

    out["authority"] = normalize_company_name(out["authority"])
    out["issued_date"] = parse_date_column(out["issued_date"])
    out["expiry_date"] = parse_date_column(out["expiry_date"])

    return out


def clean_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Clean a raw summary DataFrame.

    Parameters
    ----------
    df:
        DataFrame with columns: ``[id, text, last_updated]``.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with the same column set.
    """
    out = df.copy()

    for col in ("id", "text"):
        out[col] = clean_text_field(out[col])

    out["last_updated"] = parse_date_column(out["last_updated"])

    return out


# ---------------------------------------------------------------------------
# Module-level canonical skill list used by normalizer (defined here so it can
# be imported without a circular dependency).
# ---------------------------------------------------------------------------

CANONICAL_SKILLS: list[str] = [
    "Python",
    "R",
    "SQL",
    "Machine Learning",
    "Deep Learning",
    "Data Analysis",
    "Data Engineering",
    "Data Visualization",
    "Natural Language Processing",
    "Computer Vision",
    "Statistics",
    "Pandas",
    "NumPy",
    "Scikit-Learn",
    "TensorFlow",
    "PyTorch",
    "Spark",
    "Hadoop",
    "Kafka",
    "Airflow",
    "Docker",
    "Kubernetes",
    "AWS",
    "Azure",
    "GCP",
    "Git",
    "Linux",
    "Tableau",
    "Power BI",
    "Excel",
    "DuckDB",
    "PostgreSQL",
    "MySQL",
    "MongoDB",
    "Redis",
    "FastAPI",
    "Flask",
    "Django",
    "React",
    "TypeScript",
    "JavaScript",
    "Java",
    "Scala",
    "Go",
    "Rust",
    "C++",
    "C",
    "Shiny",
    "dbt",
    "Snowflake",
    "BigQuery",
    "Databricks",
    "MLflow",
    "Kubernetes",
    "Terraform",
    "CI/CD",
    "Agile",
    "Scrum",
]


def _build_skill_lookup() -> dict[str, str]:
    """Build a case-insensitive lookup mapping aliases to canonical names."""
    lookup: dict[str, str] = {}
    for skill in CANONICAL_SKILLS:
        lookup[skill.lower()] = skill
        # Strip punctuation variant
        alias = re.sub(r"[^a-z0-9 ]", "", skill.lower()).strip()
        if alias and alias not in lookup:
            lookup[alias] = skill
    # Hand-crafted aliases
    _ALIASES: dict[str, str] = {
        "ml": "Machine Learning",
        "dl": "Deep Learning",
        "nlp": "Natural Language Processing",
        "cv": "Computer Vision",
        "scikit learn": "Scikit-Learn",
        "sklearn": "Scikit-Learn",
        "tensorflow 2": "TensorFlow",
        "tf": "TensorFlow",
        "pytorch": "PyTorch",
        "torch": "PyTorch",
        "power bi": "Power BI",
        "powerbi": "Power BI",
        "gcp": "GCP",
        "google cloud": "GCP",
        "amazon web services": "AWS",
        "microsoft azure": "Azure",
        "postgresql": "PostgreSQL",
        "postgres": "PostgreSQL",
        "mysql": "MySQL",
        "mongodb": "MongoDB",
    }
    lookup.update({k.lower(): v for k, v in _ALIASES.items()})
    return lookup


SKILL_LOOKUP: dict[str, str] = _build_skill_lookup()
