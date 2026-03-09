"""Data normalization functions for LinkedIn profile data.

All operations use vectorized pandas operations — no row-level loops.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from linkedin_project.transform.cleaner import SKILL_LOOKUP


def normalize_skills(df: pd.DataFrame) -> pd.DataFrame:
    """Map raw skill names to canonical skill names.

    Performs a vectorized lookup against :data:`~linkedin_project.transform.cleaner.SKILL_LOOKUP`.
    Skills that do not match any canonical entry are kept as-is (title-cased).

    Parameters
    ----------
    df:
        DataFrame with at minimum a ``skill_name`` column.

    Returns
    -------
    pd.DataFrame
        Copy of *df* with ``skill_name`` replaced by canonical skill name.
    """
    out = df.copy()
    lowered = out["skill_name"].str.lower().str.strip()
    out["skill_name"] = lowered.map(SKILL_LOOKUP).fillna(out["skill_name"].str.title())
    return out


def deduplicate(
    df: pd.DataFrame,
    subset: Optional[list[str]] = None,
    keep: str = "first",
) -> pd.DataFrame:
    """Remove duplicate rows from a DataFrame.

    Parameters
    ----------
    df:
        Input DataFrame.
    subset:
        Column labels to consider for identifying duplicates.  When
        ``None``, all columns are used.
    keep:
        Which duplicate to keep — ``"first"`` (default) or ``"last"``.

    Returns
    -------
    pd.DataFrame
        DataFrame with duplicates removed and index reset.
    """
    return df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)


def standardize_date_range(
    df: pd.DataFrame,
    start_col: str = "start_date",
    end_col: str = "end_date",
) -> pd.DataFrame:
    """Ensure date-range columns are consistent datetime columns.

    * Converts both columns to ``datetime64[ns]`` (via
      :func:`pandas.to_datetime`).
    * Where *start_date* > *end_date* and *end_date* is not NaT, the two
      values are swapped so the earlier date is always the start.

    Parameters
    ----------
    df:
        DataFrame containing date-range columns.
    start_col:
        Name of the start-date column.
    end_col:
        Name of the end-date column.

    Returns
    -------
    pd.DataFrame
        Copy of *df* with corrected date columns.
    """
    out = df.copy()
    out[start_col] = pd.to_datetime(out[start_col], errors="coerce")
    out[end_col] = pd.to_datetime(out[end_col], errors="coerce")

    both_valid = out[start_col].notna() & out[end_col].notna()
    inverted = both_valid & (out[start_col] > out[end_col])

    # Swap using vectorized assignment (no iterrows)
    tmp = out.loc[inverted, start_col].copy()
    out.loc[inverted, start_col] = out.loc[inverted, end_col]
    out.loc[inverted, end_col] = tmp

    return out


def normalize_experience(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a cleaned experience DataFrame.

    Applies :func:`deduplicate` (on ``id``) and
    :func:`standardize_date_range`.

    Parameters
    ----------
    df:
        Cleaned experience DataFrame.

    Returns
    -------
    pd.DataFrame
        Normalized DataFrame.
    """
    out = deduplicate(df, subset=["id"])
    out = standardize_date_range(out, "start_date", "end_date")
    return out


def normalize_education(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a cleaned education DataFrame.

    Parameters
    ----------
    df:
        Cleaned education DataFrame.

    Returns
    -------
    pd.DataFrame
        Normalized DataFrame.
    """
    out = deduplicate(df, subset=["id"])
    out = standardize_date_range(out, "start_date", "end_date")
    return out


def normalize_skills_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a cleaned skills DataFrame.

    Applies canonical name mapping and deduplication on ``skill_name``.

    Parameters
    ----------
    df:
        Cleaned skills DataFrame.

    Returns
    -------
    pd.DataFrame
        Normalized DataFrame.
    """
    out = normalize_skills(df)
    out = deduplicate(out, subset=["skill_name"])
    return out


def normalize_certifications(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a cleaned certifications DataFrame.

    Parameters
    ----------
    df:
        Cleaned certifications DataFrame.

    Returns
    -------
    pd.DataFrame
        Normalized DataFrame.
    """
    out = deduplicate(df, subset=["id"])
    out = standardize_date_range(out, "issued_date", "expiry_date")
    return out


def normalize_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a cleaned summary DataFrame.

    Parameters
    ----------
    df:
        Cleaned summary DataFrame.

    Returns
    -------
    pd.DataFrame
        Normalized DataFrame — keeps only the most recently updated row
        when duplicates exist.
    """
    out = df.copy()
    if "last_updated" in out.columns:
        out = out.sort_values("last_updated", ascending=False)
    out = deduplicate(out, subset=["id"])
    return out
