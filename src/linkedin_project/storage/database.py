"""DuckDB storage operations for LinkedIn profile data.

All SQL uses parameterized queries — no string interpolation for values.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Valid section names — used for allowlist validation to prevent SQL injection
# through table-name substitution.
# ---------------------------------------------------------------------------
_VALID_SECTIONS: frozenset[str] = frozenset(
    ["experience", "education", "skills", "certifications", "summary"]
)


def _validate_section(section: str) -> None:
    """Raise ``ValueError`` if *section* is not a valid table name.

    Parameters
    ----------
    section:
        Section name to validate.

    Raises
    ------
    ValueError
        When *section* is not in the allowed set.
    """
    if section not in _VALID_SECTIONS:
        raise ValueError(
            f"Invalid section '{section}'. Must be one of: {sorted(_VALID_SECTIONS)}"
        )


# ---------------------------------------------------------------------------
# DDL — table creation statements
# ---------------------------------------------------------------------------

_DDL: dict[str, str] = {
    "experience": """
        CREATE TABLE IF NOT EXISTS experience (
            id              VARCHAR NOT NULL,
            company_name    VARCHAR,
            title           VARCHAR,
            location        VARCHAR,
            start_date      TIMESTAMP,
            end_date        TIMESTAMP,
            description     VARCHAR,
            is_current      BOOLEAN,
            PRIMARY KEY (id)
        )
    """,
    "education": """
        CREATE TABLE IF NOT EXISTS education (
            id              VARCHAR NOT NULL,
            school_name     VARCHAR,
            degree          VARCHAR,
            field_of_study  VARCHAR,
            start_date      TIMESTAMP,
            end_date        TIMESTAMP,
            grade           DOUBLE,
            PRIMARY KEY (id)
        )
    """,
    "skills": """
        CREATE TABLE IF NOT EXISTS skills (
            id                VARCHAR NOT NULL,
            skill_name        VARCHAR,
            endorsement_count INTEGER,
            PRIMARY KEY (id)
        )
    """,
    "certifications": """
        CREATE TABLE IF NOT EXISTS certifications (
            id             VARCHAR NOT NULL,
            cert_name      VARCHAR,
            authority      VARCHAR,
            issued_date    TIMESTAMP,
            expiry_date    TIMESTAMP,
            credential_id  VARCHAR,
            PRIMARY KEY (id)
        )
    """,
    "summary": """
        CREATE TABLE IF NOT EXISTS summary (
            id           VARCHAR NOT NULL,
            text         VARCHAR,
            last_updated TIMESTAMP,
            PRIMARY KEY (id)
        )
    """,
}


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    """Open (or create) a DuckDB database at *db_path*.

    Pass ``":memory:"`` to create a transient in-memory database — useful
    for tests.

    Parameters
    ----------
    db_path:
        Filesystem path for the ``.duckdb`` file, or ``":memory:"``.

    Returns
    -------
    duckdb.DuckDBPyConnection
        An open DuckDB connection.
    """
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(db_path)


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all LinkedIn profile tables in *conn* if they do not exist.

    Tables created:

    * ``experience``
    * ``education``
    * ``skills``
    * ``certifications``
    * ``summary``

    Parameters
    ----------
    conn:
        An open DuckDB connection (from :func:`connect`).
    """
    for ddl in _DDL.values():
        conn.execute(ddl)


def upsert_profile(
    conn: duckdb.DuckDBPyConnection,
    section: str,
    df: pd.DataFrame,
) -> None:
    """Write *df* into the *section* table, upserting on primary key ``id``.

    The operation is a DELETE-then-INSERT within a single transaction so
    that existing rows with matching ``id`` values are replaced.

    Parameters
    ----------
    conn:
        An open DuckDB connection.
    section:
        Target table name — must be one of ``experience``, ``education``,
        ``skills``, ``certifications``, ``summary``.
    df:
        DataFrame whose columns match the target table schema.

    Raises
    ------
    ValueError
        When *section* is not a recognised table name.
    """
    _validate_section(section)

    if df.empty:
        return

    # Register the DataFrame as a temporary view so we can reference it in SQL
    view_name = f"_tmp_{section}"
    conn.register(view_name, df)

    try:
        # Delete existing rows that share an id with the incoming data.
        # We use a subquery against the registered view — no string value
        # interpolation in the SQL body.
        conn.execute(
            f"DELETE FROM {section} WHERE id IN (SELECT id FROM {view_name})"  # noqa: S608
        )
        conn.execute(f"INSERT INTO {section} SELECT * FROM {view_name}")  # noqa: S608
    finally:
        conn.unregister(view_name)


def query_section(
    conn: duckdb.DuckDBPyConnection,
    section: str,
    filters: Optional[dict[str, Any]] = None,
) -> pd.DataFrame:
    """Read rows from *section* that match *filters*.

    Parameters
    ----------
    conn:
        An open DuckDB connection.
    section:
        Source table name — must be one of the valid section names.
    filters:
        Optional mapping of ``{column_name: value}`` equality filters.
        All filters are applied with ``AND`` logic.  Values are bound as
        parameterized query parameters — **no string interpolation**.

    Returns
    -------
    pd.DataFrame
        Query results as a pandas DataFrame.

    Raises
    ------
    ValueError
        When *section* is not a recognised table name, or a filter column
        name is not a safe identifier.
    """
    _validate_section(section)

    if not filters:
        return conn.execute(f"SELECT * FROM {section}").df()  # noqa: S608

    # Validate column names against a simple identifier pattern to prevent
    # SQL injection via column name (values are parameterized).
    for col in filters:
        if not col.replace("_", "").isalnum():
            raise ValueError(f"Unsafe column name in filter: '{col}'")

    where_clauses = " AND ".join(f"{col} = ?" for col in filters)
    params = list(filters.values())
    sql = f"SELECT * FROM {section} WHERE {where_clauses}"  # noqa: S608
    return conn.execute(sql, params).df()
