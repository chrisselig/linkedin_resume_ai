"""Main entry point for the LinkedIn Resume Python Shiny application.

Reads resume data from a DuckDB database when available, falling back
to built-in sample data when the database is absent or unreachable.

Usage:
    shiny run app/app.py
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
from shiny import App, reactive, render, ui

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

_APP_DIR = Path(__file__).parent
_PROJECT_ROOT = _APP_DIR.parent
_DB_PATH = _PROJECT_ROOT / "data" / "duckdb" / "linkedin.db"
_CSS_PATH = _APP_DIR / "assets" / "style.css"

# ---------------------------------------------------------------------------
# Sample / fallback data
# ---------------------------------------------------------------------------

_SAMPLE_SUMMARY = (
    "Results-driven data engineer and analytics professional with 8+ years of "
    "experience designing scalable data pipelines, building interactive "
    "dashboards, and delivering actionable insights across fintech and SaaS "
    "industries. Passionate about open-source tooling and reproducible research."
)

_SAMPLE_PROFILE = {
    "name": "Alex Johnson",
    "headline": "Senior Data Engineer · Analytics · Python · Cloud",
    "location": "San Francisco, CA",
    "email": "alex.johnson@example.com",
    "linkedin_url": "https://linkedin.com/in/alexjohnson",
}

_SAMPLE_EXPERIENCE = pd.DataFrame(
    [
        {
            "title": "Senior Data Engineer",
            "company": "Acme Analytics",
            "location": "San Francisco, CA",
            "start_date": "Jan 2021",
            "end_date": "Present",
            "description": (
                "• Designed and maintained 15+ production ELT pipelines "
                "processing 500 GB/day using dbt, Airflow, and Snowflake.\n"
                "• Reduced p95 query latency by 40 % through strategic "
                "materialisation and partition pruning.\n"
                "• Mentored 3 junior engineers and led bi-weekly tech-talk series."
            ),
        },
        {
            "title": "Data Engineer",
            "company": "FinStream Inc.",
            "location": "New York, NY",
            "start_date": "Jun 2018",
            "end_date": "Dec 2020",
            "description": (
                "• Built real-time fraud-detection feature store on Kafka + Flink, "
                "cutting false-positive rate by 18 %.\n"
                "• Migrated legacy Oracle pipelines to BigQuery, saving $120 k/yr."
            ),
        },
        {
            "title": "Data Analyst",
            "company": "DataFirst Consulting",
            "location": "Chicago, IL",
            "start_date": "Jul 2016",
            "end_date": "May 2018",
            "description": (
                "• Delivered weekly executive dashboards in Tableau for 6 Fortune-500"
                " clients.\n"
                "• Automated reporting workflows in Python, saving 10 hrs/week."
            ),
        },
    ]
)

_SAMPLE_EDUCATION = pd.DataFrame(
    [
        {
            "school": "University of Illinois Urbana-Champaign",
            "degree": "Master of Science",
            "field_of_study": "Computer Science",
            "start_year": "2014",
            "end_year": "2016",
            "description": "Specialisation in data-intensive computing systems.",
        },
        {
            "school": "Purdue University",
            "degree": "Bachelor of Science",
            "field_of_study": "Statistics",
            "start_year": "2010",
            "end_year": "2014",
            "description": "",
        },
    ]
)

_SAMPLE_SKILLS = pd.DataFrame(
    [
        {"name": "Python", "endorsements": 48},
        {"name": "SQL", "endorsements": 42},
        {"name": "dbt", "endorsements": 31},
        {"name": "Apache Airflow", "endorsements": 28},
        {"name": "Spark", "endorsements": 24},
        {"name": "Snowflake", "endorsements": 22},
        {"name": "BigQuery", "endorsements": 19},
        {"name": "Docker", "endorsements": 17},
        {"name": "Kubernetes", "endorsements": 14},
        {"name": "Tableau", "endorsements": 12},
        {"name": "AWS", "endorsements": 11},
        {"name": "Terraform", "endorsements": 9},
    ]
)

_SAMPLE_CERTIFICATIONS = pd.DataFrame(
    [
        {
            "name": "AWS Certified Data Analytics – Specialty",
            "issuer": "Amazon Web Services",
            "issued_date": "2023-04",
        },
        {
            "name": "Google Professional Data Engineer",
            "issuer": "Google Cloud",
            "issued_date": "2022-11",
        },
        {
            "name": "dbt Analytics Engineering Certification",
            "issuer": "dbt Labs",
            "issued_date": "2022-06",
        },
    ]
)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def get_db_connection(
    db_path: str | Path = _DB_PATH,
) -> Optional[duckdb.DuckDBPyConnection]:
    """Open a DuckDB connection to the resume database.

    Args:
        db_path: Path to the DuckDB file. Defaults to the project-standard
                 location at ``data/duckdb/linkedin.db``.

    Returns:
        An open ``DuckDBPyConnection`` if the file exists and is readable,
        or ``None`` if the connection cannot be established (graceful fallback).
    """
    try:
        path = Path(db_path)
        if not path.exists():
            return None
        conn = duckdb.connect(str(path), read_only=True)
        return conn
    except Exception:
        return None


def _safe_query(conn: duckdb.DuckDBPyConnection, query: str) -> Optional[pd.DataFrame]:
    """Execute a SQL query and return results as a DataFrame.

    Args:
        conn: An open DuckDB connection.
        query: SQL query string to execute.

    Returns:
        A pandas DataFrame with query results, or ``None`` on error.
    """
    try:
        return conn.execute(query).df()
    except Exception:
        return None


def get_sample_data(section: str) -> pd.DataFrame:
    """Return hardcoded sample data for a named resume section.

    Used as a fallback when the DuckDB database is unavailable.

    Args:
        section: One of ``"experience"``, ``"education"``, ``"skills"``,
                 or ``"certifications"``.

    Returns:
        A pandas DataFrame with sample records for the requested section.

    Raises:
        ValueError: If an unknown section name is provided.
    """
    sample_map: dict[str, pd.DataFrame] = {
        "experience": _SAMPLE_EXPERIENCE,
        "education": _SAMPLE_EDUCATION,
        "skills": _SAMPLE_SKILLS,
        "certifications": _SAMPLE_CERTIFICATIONS,
    }
    if section not in sample_map:
        raise ValueError(
            f"Unknown section '{section}'. "
            f"Valid sections: {list(sample_map.keys())}"
        )
    return sample_map[section].copy()


def _extract_year(date_series: pd.Series) -> pd.Series:
    """Extract the year from ISO 8601 date strings (e.g. '2020-01-01' -> '2020').

    Non-parseable values are replaced with an empty string.

    Args:
        date_series: A pandas Series of ISO 8601 date strings or nulls.

    Returns:
        A Series of year strings (e.g. '2020') or empty strings for nulls.
    """
    parsed = pd.to_datetime(date_series, errors="coerce")
    return parsed.dt.year.astype("Int64").astype(str).where(parsed.notna(), other="")


def load_section(
    conn: Optional[duckdb.DuckDBPyConnection], section: str
) -> pd.DataFrame:
    """Load data for a resume section from DuckDB or sample data.

    When loading the education section from DuckDB the actual scrape schema
    stores dates as ISO 8601 strings (``start_date`` / ``end_date``).  This
    function derives ``start_year`` / ``end_year`` integer-year string columns
    from those date strings so that the education component can render them
    consistently regardless of whether data came from the DB or sample data.

    Args:
        conn: An open DuckDB connection, or ``None`` to use sample data.
        section: One of ``"experience"``, ``"education"``, ``"skills"``,
                 or ``"certifications"``.

    Returns:
        A pandas DataFrame with the section data.
    """
    if conn is None:
        return get_sample_data(section)

    table_map = {
        "experience": "SELECT * FROM experience ORDER BY start_date DESC",
        "education": "SELECT * FROM education ORDER BY end_date DESC",
        "skills": "SELECT * FROM skills ORDER BY endorsement_count DESC",
        "certifications": "SELECT * FROM certifications ORDER BY issued_date DESC",
    }
    if section not in table_map:
        return get_sample_data(section)

    result = _safe_query(conn, table_map[section])
    if result is None or result.empty:
        return get_sample_data(section)

    # When loading education from DB, derive start_year/end_year from ISO date strings
    if section == "education":
        if "start_date" in result.columns and "start_year" not in result.columns:
            result = result.copy()
            result["start_year"] = _extract_year(result["start_date"])
        if "end_date" in result.columns and "end_year" not in result.columns:
            result = result.copy()
            result["end_year"] = _extract_year(result["end_date"])

    return result


def load_profile(
    conn: Optional[duckdb.DuckDBPyConnection],
) -> dict[str, str]:
    """Load the profile/summary record from DuckDB or return sample data.

    Args:
        conn: An open DuckDB connection, or ``None`` to use sample data.

    Returns:
        A dict with keys: name, headline, location, email, linkedin_url, summary.
    """
    if conn is None:
        return {**_SAMPLE_PROFILE, "summary": _SAMPLE_SUMMARY}

    result = _safe_query(conn, "SELECT * FROM profile LIMIT 1")
    if result is None or result.empty:
        return {**_SAMPLE_PROFILE, "summary": _SAMPLE_SUMMARY}

    row = result.iloc[0].to_dict()
    row.setdefault("summary", _SAMPLE_SUMMARY)
    return row


# ---------------------------------------------------------------------------
# Certification card builder (inline, no separate module needed)
# ---------------------------------------------------------------------------


def _build_cert_card(row: pd.Series) -> ui.Tag:
    """Build a certification card HTML element from a DataFrame row.

    Args:
        row: A pandas Series with keys: name, issuer, issued_date.

    Returns:
        An htmltools Tag representing the certification entry.
    """
    return ui.div(
        ui.span(str(row.get("name", "")), class_="cert-name"),
        ui.span(str(row.get("issuer", "")), class_="cert-issuer"),
        ui.span(str(row.get("issued_date", "")), class_="cert-date"),
        class_="cert-card",
    )


# ---------------------------------------------------------------------------
# UI definition
# ---------------------------------------------------------------------------


def build_ui(css_path: Path = _CSS_PATH) -> ui.Tag:
    """Construct the top-level Shiny UI layout for the resume application.

    Args:
        css_path: Absolute path to the CSS stylesheet.

    Returns:
        A Shiny ``Tag`` representing the full application UI.
    """
    google_fonts_link = ui.tags.link(
        rel="stylesheet",
        href=(
            "https://fonts.googleapis.com/css2?"
            "family=Inter:wght@400;500;600&"
            "family=Playfair+Display:wght@700&"
            "display=swap"
        ),
    )

    css_tag: ui.Tag
    if css_path.exists():
        css_tag = ui.tags.link(rel="stylesheet", href="assets/style.css")
    else:
        css_tag = ui.span("")  # no-op placeholder

    return ui.page_fluid(
        ui.head_content(google_fonts_link, css_tag),
        ui.div(
            # Header
            ui.div(
                ui.output_ui("header_section"),
                class_="resume-header",
            ),
            # Summary
            ui.div(
                ui.output_ui("summary_section"),
                class_="section summary-section",
            ),
            # Experience
            ui.div(
                ui.h2("Experience", class_="section-heading"),
                ui.output_ui("experience_section"),
                class_="section experience-section",
            ),
            # Education
            ui.div(
                ui.h2("Education", class_="section-heading"),
                ui.output_ui("education_section"),
                class_="section education-section",
            ),
            # Skills
            ui.div(
                ui.h2("Skills", class_="section-heading"),
                ui.output_ui("skills_chart_section"),
                ui.output_ui("skills_tags_section"),
                class_="section skills-section",
            ),
            # Certifications
            ui.div(
                ui.h2("Certifications", class_="section-heading"),
                ui.output_ui("certifications_section"),
                class_="section certifications-section",
            ),
            class_="resume-wrapper",
        ),
    )


app_ui = build_ui()

# ---------------------------------------------------------------------------
# Server logic
# ---------------------------------------------------------------------------


def server(input, output, session) -> None:
    """Define the Shiny server logic for the LinkedIn resume application.

    Establishes a DuckDB connection (or graceful fallback), loads all resume
    sections reactively, and registers render functions for each UI output.

    Args:
        input: Shiny input object.
        output: Shiny output object.
        session: Shiny session object.
    """
    # -- Data loading (evaluated once per session) ---------------------------
    conn = get_db_connection()

    profile_data: dict[str, str] = load_profile(conn)
    experience_df = reactive.Value(load_section(conn, "experience"))
    education_df = reactive.Value(load_section(conn, "education"))
    skills_df = reactive.Value(load_section(conn, "skills"))
    certifications_df = reactive.Value(load_section(conn, "certifications"))

    # -- Header --------------------------------------------------------------
    @output
    @render.ui
    def header_section() -> ui.Tag:
        """Render the resume header with name, headline, and contact info."""
        name = profile_data.get("name", "")
        headline = profile_data.get("headline", "")
        location = profile_data.get("location", "")
        email = profile_data.get("email", "")
        linkedin_url = profile_data.get("linkedin_url", "")

        contact_items: list[ui.Tag] = []
        if email:
            contact_items.append(ui.tags.a(email, href=f"mailto:{email}"))
        if linkedin_url:
            contact_items.append(
                ui.tags.a("LinkedIn", href=linkedin_url, target="_blank")
            )

        return ui.div(
            ui.div(
                ui.h1(name),
                ui.p(headline, class_="headline"),
                ui.p(location, class_="location") if location else ui.span(""),
                (
                    ui.div(*contact_items, class_="contact-links")
                    if contact_items
                    else ui.span("")
                ),
                class_="header-text",
            ),
        )

    # -- Summary -------------------------------------------------------------
    @output
    @render.ui
    def summary_section() -> ui.Tag:
        """Render the professional summary paragraph."""
        summary = profile_data.get("summary", "")
        if not summary:
            return ui.span("")
        return ui.p(summary)

    # -- Experience ----------------------------------------------------------
    @output
    @render.ui
    def experience_section() -> ui.Tag:
        """Render experience cards in a timeline layout."""
        from app.components.experience import _build_experience_card

        df = experience_df()
        if df is None or df.empty:
            return ui.p("No experience data available.", class_="no-data")
        cards = [_build_experience_card(row) for _, row in df.iterrows()]
        return ui.div(*cards, class_="exp-timeline")

    # -- Education -----------------------------------------------------------
    @output
    @render.ui
    def education_section() -> ui.Tag:
        """Render education cards."""
        from app.components.education import _build_education_card

        df = education_df()
        if df is None or df.empty:
            return ui.p("No education data available.", class_="no-data")
        cards = [_build_education_card(row) for _, row in df.iterrows()]
        return ui.div(*cards, class_="edu-list")

    # -- Skills chart --------------------------------------------------------
    @output
    @render.ui
    def skills_chart_section() -> ui.Tag:
        """Render the skills endorsement bar chart (if endorsement data exists)."""
        from app.components.skills import _build_skills_chart

        df = skills_df()
        if df is None or df.empty:
            return ui.span("")
        chart = _build_skills_chart(df)
        if chart is None:
            return ui.span("")
        return ui.div(chart, class_="skills-chart-wrapper")

    # -- Skills tags ---------------------------------------------------------
    @output
    @render.ui
    def skills_tags_section() -> ui.Tag:
        """Render skill badge tags."""
        from app.components.skills import _build_skill_tag

        df = skills_df()
        if df is None or df.empty:
            return ui.p("No skills data available.", class_="no-data")
        tags = [_build_skill_tag(str(row.get("name", ""))) for _, row in df.iterrows()]
        return ui.div(*tags, class_="skills-tag-cloud")

    # -- Certifications ------------------------------------------------------
    @output
    @render.ui
    def certifications_section() -> ui.Tag:
        """Render certification cards."""
        df = certifications_df()
        if df is None or df.empty:
            return ui.p("No certifications data available.", class_="no-data")
        cards = [_build_cert_card(row) for _, row in df.iterrows()]
        return ui.div(*cards, class_="cert-list")


# ---------------------------------------------------------------------------
# Application object
# ---------------------------------------------------------------------------

app = App(
    app_ui,
    server,
    static_assets=str(_APP_DIR / "assets"),
)
