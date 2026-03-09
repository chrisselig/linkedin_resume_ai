"""LinkedIn Resume — Nebula Dark edition.

Reads resume data from DuckDB when available; falls back to built-in
sample data.  All component rendering is inlined here to avoid
import-path issues when deployed to shinyapps.io.

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
# Paths
# ---------------------------------------------------------------------------
_APP_DIR = Path(__file__).parent
_PROJECT_ROOT = _APP_DIR.parent
_DB_PATH = _PROJECT_ROOT / "data" / "duckdb" / "linkedin.db"
_CSS_PATH = _APP_DIR / "assets" / "style.css"

# ---------------------------------------------------------------------------
# Sample data — Chris Selig
# ---------------------------------------------------------------------------
_SAMPLE_PROFILE = {
    "name": "Chris Selig",
    "headline": "Data & Analytics Professional | Python · SQL · dbt · Cloud",
    "location": "Calgary, AB, Canada",
    "email": "",
    "linkedin_url": "https://www.linkedin.com/in/chris-selig/",
    "summary": (
        "Data and analytics professional based in Calgary with deep expertise "
        "in building end-to-end data platforms, analytics engineering, and "
        "cloud-native data solutions. Passionate about clean data architecture, "
        "open-source tooling, and turning raw data into reliable business insight."
    ),
}

_SAMPLE_EXPERIENCE = pd.DataFrame(
    [
        {
            "title": "Analytics Engineer",
            "company": "BIDAMIA",
            "location": "Calgary, AB",
            "start_date": "2022-01-01",
            "end_date": None,
            "is_current": True,
            "description": (
                "• Designing and maintaining modern data stack using dbt, "
                "Snowflake, and Airflow.\n"
                "• Building self-serve analytics capabilities across the org.\n"
                "• Leading cloud data platform architecture and best practices."
            ),
        },
        {
            "title": "Senior Data Analyst",
            "company": "Previous Role",
            "location": "Calgary, AB",
            "start_date": "2019-01-01",
            "end_date": "2021-12-01",
            "is_current": False,
            "description": (
                "• Delivered executive-level dashboards and KPI reporting.\n"
                "• Automated ETL workflows reducing manual effort by 60 %.\n"
                "• Mentored junior analysts on SQL and data modelling practices."
            ),
        },
    ]
)

_SAMPLE_EDUCATION = pd.DataFrame(
    [
        {
            "school": "University",
            "degree": "Bachelor of Science",
            "field_of_study": "Business / Technology",
            "start_year": "2010",
            "end_year": "2014",
            "description": "",
        }
    ]
)

_SAMPLE_SKILLS = pd.DataFrame(
    [
        {"name": "Python", "endorsements": 45},
        {"name": "SQL", "endorsements": 52},
        {"name": "dbt", "endorsements": 38},
        {"name": "Snowflake", "endorsements": 30},
        {"name": "Apache Airflow", "endorsements": 26},
        {"name": "Power BI", "endorsements": 22},
        {"name": "Tableau", "endorsements": 20},
        {"name": "AWS", "endorsements": 18},
        {"name": "Docker", "endorsements": 15},
        {"name": "Git", "endorsements": 14},
        {"name": "Spark", "endorsements": 12},
        {"name": "Terraform", "endorsements": 8},
    ]
)

_SAMPLE_CERTIFICATIONS = pd.DataFrame(
    [
        {
            "name": "AWS Certified Data Analytics – Specialty",
            "authority": "Amazon Web Services",
            "issued_date": "2023-04",
        },
        {
            "name": "dbt Analytics Engineering Certification",
            "authority": "dbt Labs",
            "issued_date": "2022-06",
        },
        {
            "name": "Google Professional Data Engineer",
            "authority": "Google Cloud",
            "issued_date": "2022-11",
        },
    ]
)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def get_db_connection(
    db_path: str | Path = _DB_PATH,
) -> Optional[duckdb.DuckDBPyConnection]:
    """Open a read-only DuckDB connection, or return None on failure."""
    try:
        path = Path(db_path)
        if not path.exists():
            return None
        return duckdb.connect(str(path), read_only=True)
    except Exception:
        return None


def _safe_query(conn: duckdb.DuckDBPyConnection, sql: str) -> Optional[pd.DataFrame]:
    """Execute SQL and return a DataFrame, or None on error."""
    try:
        return conn.execute(sql).df()
    except Exception:
        return None


def get_sample_data(section: str) -> pd.DataFrame:
    """Return built-in sample data for a resume section."""
    mapping: dict[str, pd.DataFrame] = {
        "experience": _SAMPLE_EXPERIENCE,
        "education": _SAMPLE_EDUCATION,
        "skills": _SAMPLE_SKILLS,
        "certifications": _SAMPLE_CERTIFICATIONS,
    }
    if section not in mapping:
        raise ValueError(f"Unknown section: {section!r}")
    return mapping[section].copy()


def _extract_year(series: pd.Series) -> pd.Series:
    """Extract year strings from ISO 8601 date strings."""
    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.dt.year.astype("Int64").astype(str).where(parsed.notna(), other="")


def load_section(
    conn: Optional[duckdb.DuckDBPyConnection], section: str
) -> pd.DataFrame:
    """Load a resume section from DuckDB or fall back to sample data."""
    if conn is None:
        return get_sample_data(section)

    queries = {
        "experience": "SELECT * FROM experience ORDER BY start_date DESC",
        "education": "SELECT * FROM education ORDER BY end_date DESC",
        "skills": "SELECT * FROM skills ORDER BY endorsement_count DESC",
        "certifications": "SELECT * FROM certifications ORDER BY issued_date DESC",
    }
    if section not in queries:
        return get_sample_data(section)

    result = _safe_query(conn, queries[section])
    if result is None or result.empty:
        return get_sample_data(section)

    if section == "education":
        if "start_date" in result.columns and "start_year" not in result.columns:
            result = result.copy()
            result["start_year"] = _extract_year(result["start_date"])
        if "end_date" in result.columns and "end_year" not in result.columns:
            result = result.copy()
            result["end_year"] = _extract_year(result["end_date"])

    return result


def load_profile(conn: Optional[duckdb.DuckDBPyConnection]) -> dict[str, str]:
    """Load profile record from DuckDB or return sample data."""
    if conn is None:
        return _SAMPLE_PROFILE.copy()
    result = _safe_query(conn, "SELECT * FROM profile LIMIT 1")
    if result is None or result.empty:
        return _SAMPLE_PROFILE.copy()
    row = result.iloc[0].to_dict()
    row.setdefault("summary", _SAMPLE_PROFILE["summary"])
    return row


# ---------------------------------------------------------------------------
# HTML component builders (inlined — no cross-dir imports)
# ---------------------------------------------------------------------------


def _build_experience_card(row: pd.Series) -> ui.Tag:
    """Render one experience entry as a glassmorphism timeline card."""
    title = str(row.get("title", ""))
    company = str(row.get("company", "") or row.get("company_name", ""))
    location = str(row.get("location", ""))
    description = str(row.get("description", ""))

    raw_start = row.get("start_date", "")
    raw_end = row.get("end_date", "")
    is_current = bool(row.get("is_current", False))

    def _fmt(d: object) -> str:
        if d is None or (isinstance(d, float) and pd.isna(d)):
            return ""
        s = str(d)[:7]
        try:
            return pd.to_datetime(s).strftime("%b %Y")
        except Exception:
            return s

    start_label = _fmt(raw_start) or str(raw_start)
    end_label = "Present" if is_current else (_fmt(raw_end) or str(raw_end))
    date_range = f"{start_label} – {end_label}" if start_label else end_label

    return ui.div(
        ui.div(
            ui.span(title, class_="exp-title"),
            (ui.span("Current", class_="exp-badge") if is_current else ui.span("")),
            class_="exp-header",
        ),
        ui.div(company, class_="exp-company"),
        ui.div(
            ui.span(date_range),
            (ui.span(f"📍 {location}") if location else ui.span("")),
            class_="exp-meta",
        ),
        ui.div(description, class_="exp-description") if description else ui.span(""),
        class_="exp-card",
    )


def _build_education_card(row: pd.Series) -> ui.Tag:
    """Render one education entry."""
    degree = str(row.get("degree", ""))
    field = str(row.get("field_of_study", "") or row.get("field", ""))
    school = str(
        row.get("school", "")
        or row.get("school_name", "")
        or row.get("institution", "")
    )
    start_y = str(row.get("start_year", ""))
    end_y = str(row.get("end_year", ""))
    years = f"{start_y} – {end_y}".strip(" –") if (start_y or end_y) else ""

    return ui.div(
        ui.div("🎓", class_="edu-icon"),
        ui.div(
            ui.div(degree, class_="edu-degree"),
            ui.div(field, class_="edu-field") if field else ui.span(""),
            ui.div(school, class_="edu-school") if school else ui.span(""),
            ui.div(years, class_="edu-years") if years else ui.span(""),
        ),
        class_="edu-card",
    )


def _build_skill_bars(df: pd.DataFrame) -> ui.Tag:
    """Render animated skill bar chart rows."""
    top = df.head(10)
    if top.empty:
        return ui.span("")

    name_col = "name" if "name" in df.columns else "skill_name"
    end_col = "endorsements" if "endorsements" in df.columns else "endorsement_count"

    max_val = pd.to_numeric(top[end_col], errors="coerce").max()
    if pd.isna(max_val) or max_val == 0:
        max_val = 1

    rows = []
    for _, r in top.iterrows():
        name = str(r.get(name_col, ""))
        raw_count = r.get(end_col, 0)
        count = int(raw_count) if pd.notna(raw_count) else 0
        pct = int((count / max_val) * 100)
        rows.append(
            ui.div(
                ui.span(name, class_="skill-bar-label"),
                ui.div(
                    ui.div(
                        class_="skill-bar-fill",
                        style=f"width:{pct}%",
                    ),
                    class_="skill-bar-track",
                ),
                ui.span(str(count), class_="skill-bar-count"),
                class_="skill-bar-row",
            )
        )
    return ui.div(*rows, class_="skills-bars")


def _build_skill_tags(df: pd.DataFrame) -> ui.Tag:
    """Render skill badge tags."""
    name_col = "name" if "name" in df.columns else "skill_name"
    tags = [
        ui.span(str(r.get(name_col, "")), class_="skill-tag") for _, r in df.iterrows()
    ]
    return ui.div(*tags, class_="skills-tag-cloud")


def _build_cert_card(row: pd.Series) -> ui.Tag:
    """Render one certification card."""
    name = str(row.get("name", "") or row.get("cert_name", ""))
    issuer = str(row.get("issuer", "") or row.get("authority", ""))
    date = str(row.get("issued_date", "") or row.get("issue_date", ""))
    icons = {
        "amazon": "☁️",
        "aws": "☁️",
        "google": "🔵",
        "microsoft": "🪟",
        "dbt": "⚙️",
        "databricks": "🔶",
        "snowflake": "❄️",
    }
    icon = "🏅"
    for key, val in icons.items():
        if key in issuer.lower() or key in name.lower():
            icon = val
            break
    return ui.div(
        ui.span(icon, class_="cert-icon"),
        ui.span(name, class_="cert-name"),
        ui.span(issuer, class_="cert-issuer") if issuer else ui.span(""),
        ui.span(date, class_="cert-date") if date else ui.span(""),
        class_="cert-card",
    )


# ---------------------------------------------------------------------------
# JavaScript for animations
# ---------------------------------------------------------------------------

_JS = """
(function () {
  // Typewriter effect
  const headlines = [
    "Analytics Engineer · dbt · Snowflake · Python",
    "Data Platform Architect · Cloud · SQL",
    "Building reliable data systems since 2014",
  ];
  const el = document.getElementById("typewriter-text");
  if (el) {
    let hi = 0, ci = 0, deleting = false;
    function tick() {
      const full = headlines[hi];
      if (!deleting) {
        el.textContent = full.slice(0, ++ci);
        if (ci === full.length) { deleting = true; setTimeout(tick, 2200); return; }
      } else {
        el.textContent = full.slice(0, --ci);
        if (ci === 0) { deleting = false; hi = (hi + 1) % headlines.length; }
      }
      setTimeout(tick, deleting ? 40 : 80);
    }
    tick();
  }

  // Intersection observer — reveal sections + animate skill bars
  const io = new IntersectionObserver((entries) => {
    entries.forEach(e => {
      if (e.isIntersecting) {
        e.target.classList.add("visible");
        e.target.querySelectorAll(".skill-bar-fill").forEach(bar => {
          bar.style.width = bar.style.width; // trigger reflow
        });
      }
    });
  }, { threshold: 0.1 });

  document.querySelectorAll(".section").forEach(s => io.observe(s));

  // Nav dots
  const sections = ["hero","about","experience","education","skills","certifications"];
  const dots = document.querySelectorAll(".nav-dot");
  const scrollSpy = new IntersectionObserver((entries) => {
    entries.forEach(e => {
      if (e.isIntersecting) {
        const id = e.target.id;
        dots.forEach(d => {
          d.classList.toggle("active", d.dataset.target === id);
        });
      }
    });
  }, { threshold: 0.4 });

  sections.forEach(id => {
    const el = document.getElementById(id);
    if (el) scrollSpy.observe(el);
  });

  dots.forEach(dot => {
    dot.addEventListener("click", () => {
      const target = document.getElementById(dot.dataset.target);
      if (target) target.scrollIntoView({ behavior: "smooth" });
    });
  });
})();
"""

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------


def build_ui(css_path: Path = _CSS_PATH) -> ui.Tag:
    """Construct the full Nebula Dark resume UI."""
    fonts = ui.tags.link(
        rel="stylesheet",
        href=(
            "https://fonts.googleapis.com/css2?"
            "family=Inter:wght@400;500;600&"
            "family=Space+Grotesk:wght@700&"
            "display=swap"
        ),
    )
    css_tag = (
        ui.tags.link(rel="stylesheet", href="assets/style.css")
        if css_path.exists()
        else ui.span("")
    )

    nav = ui.div(
        *[
            ui.tags.button(
                class_="nav-dot",
                **{"data-target": sec, "data-label": label},
            )
            for sec, label in [
                ("hero", "Home"),
                ("about", "About"),
                ("experience", "Experience"),
                ("education", "Education"),
                ("skills", "Skills"),
                ("certifications", "Certifications"),
            ]
        ],
        class_="nav-dots",
    )

    bg = ui.div(
        ui.div(class_="orb orb-1"),
        ui.div(class_="orb orb-2"),
        ui.div(class_="orb orb-3"),
        class_="nebula-bg",
    )

    return ui.page_fluid(
        ui.head_content(fonts, css_tag),
        bg,
        nav,
        ui.div(
            # ---- Hero ----
            ui.div(
                ui.output_ui("hero_section"),
                id="hero",
                class_="hero-section",
            ),
            # ---- About ----
            ui.div(
                ui.div("/ About", class_="section-label"),
                ui.h2("About Me", class_="section-heading"),
                ui.output_ui("summary_section"),
                id="about",
                class_="section",
            ),
            # ---- Experience ----
            ui.div(
                ui.div("/ Experience", class_="section-label"),
                ui.h2("Experience", class_="section-heading"),
                ui.output_ui("experience_section"),
                id="experience",
                class_="section",
            ),
            # ---- Education ----
            ui.div(
                ui.div("/ Education", class_="section-label"),
                ui.h2("Education", class_="section-heading"),
                ui.output_ui("education_section"),
                id="education",
                class_="section",
            ),
            # ---- Skills ----
            ui.div(
                ui.div("/ Skills", class_="section-label"),
                ui.h2("Skills", class_="section-heading"),
                ui.output_ui("skills_section"),
                id="skills",
                class_="section",
            ),
            # ---- Certifications ----
            ui.div(
                ui.div("/ Certifications", class_="section-label"),
                ui.h2("Certifications", class_="section-heading"),
                ui.output_ui("certifications_section"),
                id="certifications",
                class_="section",
            ),
            # ---- Footer ----
            ui.div(
                ui.HTML(
                    'Built with <a href="https://shiny.posit.co/py/">Python Shiny</a>'
                    " &amp; ☕ in Calgary"
                ),
                class_="resume-footer",
            ),
            class_="resume-wrapper",
        ),
        ui.tags.script(ui.HTML(_JS)),
    )


app_ui = build_ui()

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------


def server(input, output, session) -> None:
    """Shiny server — loads data once per session, renders all sections."""
    conn = get_db_connection()

    profile = load_profile(conn)
    exp_df = reactive.Value(load_section(conn, "experience"))
    edu_df = reactive.Value(load_section(conn, "education"))
    skills_df = reactive.Value(load_section(conn, "skills"))
    certs_df = reactive.Value(load_section(conn, "certifications"))

    @output
    @render.ui
    def hero_section() -> ui.Tag:
        """Render the animated hero with name, typewriter headline, links."""
        name = profile.get("name", "")
        location = profile.get("location", "")
        linkedin = profile.get("linkedin_url", "")
        email = profile.get("email", "")

        links = []
        if linkedin:
            links.append(
                ui.tags.a(
                    "LinkedIn",
                    href=linkedin,
                    target="_blank",
                    class_="hero-link hero-link-primary",
                )
            )
        if email:
            links.append(
                ui.tags.a(
                    email,
                    href=f"mailto:{email}",
                    class_="hero-link hero-link-secondary",
                )
            )

        return ui.div(
            ui.div("Hello, I'm", class_="hero-eyebrow"),
            ui.h1(name, class_="hero-name"),
            ui.div(
                ui.tags.span(id="typewriter-text"),
                ui.span(class_="typewriter-cursor"),
                class_="hero-headline",
            ),
            (
                ui.div(
                    ui.HTML("📍 "),
                    ui.span(location),
                    class_="hero-location",
                )
                if location
                else ui.span("")
            ),
            ui.div(*links, class_="hero-links") if links else ui.span(""),
            ui.div(
                ui.div(class_="scroll-line"),
                ui.div("scroll", class_="scroll-label"),
                class_="scroll-hint",
            ),
        )

    @output
    @render.ui
    def summary_section() -> ui.Tag:
        """Render the professional summary card."""
        summary = profile.get("summary", "")
        if not summary:
            return ui.span("")
        return ui.div(
            ui.p(summary, class_="summary-text"),
            class_="summary-card",
        )

    @output
    @render.ui
    def experience_section() -> ui.Tag:
        """Render the experience timeline."""
        df = exp_df()
        if df is None or df.empty:
            return ui.p("No experience data.", class_="no-data")
        cards = [_build_experience_card(row) for _, row in df.iterrows()]
        return ui.div(*cards, class_="exp-timeline")

    @output
    @render.ui
    def education_section() -> ui.Tag:
        """Render education cards."""
        df = edu_df()
        if df is None or df.empty:
            return ui.p("No education data.", class_="no-data")
        cards = [_build_education_card(row) for _, row in df.iterrows()]
        return ui.div(*cards, class_="edu-list")

    @output
    @render.ui
    def skills_section() -> ui.Tag:
        """Render skill bars and tag cloud."""
        df = skills_df()
        if df is None or df.empty:
            return ui.p("No skills data.", class_="no-data")
        return ui.div(
            _build_skill_bars(df),
            _build_skill_tags(df),
        )

    @output
    @render.ui
    def certifications_section() -> ui.Tag:
        """Render certification cards."""
        df = certs_df()
        if df is None or df.empty:
            return ui.p("No certifications data.", class_="no-data")
        cards = [_build_cert_card(row) for _, row in df.iterrows()]
        return ui.div(*cards, class_="cert-grid")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = App(
    app_ui,
    server,
    static_assets=str(_APP_DIR / "assets"),
)
