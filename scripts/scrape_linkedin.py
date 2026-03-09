#!/usr/bin/env python3
"""Scrape a LinkedIn profile and store results in DuckDB.

Usage:
    export LINKEDIN_USERNAME="your@email.com"
    export LINKEDIN_PASSWORD="yourpassword"
    export LINKEDIN_PROFILE="chris-selig"          # LinkedIn public slug
    python scripts/scrape_linkedin.py
"""

import os
import sys
from pathlib import Path

# Ensure src/ is on the path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from linkedin_project.scrape import scrape_profile  # noqa: E402
from linkedin_project.storage import (  # noqa: E402
    connect,
    create_tables,
    upsert_profile,
)

DB_PATH = Path(__file__).parent.parent / "data" / "duckdb" / "linkedin.db"


def main() -> None:
    """Run the full scrape → store pipeline."""
    username = os.environ.get("LINKEDIN_USERNAME")
    password = os.environ.get("LINKEDIN_PASSWORD")
    public_id = os.environ.get("LINKEDIN_PROFILE", "chris-selig")

    if not username or not password:
        print(
            "ERROR: Set LINKEDIN_USERNAME and LINKEDIN_PASSWORD env vars.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Scraping LinkedIn profile: {public_id}")
    data = scrape_profile(public_id)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(str(DB_PATH))
    create_tables(conn)

    sections = {
        "experience": data.experience,
        "education": data.education,
        "skills": data.skills,
        "certifications": data.certifications,
    }

    for section, df in sections.items():
        if df is not None and not df.empty:
            upsert_profile(conn, section, df)
            print(f"  ✓ {section}: {len(df)} records stored")
        else:
            print(f"  - {section}: no data returned")

    print(f"\nDone. Database: {DB_PATH}")


if __name__ == "__main__":
    main()
