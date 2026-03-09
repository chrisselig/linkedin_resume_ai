#!/usr/bin/env python3
"""Scrape a LinkedIn profile using Playwright and store results in DuckDB.

Usage:
    export LINKEDIN_USERNAME="your@email.com"
    export LINKEDIN_PASSWORD="yourpassword"
    export LINKEDIN_PROFILE="chris-selig"      # LinkedIn public slug

    source .venv/bin/activate
    python scripts/scrape_linkedin.py

Optional: add HEADLESS=false to watch the browser while scraping.
    HEADLESS=false python scripts/scrape_linkedin.py
"""

import logging
import os
import sys
from pathlib import Path

# Ensure src/ is on sys.path before local imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from linkedin_project.scrape.scraper import (  # noqa: E402
    AuthError,
    ScraperError,
    scrape_profile,
)
from linkedin_project.storage import (  # noqa: E402
    connect,
    create_tables,
    upsert_profile,
)  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

DB_PATH = Path(__file__).parent.parent / "data" / "duckdb" / "linkedin.db"


def main() -> None:
    """Run the full scrape → store pipeline."""
    public_id = os.environ.get("LINKEDIN_PROFILE", "chris-selig")
    headless = os.environ.get("HEADLESS", "true").lower() != "false"

    print(f"Scraping LinkedIn profile: {public_id} (headless={headless})")

    try:
        data = scrape_profile(public_id=public_id, headless=headless)
    except AuthError as e:
        print(f"AUTH ERROR: {e}", file=sys.stderr)
        print(
            "\nTips:\n"
            "  1. Try running with HEADLESS=false to see the browser\n"
            "  2. Log in to LinkedIn manually in Chrome first to clear any\n"
            "     verification challenges\n"
            "  3. Ensure LINKEDIN_USERNAME and LINKEDIN_PASSWORD are correct",
            file=sys.stderr,
        )
        sys.exit(1)
    except ScraperError as e:
        print(f"SCRAPE ERROR: {e}", file=sys.stderr)
        sys.exit(1)

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
