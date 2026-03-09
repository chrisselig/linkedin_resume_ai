# Architecture

## Overview

This project scrapes LinkedIn profile data, transforms it, stores it in DuckDB, and surfaces it via a Python Shiny web application styled as an interactive resume.

## Data Flow

```
LinkedIn Profile
      │
      ▼
/src/linkedin_project/scrape/    ← raw data collection
      │
      ▼
/data/raw/                       ← raw snapshots stored here
      │
      ▼
/src/linkedin_project/transform/ ← cleaning, normalization
      │
      ▼
/data/processed/                 ← final datasets for the app
      │
      ▼
/src/linkedin_project/storage/   ← DuckDB persistence layer
      │
      ▼
/data/duckdb/                    ← DuckDB database files
      │
      ▼
/app/                            ← Python Shiny UI + server
```

## Key Layers

| Layer | Location | Responsibility |
|-------|----------|----------------|
| Scrape | `/src/linkedin_project/scrape/` | Headless Chromium (Playwright) — logs in to LinkedIn and scrapes profile sections via DOM selectors |
| Transform | `/src/linkedin_project/transform/` | Clean and normalize data; vectorized pandas operations |
| Storage | `/src/linkedin_project/storage/` | DuckDB read/write with parameterized queries |
| Pipelines | `/src/linkedin_project/pipelines/` | Orchestrate scrape → transform → load |
| App | `/app/` | Python Shiny UI (Nebula Dark theme) — reads from DuckDB, falls back to sample data |
| Utils | `/src/linkedin_project/utils/` | Shared helpers |

## Scraper

The scraper (`src/linkedin_project/scrape/scraper.py`) uses Playwright's `sync_api` to drive a headless Chromium browser:

1. Navigates to `https://www.linkedin.com/login` and submits credentials
2. Detects auth challenges (checkpoint/verification) and raises `AuthError`
3. Loads the target profile URL, scrolls to trigger lazy content, and clicks "Show all" links
4. Extracts experience, education, skills, and certifications via CSS selectors
5. Returns a `ProfileData` dataclass containing one pandas DataFrame per section

Run via `scripts/scrape_linkedin.py`. Set `HEADLESS=false` to watch the browser.

## App

The Shiny app (`app/app.py`) uses an inlined "Nebula Dark" design:
- Animated gradient orb background with CSS grid overlay
- Glassmorphism cards with neon hover glow
- Typewriter headline animation (JavaScript `IntersectionObserver`)
- Animated skill bars that fill on scroll
- Floating nav dots with active section tracking

All component rendering is inlined in `app.py` (no cross-directory imports) to ensure compatibility with shinyapps.io's deployment working directory. CSS is embedded as a `<style>` tag at startup.

## Database

DuckDB is used as the local analytical database. Files are stored in `/data/duckdb/linkedin.db`. Never commit `.duckdb` files to version control.

## Deployment

**Live app:** https://chris-selig.shinyapps.io/linkedin-resume/

The Shiny app is deployed to shinyapps.io via:
- **GitHub Actions** — `deploy.yml` runs on push to `main` using `rsconnect-python`
- **Manual** — `bash deployment/deploy.sh` (overwrites the existing app by title)

Required secrets: `SHINYAPPS_ACCOUNT`, `SHINYAPPS_TOKEN`, `SHINYAPPS_SECRET`.
