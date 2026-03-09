# LinkedIn Resume AI

A Python Shiny web application that presents a LinkedIn profile as an interactive resume, built with Claude Code. Data is scraped from LinkedIn using a headless Chromium browser (Playwright), transformed, stored in DuckDB, and rendered in a Shiny app deployed to shinyapps.io.

**Live app:** https://chris-selig.shinyapps.io/linkedin-resume/

## Project Structure

```
.
├── app/                        # Python Shiny application
│   ├── components/             # Reusable UI modules
│   └── assets/                 # CSS, images, static files
├── src/linkedin_project/       # Core Python package
│   ├── scrape/                 # LinkedIn data collection
│   ├── transform/              # Data cleaning & normalization
│   ├── storage/                # DuckDB persistence layer
│   ├── pipelines/              # Orchestration scripts
│   └── utils/                  # Shared helpers
├── data/                       # Local data (git-ignored)
│   ├── raw/                    # Raw scraped snapshots
│   ├── interim/                # Intermediate datasets
│   ├── processed/              # Final datasets for the app
│   └── duckdb/                 # DuckDB database files
├── scripts/                    # CLI scripts for scraping/pipelines
├── tests/                      # Unit and integration tests
├── docs/                       # Architecture and data model docs
├── deployment/                 # Deployment config and scripts
└── .github/workflows/          # CI/CD → shinyapps.io
```

## Setup

```bash
# Create and activate virtual environment
python3.10 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install Playwright's Chromium browser (needed for scraping)
playwright install chromium
```

## Scraping LinkedIn data

The scraper uses a headless Chromium browser to log in and extract profile data.

```bash
export LINKEDIN_USERNAME="your@email.com"
export LINKEDIN_PASSWORD="yourpassword"
export LINKEDIN_PROFILE="chris-selig"   # LinkedIn public slug

python scripts/scrape_linkedin.py

# Optional: watch the browser (useful for debugging auth challenges)
HEADLESS=false python scripts/scrape_linkedin.py
```

This populates `data/duckdb/linkedin.db`, which the app reads automatically.

## Deploying the app

```bash
export SHINYAPPS_ACCOUNT="chris-selig"
export SHINYAPPS_TOKEN="your-token"
export SHINYAPPS_SECRET="your-secret"

bash deployment/deploy.sh
```

Overwrites the existing app at https://chris-selig.shinyapps.io/linkedin-resume/.

## Development Commands

```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=src

# Format code
black .

# Lint
flake8
```

## Workflow

1. Create a feature branch: `git checkout -b feature_<short_description>`
2. Write code with type hints and docstrings
3. Write tests in `/tests/`
4. Run `black .` and `flake8`, then `pytest` — all must pass
5. Commit and open a pull request

## CI/CD

| Trigger | Workflow | What it does |
|---------|----------|--------------|
| Pull request | `ci.yml` | black, flake8, pytest --cov |
| Push to `main` | `deploy.yml` | Deploys to shinyapps.io |

Required GitHub Actions secrets: `SHINYAPPS_ACCOUNT`, `SHINYAPPS_TOKEN`, `SHINYAPPS_SECRET`.

## Documentation

- [Architecture](docs/architecture.md) — data flow and layer responsibilities
- [Data](data/README.md) — data directory conventions

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.10 | Core language |
| Shiny (Python) | Web application framework |
| Playwright / Chromium | Headless browser scraping |
| DuckDB | Local analytical database |
| pandas / numpy | Data manipulation |
| pytest | Testing |
| Black / flake8 | Formatting and linting |
| GitHub Actions | CI/CD and deployment |
| rsconnect-python | shinyapps.io deployment |
