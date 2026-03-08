# LinkedIn Resume AI

A Python Shiny web application that presents a LinkedIn profile as an interactive resume, built with Claude Code. Data is scraped from LinkedIn, transformed, stored in DuckDB, and rendered in a Shiny app deployed to shinyapps.io.

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
```

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

## Documentation

- [Architecture](docs/architecture.md) — data flow and layer responsibilities
- [Data](data/README.md) — data directory conventions

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.10 | Core language |
| Shiny (Python) | Web application framework |
| DuckDB | Local analytical database |
| pandas / numpy | Data manipulation |
| seaborn / matplotlib | Visualizations |
| pytest | Testing |
| Black / flake8 | Formatting and linting |
| GitHub Actions | CI/CD and deployment |
