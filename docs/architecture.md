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
| Scrape | `/src/linkedin_project/scrape/` | Collect raw LinkedIn data |
| Transform | `/src/linkedin_project/transform/` | Clean and normalize data |
| Storage | `/src/linkedin_project/storage/` | DuckDB read/write operations |
| Pipelines | `/src/linkedin_project/pipelines/` | Orchestrate scrape → transform → load |
| App | `/app/` | Shiny UI and server logic |
| Utils | `/src/linkedin_project/utils/` | Shared helpers |

## Database

DuckDB is used as the local analytical database. Files are stored in `/data/duckdb/`. Never commit `.duckdb` files to version control.

## Deployment

The Shiny app is deployed to shinyapps.io via GitHub Actions (`.github/workflows/`). See `/deployment/` for configuration details.
