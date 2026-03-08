# Data Directory

All data files are excluded from version control via `.gitignore`.

| Directory | Contents |
|-----------|----------|
| `raw/` | Raw scraped or ingested snapshots — never modify these |
| `interim/` | Intermediate datasets produced during cleaning/transformation |
| `processed/` | Final cleaned datasets consumed by the Shiny app |
| `duckdb/` | DuckDB database files |

## Adding a New Dataset

1. Document the source, date collected, and schema here.
2. Place the raw file in `raw/`.
3. Add any transformation logic to `/src/linkedin_project/transform/`.
4. Output processed data to `processed/`.
