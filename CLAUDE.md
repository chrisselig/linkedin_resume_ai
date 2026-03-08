# CLAUDE.md
# Project: LinkedIn Resume Shiny App

## Project Overview

A LinkedIn resume built/generated with Claude Code.

## Important
1. Before you make any change, create and checkout a feature branch named "feature_some_short_name". Make and then commit your changes in this branch.
2. You must write automated tests for all code.
3. You must compile the code and pass ALL tests before committing.
4. Code Reviews: Submit a pull request for code reviews before merging into the main branch.
5. Install all python libraries to the virtual environment

## Development Standards
- **Language**: Python 3.10.12
- **Code Style**: Follow PEP 8 strictly, use Black for formatting
- **Type Hints**: Required for all function signatures and class definitions
- **Documentation**: Docstrings required for all public functions and classes

## Workflow Requirements
1. Create feature branch: `feature-datapipeline-[description]` or `feature-visualization-[description]`
2. Write unit tests for all data processing functions
3. Run `pytest` and ensure all tests pass
4. Run `black .` and `flake8` before committing
5. Update relevant documentation in `/docs` if adding new features

##Project Structure
- `/app`: Python Shiny application (UI, server logic, and frontend assets)
- `/app/components`: Reusable UI modules and Shiny components
- `/app/assets`: Static assets such as CSS, images, and styling files
- `/src/linkedin_project`: Main Python package containing the core project logic
- `/src/linkedin_project/scrape`: Code responsible for collecting LinkedIn profile data
- `/src/linkedin_project/transform`: Data cleaning, normalization, and transformation logic
- `/src/linkedin_project/storage`: Data persistence logic including DuckDB interactions
- `/src/linkedin_project/pipelines`: Pipeline orchestration scripts that run scraping, cleaning, and loading steps
- `/src/linkedin_project/utils`: Shared helper functions used across modules
- `/data`: Local project data directory
- `/data/raw`: Raw scraped or ingested data snapshots
- `/data/interim`: Intermediate datasets produced during cleaning or transformation
- `/data/processed`: Final cleaned datasets used by the Shiny app
- `/data/duckdb`: DuckDB database files used for querying and analytics
- `/scripts`: Command-line scripts used to run scraping, transformation, or full pipelines
- `/tests`: Unit and integration tests for project components
- `/.github/workflows`: GitHub Actions workflows used for CI/CD and automatic deployment to shinyapps.io
- `/deployment`: Deployment-related configuration and scripts
- `/docs`: Project documentation such as architecture notes, data models, and deployment instructions

## Data Handling Standards
- Use Pandas for data manipulation, prefer vectorized operations
- All data files must be documented in `/data/README.md`
- Never commit raw data files to version control
- Use environment variables for database connections and API keys


## Dependencies
- Core: pandas, numpy, seaborn
- Data: duckdb
- Testing: pytest, pytest-cov
