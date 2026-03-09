# Parallel Feature Development Command

I want to develop features in parallel using Git worktrees and subagents: $ARGUMENTS

You are in the parent folder of the main repo. You will need to change to the main repo folder to create the worktrees.

Please execute this complete workflow:

## PHASE 1 - SETUP WORKTREES

Change into the `linkedin_resume_ai` directory, then create the following four worktrees:

| Worktree path | Branch |
|---|---|
| `../linkedin-resume-scrape` | `feature/scrape` |
| `../linkedin-resume-transform` | `feature/transform` |
| `../linkedin-resume-cicd` | `feature/cicd` |
| `../linkedin-resume-shiny` | `feature/shiny` |

For each worktree:
1. Create it with `git worktree add`
2. Activate the `.venv` virtual environment (located in the main repo root) — do not create a new one
3. List all worktrees after setup to confirm

## PHASE 2 - SPAWN SUBAGENTS

Run all four subagents **in parallel**. Each works in its isolated worktree directory.

---

### Subagent 1 — LinkedIn Scraping (`../linkedin-resume-scrape`)
- Working directory: `../linkedin-resume-scrape`
- Implement `src/linkedin_project/scrape/` — functions to collect LinkedIn profile data (experience, education, skills, certifications, summary)
- Define and document the output schema as a pandas DataFrame with explicit column names and dtypes in `src/linkedin_project/scrape/schema.py` — this schema is the contract for the transform subagent
- Follow PEP 8, add type hints and docstrings to all public functions
- Write unit tests in `tests/test_scrape.py` using mocked HTTP responses (do not make real network calls)
- Run `pytest tests/test_scrape.py` and ensure all tests pass
- Run `black .` and `flake8` and fix any issues
- Do not run the application
- Write a summary to `../linkedin_resume_ai/scrape.work.txt` covering: what was implemented, files created/modified, output schema definition, dependencies added, testing approach, and integration notes

---

### Subagent 2 — Data Wrangling / Transform (`../linkedin-resume-transform`)
- Working directory: `../linkedin-resume-transform`
- Implement `src/linkedin_project/transform/` — cleaning, normalization, and transformation logic
- Implement `src/linkedin_project/storage/` — DuckDB read/write operations (use parameterized queries only, no string interpolation in SQL)
- Assume the scraping output schema defined in `src/linkedin_project/scrape/schema.py`; if it does not exist yet, define reasonable placeholder column names and note the assumption in your work summary
- Use vectorized pandas operations; avoid row-level loops
- Write unit tests in `tests/test_transform.py` and `tests/test_storage.py`
- Run `pytest tests/test_transform.py tests/test_storage.py` and ensure all tests pass
- Run `black .` and `flake8` and fix any issues
- Do not run the application
- Write a summary to `../linkedin_resume_ai/transform.work.txt` covering: what was implemented, files created/modified, assumed schema (and any deviations from scrape schema), dependencies added, testing approach, and integration notes

---

### Subagent 3 — CI/CD (`../linkedin-resume-cicd`)
- Working directory: `../linkedin-resume-cicd`
- Implement `.github/workflows/` with two workflows:
  1. `ci.yml` — runs on every pull request: activate venv, run `black --check .`, run `flake8`, run `pytest --cov=src`
  2. `deploy.yml` — runs on push to `main`: deploys the Shiny app to shinyapps.io using `rsconnect-python`
- Add `rsconnect-python` to `requirements.txt` if not already present
- Implement `deployment/` — any deployment config or helper scripts needed
- Do not run the workflows (no `act` or live deploys)
- Write a summary to `../linkedin_resume_ai/cicd.work.txt` covering: what was implemented, files created/modified, required GitHub Actions secrets (e.g. shinyapps.io token), dependencies added, and integration notes

---

### Subagent 4 — Python Shiny App (`../linkedin-resume-shiny`)
- Working directory: `../linkedin-resume-shiny`
- Implement `app/` — a Python Shiny application presenting the LinkedIn resume data interactively
- Implement `app/components/` — reusable UI modules (e.g. experience card, skills section, education panel)
- Implement `app/assets/` — CSS styling for a clean resume look
- The app should read from the DuckDB database in `data/duckdb/`; stub the DB connection if data is not yet available
- Follow PEP 8, add type hints and docstrings to all public functions and Shiny modules
- Write unit tests in `tests/test_app.py` for any logic that can be tested outside the Shiny runtime
- Run `pytest tests/test_app.py` and ensure all tests pass
- Run `black .` and `flake8` and fix any issues
- Do not start the Shiny server
- Write a summary to `../linkedin_resume_ai/shiny.work.txt` covering: what was implemented, files created/modified, DB connection assumptions, dependencies added, testing approach, and integration notes

---

## PHASE 3 - COORDINATION

- Monitor all four subagents working in parallel
- Ensure each subagent completes and creates its `.work.txt` summary file
- Flag any schema mismatches between the scrape and transform subagents for resolution in Phase 4

## PHASE 4 - FINAL SUMMARY

After all subagents complete:
1. Read all four `.work.txt` files: `scrape.work.txt`, `transform.work.txt`, `cicd.work.txt`, `shiny.work.txt`
2. Check for schema conflicts between scrape and transform outputs and note them explicitly
3. Provide a comprehensive summary of what was accomplished across all four features
4. List each feature, its status, and any open integration issues
5. Provide prioritized next steps for merging the worktrees into `main`
