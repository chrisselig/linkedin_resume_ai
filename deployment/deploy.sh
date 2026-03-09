#!/usr/bin/env bash
# Deploy the LinkedIn Resume Shiny app to shinyapps.io.
# Overwrites the existing app named "linkedin-resume" if it already exists.
set -euo pipefail

VENV_PATH="${VENV_PATH:-.venv}"
APP_TITLE="${APP_TITLE:-linkedin-resume}"
APP_DIR="$(dirname "$0")/../app"

# Validate required env vars
missing=()
[[ -z "${SHINYAPPS_ACCOUNT:-}" ]] && missing+=("SHINYAPPS_ACCOUNT")
[[ -z "${SHINYAPPS_TOKEN:-}" ]]   && missing+=("SHINYAPPS_TOKEN")
[[ -z "${SHINYAPPS_SECRET:-}" ]]  && missing+=("SHINYAPPS_SECRET")

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "ERROR: Missing required environment variables:" >&2
  printf '  %s\n' "${missing[@]}" >&2
  exit 1
fi

# Activate virtual environment
if [[ -f "${VENV_PATH}/bin/activate" ]]; then
  source "${VENV_PATH}/bin/activate"
else
  echo "ERROR: Virtual environment not found at ${VENV_PATH}" >&2
  exit 1
fi

# Verify rsconnect is available
if ! command -v rsconnect &>/dev/null; then
  echo "ERROR: rsconnect not found. Run: pip install rsconnect-python" >&2
  exit 1
fi

echo "Deploying '${APP_TITLE}' to shinyapps.io (account: ${SHINYAPPS_ACCOUNT})..."
echo "Using app dir: ${APP_DIR}"
echo ""

rsconnect deploy shiny \
  --account "${SHINYAPPS_ACCOUNT}" \
  --token   "${SHINYAPPS_TOKEN}" \
  --secret  "${SHINYAPPS_SECRET}" \
  --title   "${APP_TITLE}" \
  "${APP_DIR}"

echo ""
echo "Done. App live at: https://${SHINYAPPS_ACCOUNT}.shinyapps.io/${APP_TITLE}"
