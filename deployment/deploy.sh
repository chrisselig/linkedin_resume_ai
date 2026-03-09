#!/usr/bin/env bash
# deploy.sh — Manual deployment script for LinkedIn Resume Shiny App
#
# Usage:
#   ./deployment/deploy.sh
#
# Required environment variables:
#   SHINYAPPS_ACCOUNT — shinyapps.io account name
#   SHINYAPPS_TOKEN   — shinyapps.io API token
#   SHINYAPPS_SECRET  — shinyapps.io API secret
#
# Optional environment variables:
#   VENV_PATH         — path to virtual environment (default: .venv in repo root)
#   APP_NAME          — name for the deployed app on shinyapps.io (default: linkedin-resume)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_PATH="${VENV_PATH:-${REPO_ROOT}/.venv}"
APP_NAME="${APP_NAME:-linkedin-resume}"
APP_DIR="${REPO_ROOT}/app"

# ── Validate required environment variables ──────────────────────────────────
required_vars=(SHINYAPPS_ACCOUNT SHINYAPPS_TOKEN SHINYAPPS_SECRET)
missing_vars=()

for var in "${required_vars[@]}"; do
    if [[ -z "${!var:-}" ]]; then
        missing_vars+=("$var")
    fi
done

if [[ ${#missing_vars[@]} -gt 0 ]]; then
    echo "ERROR: The following required environment variables are not set:" >&2
    for var in "${missing_vars[@]}"; do
        echo "  - $var" >&2
    done
    echo "" >&2
    echo "Export them before running this script, e.g.:" >&2
    echo "  export SHINYAPPS_ACCOUNT=your_account" >&2
    echo "  export SHINYAPPS_TOKEN=your_token" >&2
    echo "  export SHINYAPPS_SECRET=your_secret" >&2
    exit 1
fi

# ── Activate virtual environment ──────────────────────────────────────────────
if [[ ! -f "${VENV_PATH}/bin/activate" ]]; then
    echo "ERROR: Virtual environment not found at: ${VENV_PATH}" >&2
    echo "Create it with: python3.10 -m venv ${VENV_PATH} && pip install -r requirements.txt -r deployment/requirements-deploy.txt" >&2
    exit 1
fi

echo "Activating virtual environment: ${VENV_PATH}"
# shellcheck disable=SC1091
source "${VENV_PATH}/bin/activate"

# ── Verify rsconnect is available ─────────────────────────────────────────────
if ! command -v rsconnect &>/dev/null; then
    echo "ERROR: rsconnect not found. Install it with: pip install rsconnect-python" >&2
    exit 1
fi

# ── Verify app directory exists ───────────────────────────────────────────────
if [[ ! -d "${APP_DIR}" ]]; then
    echo "ERROR: App directory not found: ${APP_DIR}" >&2
    exit 1
fi

# ── Configure rsconnect credentials ──────────────────────────────────────────
echo "Configuring rsconnect credentials for account: ${SHINYAPPS_ACCOUNT}"
rsconnect add shinyapps \
    --account "${SHINYAPPS_ACCOUNT}" \
    --token "${SHINYAPPS_TOKEN}" \
    --secret "${SHINYAPPS_SECRET}"

# ── Deploy ────────────────────────────────────────────────────────────────────
echo "Deploying '${APP_NAME}' from ${APP_DIR} to shinyapps.io..."
rsconnect deploy shiny \
    --account "${SHINYAPPS_ACCOUNT}" \
    --name "${APP_NAME}" \
    "${APP_DIR}"

echo "Deployment complete."
