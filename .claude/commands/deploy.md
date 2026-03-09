# Deploy to shinyapps.io

Deploys the Shiny app to shinyapps.io, **overwriting the existing app** named `linkedin-resume`.

## Prerequisites
Set these environment variables (or add them to your shell profile):
```bash
export SHINYAPPS_ACCOUNT="your-account-name"
export SHINYAPPS_TOKEN="your-token"
export SHINYAPPS_SECRET="your-secret"
```

## Run
```bash
source .venv/bin/activate
bash deployment/deploy.sh
```

This will overwrite the existing `linkedin-resume` app on shinyapps.io.
