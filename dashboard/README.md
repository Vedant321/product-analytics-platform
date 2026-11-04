# Commerce Intelligence dashboard

A React/TypeScript interface backed by a read-only FastAPI service over the existing Databricks Gold tables.

## Local development

From the repository root:

```bash
cd dashboard/web
npm run dev
```

Open <http://localhost:5173>. The command creates a Python 3.12 environment with `uv` if needed, installs missing dependencies, checks Databricks authentication, starts the Python API on port 8000, waits for its health endpoint, then starts Vite. **Live Databricks data is the default.**

For a design-only preview with deterministic sample data:

```bash
DEMO_MODE=true npm run dev
```

If an older Vite process still holds port 5173, stop it with **Ctrl+C** first. If port 8000 already runs this dashboard API, the launcher reuses it. To run the web preview on a different port:

```bash
DASHBOARD_WEB_PORT=5174 npm run dev
```

The root repository `.env` contains a placeholder host and should not be loaded. For the existing personal access token setup, update the `token` value in your local `~/.databrickscfg` profile with a newly generated token for the configured workspace. Keep the token out of this repository, the browser, and chat. Then run `npm run dev` again. The launcher checks authentication before starting the live API.

If you prefer OAuth later, create a profile with the official Databricks CLI:

```bash
brew install databricks/tap/databricks
databricks auth login --host https://dbc-eedc1a2c-39d4.cloud.databricks.com --profile commerce-iq
DATABRICKS_CONFIG_PROFILE=commerce-iq npm run dev
```

Complete the browser sign-in yourself. The Databricks SDK can then use that profile without a token in source code. The API reads `gold_daily_metrics`, `gold_product_performance`, and `gold_category_performance`. The SQL warehouse ID defaults to the current project warehouse and can be overridden with `DATABRICKS_WAREHOUSE_ID`. Do not put credentials in the React app.

## What each view answers

- **Overview:** What happened in the selected daily window? Revenue, purchases, active users, conversion, trend, and the event funnel.
- **Performance:** Which brands and categories drive all-time revenue? These Gold tables do not contain dates, so this view intentionally does not show the history selector.
- **Forecast Lab:** What might happen next? Five methods use the selected daily window; the last few days are held out for the same MAE comparison.
- **Data model:** What tables feed the interface, and what date coverage is available?

The preview banner labels synthetic data. Forecasts do not use promotions, stock, or other external signals. The chart does not claim a confidence interval because one is not yet calibrated.

## Deployment

Build the React bundle with `npm run build` from `dashboard/web`. The FastAPI app serves that bundle from `dashboard/web/dist`, so a single Python service can host the UI and API. See [deployment options](DEPLOYMENT.md) for public access and Databricks-hosted access.
