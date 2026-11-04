# Dashboard hosting

## Recommended: public portfolio site

Publish a static React build on Vercel or Cloudflare Pages, backed by an exported snapshot of the Databricks Gold aggregates. Both can provide a public URL without a Databricks account or a running SQL warehouse. The snapshot should contain only the daily, category, and brand aggregates already used by the dashboard. Generate the forecast outputs from that snapshot during the export so the history, metric, horizon, and model controls remain interactive. Display the source data's last observed date in the UI.

This is a deployment plan, not a completed static build. The current React app calls `/api/dashboard` and `/api/forecast`; publishing `dashboard/web/dist` alone would leave those requests unanswered. The remaining work is to add a snapshot export, a static data adapter, and a refresh workflow. The export must never include the local Databricks token, user-level tables, or private workspace configuration.

After the static adapter is built, connect this repository to Vercel or Cloudflare Pages. Use `dashboard/web` as the project root, `npm ci && npm run build` as the build command, and `dist` as the output directory. Vercel's Hobby plan and Cloudflare Pages both offer free hosting within their limits. A new snapshot can be published by rebuilding the site after a manual or scheduled export.

Vercel also supports Python FastAPI functions. That makes a live API deployment possible, but the current long-running Uvicorn setup and process-local cache would need to be adapted for serverless execution. Each function instance may have a different cache, so repeated requests can reach the SQL warehouse. Python data-science dependencies also increase the function bundle and startup work. A service principal and cost controls would still be required for a public live API. For a recruiter portfolio, the static snapshot avoids these issues on either host.

## Live, account-restricted version

Databricks Apps can host the FastAPI service and its built React assets in the same workspace as the SQL warehouse. Give the app service principal `CAN USE` on the warehouse and `SELECT` on the three Gold tables. This keeps workspace credentials out of the browser and avoids deploying a personal access token. Databricks Apps require authenticated users in the Databricks account; they do not support an anonymous public recruiter link. App compute and SQL warehouse usage are billed while used.

## Public live API alternative

A public Python web service such as Render can serve the existing FastAPI and React bundle with relatively little code change. It would need a server-side Databricks service principal, query caching, and a limit on warehouse usage. Render's free web services sleep after 15 minutes without traffic and can take about a minute to restart. The SQL warehouse may still incur charges when the service runs queries. This is a weaker recruiter experience than the static site and requires more cost controls.

## Current state

Local development uses live Databricks Gold queries by default. No public deployment or data export has been configured yet. The public static path is the recommended next implementation step; Vercel and Cloudflare Pages are both viable hosts for it.

## References

- [Databricks App permissions and public access](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/permissions)
- [Databricks App resource access](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/resources)
- [Databricks Apps compute billing](https://docs.databricks.com/aws/en/dev-tools/databricks-apps)
- [Cloudflare Pages free plan limits](https://developers.cloudflare.com/pages/platform/limits/)
- [Vite on Vercel](https://vercel.com/docs/frameworks/frontend/vite)
- [Python FastAPI on Vercel](https://vercel.com/docs/functions/runtimes/python)
- [Vercel function state and limits](https://vercel.com/i/serverless-python)
- [Vercel Hobby plan](https://vercel.com/docs/plans/hobby)
- [Render free service limits](https://render.com/docs/free)
