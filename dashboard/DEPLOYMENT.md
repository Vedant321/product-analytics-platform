# Live dashboard deployment

## Target architecture

Deploy the React interface and FastAPI service on Vercel. Browser controls call `/api/dashboard` and `/api/forecast`. The backend reads the Databricks Gold tables using a dedicated service principal and computes model comparisons and forecasts for each requested history window, metric, horizon, and model. No forecast values or Gold data are bundled into the frontend.

The production backend must use Databricks OAuth machine-to-machine authentication rather than a personal access token. Store `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_CATALOG`, and `DATABRICKS_SCHEMA` as server-side Vercel environment variables. Never expose credentials through Vite variables or browser responses.

Grant the service principal `CAN USE` on the SQL warehouse, `USE CATALOG` on the catalog, `USE SCHEMA` on the schema, and `SELECT` on `gold_daily_metrics`, `gold_product_performance`, and `gold_category_performance`. These are read-only permissions.

Use a short CDN response cache for public, identical queries to control warehouse usage. Include all request parameters in cache keys, and display the last observed Gold date in the UI. Cache expiry may delay a newly loaded Gold row briefly; it does not substitute generated or precomputed data. Rate limiting and query timeouts should protect the warehouse from abusive traffic.

## What remains to deploy

1. Set up a Databricks service principal with OAuth and the read-only permissions above.
2. Configure the service principal credentials as Vercel secrets, connect the Git repository, and deploy a preview URL using the root `vercel.json` configuration.
3. Verify the Vercel build and every view, control, forecast method, and error state against the live Gold tables before making the deployment public.
4. Set usage and cost alerts for Vercel and the SQL warehouse.

The repository now includes Vercel service routing and production credential checks, but no Vercel deployment has been created or verified yet. Vercel Services is currently a beta feature. A live API reads the latest rows available in Gold, but it cannot show newer events until the ingestion and Gold refresh pipelines load them.

## References

- [Vercel Python and FastAPI runtime](https://vercel.com/docs/functions/runtimes/python)
- [Vercel Services for a frontend and Python backend](https://vercel.com/kb/guide/vercel-services)
- [Vercel CDN cache control](https://vercel.com/docs/caching/cache-control-headers)
- [Databricks OAuth for service principals](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m)
- [Unity Catalog read privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/permissions-concepts)
