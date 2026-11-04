"""Read-only API for the Commerce Intelligence dashboard."""

from __future__ import annotations

import math
import logging
import os
import time
from datetime import date, timedelta
from pathlib import Path
from threading import Lock

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service.sql import StatementState
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .forecasting import METRICS, MODELS, compare_and_forecast

CATALOG = os.getenv("DATABRICKS_CATALOG", "product_analytics")
SCHEMA = os.getenv("DATABRICKS_SCHEMA", "ecommerce")
WAREHOUSE = os.getenv("DATABRICKS_WAREHOUSE_ID", "b1660d805834aacd")
DEMO_MODE = os.getenv("DEMO_MODE", "").lower() == "true"
ON_VERCEL = bool(os.getenv("VERCEL"))
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"
TTL_SECONDS = 300
_cache: tuple[float, dict] | None = None
_lock = Lock()

app = FastAPI(title="Commerce Intelligence API", docs_url=None, redoc_url=None)
logger = logging.getLogger(__name__)


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok", "mode": "demo" if DEMO_MODE else "live"}


def _query(client: WorkspaceClient, sql: str) -> list[dict]:
    result = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE,
        statement=sql,
        catalog=CATALOG,
        schema=SCHEMA,
        wait_timeout="30s",
    )
    deadline = time.monotonic() + 90
    while result.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.monotonic() > deadline:
            raise TimeoutError("Databricks SQL query exceeded 90 seconds")
        time.sleep(0.5)
        result = client.statement_execution.get_statement(result.statement_id)
    if result.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"Databricks SQL query failed: {result.status.error}")
    if not result.result or not result.result.data_array:
        return []
    columns = [column.name for column in result.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in result.result.data_array]


def _number(value: object) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _load_live() -> dict:
    if ON_VERCEL:
        required = ("DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET", "DATABRICKS_WAREHOUSE_ID")
        if any(not os.getenv(name) for name in required) or os.getenv("DATABRICKS_TOKEN"):
            raise RuntimeError("Production Databricks OAuth configuration is incomplete or conflicts with a PAT")
    if "your-workspace" in os.getenv("DATABRICKS_HOST", ""):
        raise RuntimeError("DATABRICKS_HOST is still a placeholder. Use a valid Databricks profile.")
    client = WorkspaceClient()
    daily = _query(
        client,
        f"""SELECT full_date, CAST(total_revenue AS DOUBLE) AS total_revenue,
            total_purchases, daily_active_users, total_views, total_carts,
            overall_conversion_rate
            FROM {TABLE_PREFIX}.gold_daily_metrics ORDER BY full_date""",
    )
    brands = _query(
        client,
        f"""SELECT brand AS name, CAST(SUM(total_revenue) AS DOUBLE) AS revenue,
            SUM(total_purchases) AS purchases, SUM(total_quantity_sold) AS units
            FROM {TABLE_PREFIX}.gold_product_performance
            WHERE brand IS NOT NULL
            GROUP BY brand ORDER BY revenue DESC LIMIT 12""",
    )
    categories = _query(
        client,
        f"""SELECT category_l1 AS name, CAST(SUM(total_revenue) AS DOUBLE) AS revenue,
            SUM(total_purchases) AS purchases
            FROM {TABLE_PREFIX}.gold_category_performance
            WHERE category_l1 IS NOT NULL
            GROUP BY category_l1 ORDER BY revenue DESC""",
    )
    if not daily:
        raise RuntimeError("Gold daily metrics returned no rows")
    return {
        "daily": [
            {
                "date": str(row["full_date"])[:10],
                **{
                    key: _number(row.get(key))
                    for key in (
                        "total_revenue",
                        "total_purchases",
                        "daily_active_users",
                        "total_views",
                        "total_carts",
                        "overall_conversion_rate",
                    )
                },
            }
            for row in daily
        ],
        "brands": [
            {"name": row["name"], "revenue": _number(row["revenue"]),
             "purchases": _number(row["purchases"]), "units": _number(row["units"])}
            for row in brands
        ],
        "categories": [
            {"name": row["name"], "revenue": _number(row["revenue"]),
             "purchases": _number(row["purchases"])}
            for row in categories
        ],
        "mode": "live",
    }


def _load_demo() -> dict:
    """Deterministic sample records for local visual review only."""
    import random

    randomizer = random.Random(47)
    start = date(2019, 8, 1)
    daily = []
    for offset in range(120):
        current = start + timedelta(days=offset)
        weekly = 1.22 if current.weekday() in (5, 6) else 1.0
        season = 1 + 0.0017 * offset
        revenue = max(0, (6900000 * weekly * season) + randomizer.gauss(0, 480000))
        views = max(0, 310000 + 18000 * weekly + randomizer.gauss(0, 11000))
        carts = views * (0.255 + randomizer.uniform(-0.018, 0.018))
        purchases = carts * (0.18 + randomizer.uniform(-0.012, 0.012))
        users = views * (0.58 + randomizer.uniform(-0.025, 0.025))
        daily.append({
            "date": current.isoformat(),
            "total_revenue": round(revenue, 2),
            "total_purchases": round(purchases),
            "daily_active_users": round(users),
            "total_views": round(views),
            "total_carts": round(carts),
            "overall_conversion_rate": round(purchases / views * 100, 2),
        })
    brand_names = ["Samsung", "Apple", "Xiaomi", "Huawei", "Lenovo", "Sony", "LG", "Asus", "Philips", "Bosch", "Acer", "Canon"]
    brands = [
        {"name": name, "revenue": round(142000000 * (0.77 ** index)),
         "purchases": round(114000 * (0.79 ** index)), "units": round(123000 * (0.8 ** index))}
        for index, name in enumerate(brand_names)
    ]
    category_names = ["Electronics", "Appliances", "Computers", "Home", "Accessories", "Mobile", "Audio", "Other"]
    categories = [
        {"name": name, "revenue": round(335000000 * (0.62 ** index)),
         "purchases": round(290000 * (0.65 ** index))}
        for index, name in enumerate(category_names)
    ]
    return {"daily": daily, "brands": brands, "categories": categories, "mode": "demo"}


def _data() -> dict:
    global _cache
    if ON_VERCEL and DEMO_MODE:
        raise RuntimeError("DEMO_MODE is disabled for the public deployment")
    with _lock:
        if _cache and time.monotonic() - _cache[0] < TTL_SECONDS:
            return _cache[1]
        data = _load_demo() if DEMO_MODE else _load_live()
        _cache = (time.monotonic(), data)
        return data


def _service_error(exc: Exception) -> HTTPException:
    logger.exception("Dashboard data request failed")
    if isinstance(exc, PermissionDenied):
        detail = "Databricks authentication failed. Check the service principal permissions." if ON_VERCEL else "Databricks authentication failed. Your local token may have expired. Sign in again with a valid Databricks profile."
        return HTTPException(status_code=503, detail=detail)
    if isinstance(exc, TimeoutError):
        return HTTPException(status_code=503, detail="The Databricks SQL warehouse did not respond in time. Try again after it starts.")
    return HTTPException(status_code=503, detail="Live data could not be loaded. Check the API terminal for the Databricks error.")


def _cache_public_response(response: Response) -> None:
    if ON_VERCEL:
        response.headers["Cache-Control"] = "public, max-age=0, s-maxage=60, stale-while-revalidate=60"


@app.get("/api/dashboard")
def dashboard(response: Response, days: int = Query(30, ge=14, le=90)) -> dict:
    try:
        data = _data()
        _cache_public_response(response)
        recent = data["daily"][-days:]
        previous = data["daily"][-2 * days : -days]
        return {
            "mode": data["mode"],
            "source": f"{TABLE_PREFIX}.gold_daily_metrics",
            "as_of": data["daily"][-1]["date"],
            "coverage": {"start": data["daily"][0]["date"], "end": data["daily"][-1]["date"], "days": len(data["daily"])},
            "days": days,
            "daily": recent,
            "previous": previous,
            "brands": data["brands"],
            "categories": data["categories"],
        }
    except Exception as exc:
        raise _service_error(exc) from exc


@app.get("/api/forecast")
def forecast(
    response: Response,
    metric: str = Query("revenue"),
    days: int = Query(60, ge=14, le=90),
    horizon: int = Query(7, ge=7, le=28),
    model: str | None = Query(None),
) -> dict:
    if metric not in METRICS or (model is not None and model not in MODELS):
        raise HTTPException(status_code=400, detail="Unsupported metric or model")
    try:
        data = _data()
        result = compare_and_forecast(data["daily"], metric, days, horizon, model)
        _cache_public_response(response)
        return {"mode": data["mode"], **result}
    except Exception as exc:
        raise _service_error(exc) from exc


WEB_DIST = Path(__file__).resolve().parents[1] / "web" / "dist"
if WEB_DIST.exists():
    app.mount("/assets", StaticFiles(directory=WEB_DIST / "assets"), name="assets")

    @app.get("/{path:path}")
    def web_app(path: str):
        requested = WEB_DIST / path
        return FileResponse(requested if requested.is_file() else WEB_DIST / "index.html")
