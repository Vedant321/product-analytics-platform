"""Small, explainable forecasting service for daily Gold metrics."""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.statespace.sarimax import SARIMAX

MODELS = (
    "Seasonal naive",
    "Moving average",
    "Holt trend",
    "Holt-Winters",
    "SARIMA",
)
METRICS = {
    "revenue": "total_revenue",
    "purchases": "total_purchases",
    "users": "daily_active_users",
}


def predict(values: np.ndarray, steps: int, model: str) -> np.ndarray:
    values = np.asarray(values, dtype=float)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        if model == "Seasonal naive":
            if len(values) < 7:
                raise ValueError("Needs 7 training days")
            return np.resize(values[-7:], steps)
        if model == "Moving average":
            if len(values) < 7:
                raise ValueError("Needs 7 training days")
            return np.full(steps, np.mean(values[-min(28, len(values)) :]))
        if model == "Holt trend":
            if len(values) < 14:
                raise ValueError("Needs 14 training days")
            fitted = ExponentialSmoothing(
                values, trend="add", damped_trend=True, initialization_method="estimated"
            ).fit(optimized=True)
            return np.asarray(fitted.forecast(steps))
        if model == "Holt-Winters":
            if len(values) < 21:
                raise ValueError("Needs 21 training days")
            fitted = ExponentialSmoothing(
                values,
                trend="add",
                damped_trend=True,
                seasonal="add",
                seasonal_periods=7,
                initialization_method="estimated",
            ).fit(optimized=True)
            return np.asarray(fitted.forecast(steps))
        if model == "SARIMA":
            if len(values) < 35:
                raise ValueError("Needs 35 training days")
            fitted = SARIMAX(
                values,
                order=(1, 1, 1),
                seasonal_order=(1, 0, 1, 7),
                enforce_stationarity=False,
                enforce_invertibility=False,
            ).fit(disp=False, maxiter=80)
            return np.asarray(fitted.forecast(steps))
    raise ValueError(f"Unknown model: {model}")


def compare_and_forecast(
    daily: list[dict], metric: str, history_days: int, horizon: int, selected: str | None
) -> dict:
    if metric not in METRICS:
        raise ValueError("Unsupported metric")
    if selected is not None and selected not in MODELS:
        raise ValueError("Unsupported model")
    frame = pd.DataFrame(daily)
    if frame.empty:
        return {"error": "No history available"}
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["value"] = pd.to_numeric(frame[METRICS[metric]], errors="coerce")
    frame = frame.dropna(subset=["date", "value"]).sort_values("date")
    frame = frame.drop_duplicates(subset="date", keep="last").tail(history_days)
    series = frame.set_index("date")["value"]
    if len(series) < 14 or (series.index.to_series().diff().dropna().dt.days != 1).any():
        return {"error": "At least 14 consecutive daily observations are required."}
    holdout = min(7, max(3, len(series) // 5))
    training, actual = series.iloc[:-holdout], series.iloc[-holdout:]
    scores: list[dict] = []
    backtests: dict[str, list[float]] = {}
    for name in MODELS:
        try:
            estimate = predict(training.to_numpy(), holdout, name)
            if not np.isfinite(estimate).all():
                raise ValueError("Forecast was not finite")
            estimate = np.maximum(estimate, 0)
            mae = float(np.mean(np.abs(actual.to_numpy() - estimate)))
            scores.append({"model": name, "mae": round(mae, 2), "status": "available"})
            backtests[name] = [round(float(value), 2) for value in estimate]
        except Exception as exc:
            scores.append({"model": name, "mae": None, "status": str(exc)})
    available = [row for row in scores if row["mae"] is not None]
    if not available:
        return {"error": "No model could be scored on this history.", "scores": scores}
    recommended = min(available, key=lambda row: row["mae"])["model"]
    chosen = selected if selected in backtests else recommended
    forecast = np.maximum(predict(series.to_numpy(), horizon, chosen), 0)
    dates = pd.date_range(series.index[-1] + pd.Timedelta(days=1), periods=horizon)
    return {
        "metric": metric,
        "history_days": len(series),
        "horizon": horizon,
        "holdout_days": holdout,
        "recommended": recommended,
        "selected": chosen,
        "scores": scores,
        "history": [
            {"date": date.date().isoformat(), "value": round(float(value), 2)}
            for date, value in series.items()
        ],
        "backtest": [
            {"date": date.date().isoformat(), "actual": round(float(real), 2), "predicted": estimate}
            for (date, real), estimate in zip(actual.items(), backtests[chosen])
        ],
        "forecast": [
            {"date": date.date().isoformat(), "value": round(float(value), 2)}
            for date, value in zip(dates, forecast)
        ],
        "note": "Forecasts use historical daily patterns only. The score is mean absolute error on the last few observed days; it does not guarantee future accuracy.",
    }
