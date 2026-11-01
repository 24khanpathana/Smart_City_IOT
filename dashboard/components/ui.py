"""Shared UI helpers: styling, KPI cards, filters, safe rendering."""
from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd
import streamlit as st

from dashboard.components import data_access as da

log = logging.getLogger("dashboard")
PALETTE = ["#2563eb", "#0ea5e9", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#14b8a6", "#64748b"]

_CSS = """
<style>
.block-container {padding-top: 1.6rem; max-width: 1250px;}
[data-testid="stMetric"] {background: var(--secondary-background-color); border: 1px solid rgba(128,128,128,.25);
  border-radius: 12px; padding: 14px 16px;}
h1, h2, h3 {letter-spacing: -0.01em;}
.sc-sub {opacity:.7; margin-top:-0.6rem; margin-bottom:1rem;}
</style>"""


def header(title: str, subtitle: str = "") -> None:
    st.markdown(_CSS, unsafe_allow_html=True)
    st.title(title)
    if subtitle:
        st.markdown(f"<div class='sc-sub'>{subtitle}</div>", unsafe_allow_html=True)


def kpis(items: list[tuple[str, object, str | None]]) -> None:
    cols = st.columns(len(items))
    for col, (label, value, help_) in zip(cols, items):
        col.metric(label, value, help=help_)


def empty_state(what: str) -> None:
    st.info(f"No {what} available yet. Open **Data Management** and run analytics. "
            "If Cassandra is unavailable, choose **Run local sample analytics** to use the included CSV.")


def need_results(*names: str) -> dict[str, pd.DataFrame] | None:
    """Load result tables; show a friendly message if the pipeline has not produced them."""
    out = {n: da.load_result(n) for n in names}
    if da.load_manifest() is None or all(df.empty for df in out.values()):
        empty_state("analytics results")
        return None
    return out


def filters(locations: list[str]) -> tuple[date, date, list[str]]:
    """Sidebar date-range + location filters shared by all analytic pages."""
    m = da.load_manifest() or {}
    lo = date.fromisoformat(m["start"]) if m.get("start") else date.today()
    hi = date.fromisoformat(m["end"]) if m.get("end") else date.today()
    st.sidebar.markdown("### Filters")
    rng = st.sidebar.date_input("Date range", (lo, hi), min_value=lo, max_value=hi)
    start, end = (rng[0], rng[1]) if isinstance(rng, tuple) and len(rng) == 2 else (lo, hi)
    locs = st.sidebar.multiselect("Locations", locations, default=locations)
    return start, end, locs


def subset(df: pd.DataFrame, start: date, end: date, locs: list[str], day_col: str = "day") -> pd.DataFrame:
    if df.empty:
        return df
    out = df
    if day_col in out.columns:
        out = out[(out[day_col] >= start) & (out[day_col] <= end)]
    if "location_id" in out.columns:
        out = out[out["location_id"].isin(locs)]
    return out


def wavg(df: pd.DataFrame, value: str, weight: str, by: list[str]) -> pd.DataFrame:
    """Weighted average of an already-aggregated metric (weights = number of readings)."""
    d = df.dropna(subset=[value]).copy()
    d["_w"] = d[weight].astype(float)
    d["_v"] = d[value] * d["_w"]
    g = d.groupby(by, as_index=False)[["_v", "_w"]].sum()
    g[value] = np.where(g["_w"] > 0, g["_v"] / g["_w"], np.nan)
    return g.drop(columns=["_v", "_w"])


def overall(df: pd.DataFrame, value: str, weight: str) -> float:
    """Single weighted average, NaN if there is nothing to average."""
    g = wavg(df.assign(_k=1), value, weight, ["_k"])
    return float(g[value].iloc[0]) if not g.empty else float("nan")


def safe(fn):
    """Run a page; show an actionable message instead of a stack trace."""
    try:
        fn()
    except ConnectionError as exc:
        st.error(f"Cassandra is not reachable. {exc}")
    except Exception:
        log.exception("Page failed")
        st.error("Something went wrong while building this page. Check the terminal log, then re-run the "
                 "analytics from Data Management. If the problem persists, the result files may be corrupt.")


def location_list() -> list[str]:
    h = da.load_result("sensor_health")
    return sorted(h["location_id"].dropna().unique().tolist()) if not h.empty else []
