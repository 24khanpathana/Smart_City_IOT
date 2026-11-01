import plotly.express as px
import streamlit as st

from dashboard.components import ui


def page():
    ui.header("Parking Analytics", "Occupancy = occupied spaces ÷ capacity × 100 (readings with invalid capacity are excluded)")
    r = ui.need_results("parking_hourly", "parking_latest", "parking_quality")
    if r is None:
        return
    start, end, locs = ui.filters(ui.location_list())
    h = ui.subset(r["parking_hourly"], start, end, locs)
    if h.empty:
        st.warning("No parking readings for the selected filters.")
        return
    thr = (ui.da.load_manifest() or {}).get("thresholds", {}).get("parking_high_demand", 80)
    by = ui.wavg(h, "avg_occupancy", "readings", ["location_id"]).merge(ui.wavg(h, "avg_available", "readings", ["location_id"]), on="location_id")
    q = r["parking_quality"]
    ui.kpis([("Avg occupancy", f"{ui.overall(h, 'avg_occupancy', 'readings'):.1f} %", None),
             ("Avg free spaces", f"{by.avg_available.mean():.0f}", "Mean across locations"),
             ("High-demand locations", int((by.avg_occupancy >= thr).sum()), f"Average occupancy ≥ {thr:g} % (configurable)"),
             ("Excluded readings", int(q["excluded_invalid_or_missing"].iloc[0]) if not q.empty else 0, "Missing/invalid capacity or occupancy")])
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Occupancy by location")
        st.plotly_chart(px.bar(by.sort_values("avg_occupancy"), x="avg_occupancy", y="location_id", orientation="h",
                               color_discrete_sequence=ui.PALETTE), use_container_width=True)
    with c2:
        st.subheader("Demand by hour")
        d = ui.wavg(h, "avg_occupancy", "readings", ["location_id", "hour"])
        st.plotly_chart(px.line(d, x="hour", y="avg_occupancy", color="location_id", color_discrete_sequence=ui.PALETTE), use_container_width=True)
    st.subheader("Current availability (latest reading per lot)")
    lt = r["parking_latest"]; lt = lt[lt["location_id"].isin(locs)]
    st.dataframe(lt[["sensor_id", "location_id", "ts", "parking_capacity", "occupied_spaces", "available_spaces", "parking_occupancy"]].round(1),
                 use_container_width=True, hide_index=True)


ui.safe(page)
