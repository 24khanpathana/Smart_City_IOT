import plotly.express as px
import streamlit as st

from dashboard.components import ui


def page():
    ui.header("Smart Waste Management", "Thresholds are operational settings (WASTE_WARNING_PCT / WASTE_CRITICAL_PCT), not universal standards")
    r = ui.need_results("waste_latest", "waste_hourly", "waste_by_location")
    if r is None:
        return
    start, end, locs = ui.filters(ui.location_list())
    lt = r["waste_latest"]; lt = lt[lt["location_id"].isin(locs)]
    th = (ui.da.load_manifest() or {}).get("thresholds", {})
    ui.kpis([("Bins monitored", len(lt), None), ("Warning", int((lt.attention == "warning").sum()), f"≥ {th.get('waste_warning', 80):g} %"),
             ("Critical", int((lt.attention == "critical").sum()), f"≥ {th.get('waste_critical', 90):g} %"),
             ("Avg fill (latest)", f"{lt.fill_pct.mean():.1f} %" if len(lt) else "n/a", None)])
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Latest fill level per bin")
        st.plotly_chart(px.bar(lt.sort_values("fill_pct"), x="fill_pct", y="sensor_id", orientation="h", color="attention",
                               color_discrete_map={"ok": "#10b981", "warning": "#f59e0b", "critical": "#ef4444"}), use_container_width=True)
    with c2:
        st.subheader("Fill-level trend")
        h = ui.subset(r["waste_hourly"], start, end, locs)
        st.plotly_chart(px.line(h.sort_values("hour_start"), x="hour_start", y="avg_fill", color="sensor_id",
                                color_discrete_sequence=ui.PALETTE), use_container_width=True)
    st.subheader("Bins requiring inspection or collection")
    need = lt[lt.attention != "ok"].sort_values("fill_pct", ascending=False)
    st.dataframe(need[["sensor_id", "location_id", "ts", "fill_pct", "attention"]], use_container_width=True, hide_index=True) if len(need) else st.success("No bin is above the warning threshold.")
    st.caption("Time-until-full forecasting is intentionally not provided: emptying events are irregular and the history is short, "
               "so an estimate would not be reliable.")


ui.safe(page)
