import plotly.express as px
import streamlit as st

from dashboard.components import ui


def page():
    ui.header("Sensor Health", "Active ≤ timeout · Delayed ≤ inactive timeout · Inactive beyond · Unknown = never seen. Silence is not proof of a fault.")
    r = ui.need_results("sensor_health")
    if r is None:
        return
    h = r["sensor_health"]
    locs = st.sidebar.multiselect("Locations", ui.location_list(), default=ui.location_list())
    status = st.sidebar.multiselect("Status", ["active", "delayed", "inactive", "unknown"], default=["active", "delayed", "inactive", "unknown"])
    h = h[h.location_id.isin(locs) & h.status.isin(status)]
    c = h.status.value_counts()
    ui.kpis([("Sensors", len(h), None), ("Active", int(c.get("active", 0)), None), ("Delayed", int(c.get("delayed", 0)), None),
             ("Inactive", int(c.get("inactive", 0)), None), ("Invalid records (rejected)", int(h.invalid_records.sum()), "From the ingestion reject log")])
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Minutes since last reading")
        st.plotly_chart(px.bar(h.sort_values("age_minutes"), x="sensor_id", y="age_minutes", color="status",
                               color_discrete_map={"active": "#10b981", "delayed": "#f59e0b", "inactive": "#ef4444", "unknown": "#64748b"}), use_container_width=True)
    with c2:
        st.subheader("Missing-value rate by sensor")
        st.plotly_chart(px.bar(h.sort_values("missing_fraction"), x="sensor_id", y="missing_fraction", color_discrete_sequence=ui.PALETTE), use_container_width=True)
    st.subheader("Data-quality summary")
    st.dataframe(h[["sensor_id", "location_id", "sensor_type", "last_seen", "age_minutes", "status", "readings_in_window", "missing_fraction", "invalid_records"]].round(3),
                 use_container_width=True, hide_index=True)


ui.safe(page)
