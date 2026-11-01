import plotly.express as px
import streamlit as st

from dashboard.components import data_access as da, ui


def page():
    ui.header("Anomaly Monitoring", "Potential anomalies = readings with |Z| ≥ threshold versus the sensor's own baseline. They need review; they are not confirmed incidents.")
    m = da.load_manifest()
    if m is None:
        ui.empty_state("anomaly results")
        return
    start, end, locs = ui.filters(ui.location_list())
    df = da.fetch_anomalies(start, end)
    if df.empty:
        st.success("No potential anomalies stored for this date range.")
        return
    types = st.sidebar.multiselect("Sensor type", sorted(df.sensor_type.unique()), default=sorted(df.sensor_type.unique()))
    stat = st.sidebar.multiselect("Status", ["new", "reviewed", "dismissed"], default=["new"])
    df = df[df.location_id.isin(locs) & df.sensor_type.isin(types) & df.status.isin(stat)]
    ui.kpis([("Shown", len(df), None), ("Critical", int((df.severity == "critical").sum()), "|Z| ≥ 5"),
             ("High", int((df.severity == "high").sum()), "4 ≤ |Z| < 5"), ("Medium", int((df.severity == "medium").sum()), "3 ≤ |Z| < 4")])
    if df.empty:
        st.info("No anomalies match the filters.")
        return
    st.plotly_chart(px.scatter(df, x="ts", y="z_score", color="severity", symbol="sensor_type", hover_data=["sensor_id", "metric", "observed_value", "expected_range"],
                               color_discrete_map={"medium": "#f59e0b", "high": "#f97316", "critical": "#ef4444"}), use_container_width=True)
    view = df.assign(select=False)[["select", "ts", "sensor_id", "location_id", "sensor_type", "metric", "observed_value", "expected_range", "z_score", "severity", "status", "day", "anomaly_id"]]
    edited = st.data_editor(view.head(500), hide_index=True, use_container_width=True, disabled=[c for c in view.columns if c != "select"],
                            column_config={"anomaly_id": None, "day": None})
    chosen = edited[edited["select"]]
    c1, c2, _ = st.columns([1, 1, 4])
    for label, new_status, col in (("Mark reviewed", "reviewed", c1), ("Dismiss", "dismissed", c2)):
        if col.button(label, disabled=chosen.empty):
            n = da.update_anomaly_status(chosen, new_status)
            st.success(f"{n} anomalies set to '{new_status}'.")
            st.rerun()


ui.safe(page)
