import plotly.express as px
import streamlit as st

from dashboard.components import data_access as da, ui


def page():
    ui.header("City Overview", "Sensor fleet, activity and potential anomalies at a glance")
    r = ui.need_results("sensor_health", "locations")
    if r is None:
        return
    h, loc = r["sensor_health"], r["locations"]
    start, end, locs = ui.filters(ui.location_list())
    h = h[h["location_id"].isin(locs)]
    cnt = h["status"].value_counts()
    anomalies = 0
    try:
        anomalies = len(da.fetch_anomalies(start, end))
    except ConnectionError:
        pass
    ui.kpis([("Registered sensors", len(h), None), ("Active", int(cnt.get("active", 0)), "Reported within the active timeout"),
             ("Delayed", int(cnt.get("delayed", 0)), "Late, but not necessarily faulty"),
             ("Locations", h["location_id"].nunique(), None), ("Potential anomalies", anomalies, "Statistical outliers, not confirmed incidents")])
    c1, c2 = st.columns([3, 2])
    with c1:
        st.subheader("Sensor map")
        m = loc[loc["location_id"].isin(locs)].dropna(subset=["latitude", "longitude"])
        if m.empty:
            st.info("No valid coordinates available for the map.")
        else:
            fig = px.scatter_map(m, lat="latitude", lon="longitude", size="sensors", hover_name="location_id",
                                 zoom=11, height=380, color_discrete_sequence=ui.PALETTE)
            fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.subheader("Sensors by status")
        fig = px.pie(h, names="status", hole=0.55, color_discrete_sequence=ui.PALETTE)
        fig.update_layout(height=380, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)
    st.subheader("Latest reading per sensor")
    st.dataframe(h.sort_values("last_seen", ascending=False)[["sensor_id", "location_id", "sensor_type", "last_seen", "status"]],
                 use_container_width=True, hide_index=True, height=260)
    st.subheader("Location comparison")
    st.dataframe(h.groupby("location_id").agg(sensors=("sensor_id", "count"), active=("status", lambda s: (s == "active").sum()),
                                              missing_fraction=("missing_fraction", "mean")).round(3),
                 use_container_width=True)


ui.safe(page)
