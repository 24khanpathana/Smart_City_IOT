import plotly.express as px
import streamlit as st

from dashboard.components import ui


def page():
    ui.header("Environmental Analytics", "Temperature, humidity and noise by location")
    r = ui.need_results("environment_hourly", "environment_humidity_dist", "environment_high_readings")
    if r is None:
        return
    start, end, locs = ui.filters(ui.location_list())
    h = ui.subset(r["environment_hourly"], start, end, locs)
    if h.empty:
        st.warning("No environmental readings for the selected filters.")
        return
    ui.kpis([("Avg temperature", f"{ui.overall(h, 'avg_temperature', 'readings'):.1f} °C", None),
             ("Max temperature", f"{h.max_temperature.max():.1f} °C", None), ("Min temperature", f"{h.min_temperature.min():.1f} °C", None),
             ("Avg noise", f"{ui.overall(h, 'avg_noise', 'readings'):.1f} dB", None)])
    st.subheader("Temperature trend")
    st.plotly_chart(px.line(h.sort_values("hour_start"), x="hour_start", y="avg_temperature", color="location_id",
                            color_discrete_sequence=ui.PALETTE), use_container_width=True)
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Humidity distribution")
        hd = r["environment_humidity_dist"]; hd = ui.subset(hd, start, end, locs)
        hd = hd.groupby("humidity_bin", as_index=False)["count"].sum()
        st.plotly_chart(px.bar(hd, x="humidity_bin", y="count", labels={"humidity_bin": "relative humidity % (10-pt bins)"},
                               color_discrete_sequence=ui.PALETTE), use_container_width=True)
    with c2:
        st.subheader("Noise level by location")
        n = ui.wavg(h, "avg_noise", "readings", ["location_id"])
        st.plotly_chart(px.bar(n, x="location_id", y="avg_noise", color_discrete_sequence=ui.PALETTE, labels={"avg_noise": "avg dB"}), use_container_width=True)
    st.subheader("By time of day")
    t = ui.wavg(h, "avg_noise", "readings", ["hour"]).merge(ui.wavg(h, "avg_temperature", "readings", ["hour"]), on="hour")
    st.plotly_chart(px.line(t.melt("hour"), x="hour", y="value", color="variable", color_discrete_sequence=ui.PALETTE), use_container_width=True)
    st.subheader("Location summary")
    s = h.groupby("location_id").agg(avg_temp=("avg_temperature", "mean"), max_temp=("max_temperature", "max"),
                                     min_temp=("min_temperature", "min"), avg_humidity=("avg_humidity", "mean"),
                                     avg_noise=("avg_noise", "mean"), max_noise=("max_noise", "max")).round(1)
    st.dataframe(s, use_container_width=True)
    with st.expander("Unusually high readings (above the location's own 95th percentile)"):
        st.caption("Statistical outliers only — NOT regulatory or health-limit violations.")
        hr = ui.subset(r["environment_high_readings"], start, end, locs)
        st.dataframe(hr.head(200), use_container_width=True, hide_index=True)


ui.safe(page)
