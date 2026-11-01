import plotly.express as px
import streamlit as st

from dashboard.components import ui


def page():
    ui.header("Traffic Analytics", "Vehicle volumes, peak periods and (speed-based) congestion estimates")
    r = ui.need_results("traffic_hourly", "traffic_peaks", "traffic_congestion", "traffic_surges", "locations")
    if r is None:
        return
    start, end, locs = ui.filters(ui.location_list())
    h = ui.subset(r["traffic_hourly"], start, end, locs)
    if h.empty:
        st.warning("No traffic readings for the selected filters.")
        return
    by_loc = h.groupby("location_id", as_index=False).agg(total_vehicles=("total_vehicles", "sum"))
    by_loc = by_loc.merge(ui.wavg(h, "avg_speed", "speed_n", ["location_id"]), on="location_id", how="left")
    by_loc["rank"] = by_loc["total_vehicles"].rank(ascending=False).astype(int)
    prof = ui.wavg(h, "avg_vehicles", "readings", ["hour"])
    peak_hour = int(prof.loc[prof["avg_vehicles"].idxmax(), "hour"])
    speed = ui.overall(h, "avg_speed", "speed_n")
    top = by_loc.sort_values("total_vehicles", ascending=False).iloc[0]
    ui.kpis([("Total vehicles", f"{int(by_loc.total_vehicles.sum()):,}", None), ("City peak hour", f"{peak_hour:02d}:00", None),
             ("Busiest location", top["location_id"], "Highest total volume"),
             ("Avg speed", f"{speed:.1f} km/h" if speed == speed else "n/a", None)])
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Hourly traffic pattern")
        p = ui.wavg(h, "avg_vehicles", "readings", ["location_id", "hour"])
        st.plotly_chart(px.line(p, x="hour", y="avg_vehicles", color="location_id", markers=True,
                                color_discrete_sequence=ui.PALETTE, labels={"avg_vehicles": "avg vehicles / reading"}), use_container_width=True)
    with c2:
        st.subheader("Traffic volume by location (ranked)")
        st.plotly_chart(px.bar(by_loc.sort_values("total_vehicles"), x="total_vehicles", y="location_id", orientation="h",
                               color_discrete_sequence=ui.PALETTE), use_container_width=True)
    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Heatmap: hour × location")
        piv = p.pivot(index="location_id", columns="hour", values="avg_vehicles")
        st.plotly_chart(px.imshow(piv, aspect="auto", color_continuous_scale="Blues", labels=dict(color="avg vehicles")), use_container_width=True)
    with c4:
        st.subheader("Peak-hour comparison (top 3 hours per location)")
        pk = r["traffic_peaks"]; pk = pk[pk["location_id"].isin(locs)]
        pk = pk.assign(peak_hour=pk["peak_hour"].astype(str).str.zfill(2) + ":00")
        st.plotly_chart(px.bar(pk, x="location_id", y="avg_vehicles", color="peak_hour", barmode="group",
                               color_discrete_sequence=ui.PALETTE), use_container_width=True)
    st.subheader("Volume ≠ congestion")
    st.caption("A location is flagged *likely congested* only when volume is high AND speed falls below 60 % of its "
               "free-flow speed. High volume with normal speed is shown as *high volume, flowing*.")
    cg = ui.subset(r["traffic_congestion"], start, end, locs)
    if not cg.empty:
        share = cg.assign(c=cg["assessment"] == "likely congested").groupby("location_id", as_index=False)["c"].mean()
        share["share_of_hours_likely_congested_%"] = (share.pop("c") * 100).round(1)
        st.dataframe(share, use_container_width=True, hide_index=True)
    with st.expander("Unusual hour-over-hour increases"):
        sg = r["traffic_surges"]; sg = sg[sg["location_id"].isin(locs)]
        st.dataframe(sg.round(1), use_container_width=True, hide_index=True) if not sg.empty else st.write("None detected.")


ui.safe(page)
