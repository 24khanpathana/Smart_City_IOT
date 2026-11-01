import hashlib

import pandas as pd
import streamlit as st

from config import REJECTS_CSV
from dashboard.components import data_access as da, ui
from ingestion.validate_data import ValidationError, validate_dataframe


def page():
    ui.header("Data Management", "Upload, validate, ingest and process data")
    ok, msg = da.db_status()
    if ok:
        st.success(f"Database: {msg}")
    else:
        st.info(f"Cassandra is unavailable; local sample analytics can still run. Database: {msg}")
        st.caption("Start Cassandra (`docker compose up -d`) and run `python -m ingestion.load_to_cassandra --init-schema`.")

    st.subheader("1 · CSV upload and validation")
    up = st.file_uploader("Sensor CSV (see data/sample_sensor_data.csv for the columns)", type="csv")
    if up is not None:
        try:
            raw = pd.read_csv(up, dtype=str)
            res = validate_dataframe(raw)
        except ValidationError as exc:
            st.error(str(exc))
            return
        except Exception:
            st.error("The file could not be read as CSV. Check the delimiter and encoding (UTF-8).")
            return
        s = res.summary
        ui.kpis([("Records processed", s["total"], None), ("Accepted", s["accepted"], None), ("Rejected", s["rejected"], None), ("Duplicates", s["duplicates"], None)])
        if s["by_reason"]:
            st.write("Rejection reasons:", s["by_reason"])
        file_id = hashlib.md5(up.getvalue()).hexdigest()
        # Ingestion runs ONLY on a button click; the same file cannot be submitted twice by a rerun.
        if st.button("Ingest accepted records into Cassandra", disabled=(not ok or res.valid.empty or st.session_state.get("done") == file_id)):
            from ingestion.load_to_cassandra import ingest_dataframe
            bar = st.progress(0.0)
            out = ingest_dataframe(raw, progress=lambda d, t: bar.progress(d / t))
            st.session_state["done"] = file_id
            st.success(f"Ingestion finished: {out['written']} writes, {out['failed']} failed, {out['seconds']} s. Re-ingesting the same data is safe (upserts).")
    if REJECTS_CSV.exists():
        st.download_button("Download last reject log", REJECTS_CSV.read_bytes(), "rejected_records.csv")

    st.subheader("2 · Spark analytics")
    running = da.pipeline_running()
    days = st.number_input("Look-back window (days)", 1, 31, 7)
    run_label = "Run Spark analytics" if ok else "Run local sample analytics"
    if st.button(run_label, disabled=running):
        st.info("Started in the background." if da.start_pipeline(int(days), local_csv=not ok)
                else "A run is already in progress.")
    if running:
        st.warning("Analytics are running. Refresh this page in a minute.")
    m = da.load_manifest()
    if m:
        st.caption(f"Last run: {m.get('generated_at', '?')} · window {m.get('start')} → {m.get('end')} · {m.get('rows_after_cleaning', '?')} readings · {m.get('total_seconds', '?')} s")
        st.dataframe(pd.DataFrame([{"job": k, "status": v["status"], "seconds": v.get("seconds")} for k, v in m["jobs"].items()]), hide_index=True)
    log = da.pipeline_log_tail()
    if log:
        with st.expander("Pipeline log (tail)"):
            st.code(log)


ui.safe(page)
