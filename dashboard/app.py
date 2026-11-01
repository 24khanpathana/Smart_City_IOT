"""Smart City IoT Analytics - Streamlit entry point.   Run:  streamlit run dashboard/app.py"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st  # noqa: E402

st.set_page_config(page_title="Smart City IoT Analytics", page_icon="🏙️", layout="wide")

# st.navigation replaces automatic discovery of the pages/ folder
nav = st.navigation({
    "Analytics": [
        st.Page("pages/overview.py", title="City Overview", icon="🏙️", default=True),
        st.Page("pages/traffic.py", title="Traffic", icon="🚗"),
        st.Page("pages/environment.py", title="Environment", icon="🌡️"),
        st.Page("pages/parking.py", title="Parking", icon="🅿️"),
        st.Page("pages/waste.py", title="Waste Management", icon="🗑️"),
    ],
    "Operations": [
        st.Page("pages/sensor_health.py", title="Sensor Health", icon="📡"),
        st.Page("pages/anomalies.py", title="Anomaly Monitoring", icon="⚠️"),
        st.Page("pages/data_management.py", title="Data Management", icon="🗄️"),
    ],
})
st.sidebar.caption("Synthetic demo data unless you upload your own CSV.")
nav.run()
