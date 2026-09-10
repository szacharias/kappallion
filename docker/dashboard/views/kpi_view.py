"""
kpi_view.py
Presentation component rendering real-time KPI metrics and active anomaly alerts.
"""

import pandas as pd
import streamlit as st

from domain.anomaly_rules import (
    CRITICAL_CARGO_TEMP_C,
    CRITICAL_VIBRATION_G,
    DOOR_OPEN_STATUS,
)


def render_fleet_kpis(silver_df: pd.DataFrame, gold_df: pd.DataFrame):
    """Render real-time fleet overview KPI metrics cards and toast notifications."""
    st.subheader("Real-Time Fleet Status")
    col1, col2, col3, col4, col5 = st.columns(5)

    if silver_df.empty:
        col1.metric("Active Vehicles", "0")
        col2.metric("Avg Cargo Temp", "N/A")
        col3.metric("Active Anomalies", "0")
        col4.metric("Condensation Risks", "0")
        col5.metric("Gold Windows", len(gold_df) if not gold_df.empty else 0)
        return

    latest_per_vehicle = silver_df.sort_values("Timestamp").groupby("Vehicle_ID").last().reset_index()
    total_vehicles = len(latest_per_vehicle)
    avg_cargo_temp = latest_per_vehicle["Cargo_Temp"].mean()

    anomalies_active = latest_per_vehicle[
        (latest_per_vehicle["Cargo_Temp"] > CRITICAL_CARGO_TEMP_C)
        | (latest_per_vehicle["Door_Status"] == DOOR_OPEN_STATUS)
        | (latest_per_vehicle["Vibration"] > CRITICAL_VIBRATION_G)
    ]
    num_anomalies = len(anomalies_active)

    condensation_active = latest_per_vehicle[latest_per_vehicle["Condensation_Risk"] == True]
    num_condensation = len(condensation_active)

    # Trigger popup toasts for active anomalies
    for _, alert in anomalies_active.iterrows():
        st.toast(
            f"🚨 **Anomaly Alert**: {alert['Vehicle_ID']} "
            f"(Cargo: {alert['Cargo_Temp']}°C, Door: {alert['Door_Status']}, Vib: {alert['Vibration']}G)",
            icon="⚠️",
        )

    col1.metric("Active Vehicles", f"{total_vehicles} Trucks")
    col2.metric(
        "Avg Cargo Temp",
        f"{avg_cargo_temp:.1f} °C",
        delta=f"{avg_cargo_temp - 3.5:.1f} °C vs Target",
        delta_color="inverse",
    )
    col3.metric("Active Anomalies", f"{num_anomalies}", delta="High Risk" if num_anomalies > 0 else "Normal")
    col4.metric("Condensation Risks", f"{num_condensation}", delta="Warning" if num_condensation > 0 else "Safe")
    col5.metric("Gold Windows (1m)", f"{len(gold_df):,}")
