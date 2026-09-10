"""
map_view.py
Geospatial visualization component rendering live Pydeck WebGL fleet routes.
"""

import pandas as pd
import pydeck as pdk
import streamlit as st

VEHICLE_PALETTE = [
    {"name": "Cyan", "rgba": [0, 210, 211, 230], "hex": "#00d2d3"},
    {"name": "Coral", "rgba": [255, 107, 107, 230], "hex": "#ff6b6b"},
    {"name": "Emerald", "rgba": [29, 209, 161, 230], "hex": "#1dd1a1"},
    {"name": "Purple", "rgba": [95, 39, 205, 230], "hex": "#5f27cd"},
    {"name": "Amber", "rgba": [255, 159, 67, 230], "hex": "#ff9f43"},
    {"name": "Sky Blue", "rgba": [72, 219, 251, 230], "hex": "#48dbfb"},
    {"name": "Magenta", "rgba": [255, 159, 243, 230], "hex": "#ff9ff3"},
    {"name": "Teal", "rgba": [16, 172, 132, 230], "hex": "#10ac84"},
    {"name": "Tangerine", "rgba": [250, 130, 49, 230], "hex": "#fa8231"},
    {"name": "Lime", "rgba": [38, 222, 129, 230], "hex": "#26de81"},
]


def get_truck_color(v_id: str) -> list:
    """Return distinct RGBA color for vehicle ID."""
    try:
        num = int(v_id.split("-")[-1]) - 1
    except Exception:
        num = hash(v_id)
    return VEHICLE_PALETTE[num % len(VEHICLE_PALETTE)]["rgba"]


def get_truck_hex(v_id: str) -> str:
    """Return distinct Hex color for vehicle ID."""
    try:
        num = int(v_id.split("-")[-1]) - 1
    except Exception:
        num = hash(v_id)
    return VEHICLE_PALETTE[num % len(VEHICLE_PALETTE)]["hex"]


def render_geospatial_fleet_map(silver_df: pd.DataFrame):
    """Render Pydeck WebGL map with vehicle routes, color badges, and filter controls."""
    st.markdown("---")
    st.subheader("Live Fleet Tracking & Transit Routes (Chicagoland)")

    if silver_df.empty:
        st.info("No data received in Silver table yet. Ensure simulator and Spark streaming are running.")
        return

    available_trucks = sorted(silver_df["Vehicle_ID"].dropna().unique().tolist())
    col1, col2 = st.columns([3, 1])
    with col1:
        selected_trucks = st.multiselect(
            "🚛 Vehicles to Display on Map:",
            options=available_trucks,
            default=available_trucks[:6] if len(available_trucks) > 6 else available_trucks,
        )
    with col2:
        trail_length = st.slider("Route Trail Length", min_value=5, max_value=50, value=15)

    if selected_trucks:
        legend_badges = "".join([
            f'<span style="background-color: {get_truck_hex(v)}; color: #000; font-weight: bold; '
            f'padding: 3px 8px; border-radius: 4px; margin-right: 8px; font-size: 13px;">{v}</span>'
            for v in selected_trucks
        ])
        st.markdown(f"**Vehicle Colors:** {legend_badges}", unsafe_allow_html=True)

    map_df = silver_df.sort_values(["Vehicle_ID", "Timestamp"], ascending=[True, False])
    if selected_trucks:
        map_df = map_df[map_df["Vehicle_ID"].isin(selected_trucks)]

    map_df = map_df.groupby("Vehicle_ID").head(trail_length).reset_index(drop=True)
    map_df = map_df[["Latitude", "Longitude", "Vehicle_ID", "Cargo_Temp", "Door_Status", "Vibration", "Condensation_Risk", "Timestamp"]].dropna()

    if not map_df.empty:
        map_df = map_df.rename(columns={"Latitude": "lat", "Longitude": "lon"})
        map_df["Timestamp"] = map_df["Timestamp"].astype(str)
        map_df["color"] = map_df["Vehicle_ID"].apply(get_truck_color)

        point_layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_df,
            get_position=["lon", "lat"],
            get_color="color",
            get_radius=200,
            radius_min_pixels=3,
            radius_max_pixels=7,
            pickable=True,
            auto_highlight=True,
        )

        view_state = pdk.ViewState(
            latitude=float(map_df["lat"].mean()),
            longitude=float(map_df["lon"].mean()),
            zoom=9.2,
            pitch=0,
        )

        st.pydeck_chart(
            pdk.Deck(
                layers=[point_layer],
                initial_view_state=view_state,
                tooltip={
                    "html": "<b>Truck: {Vehicle_ID}</b><br/>"
                            "<b>Time:</b> {Timestamp}<br/>"
                            "<b>Cargo Temp:</b> {Cargo_Temp}°C<br/>"
                            "<b>Door:</b> {Door_Status}<br/>"
                            "<b>Vibration:</b> {Vibration}G",
                    "style": {"backgroundColor": "#1a202c", "color": "white", "fontSize": "12px"},
                },
                map_style="dark",
            ),
            use_container_width=True,
        )
    else:
        st.info("No coordinates available for the selected vehicle(s).")
