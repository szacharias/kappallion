"""
map_view.py
Geospatial visualization component rendering live Pydeck WebGL fleet routes.
Adheres strictly to GEMINI.md guidelines (pure presentation, no inline magic numbers).
"""

from typing import Dict, List, Tuple
import pandas as pd
import pydeck as pdk
import streamlit as st

from domain.anomaly_rules import (
    format_anomaly_description,
    is_anomaly,
)

# Visual styling constants
DEFAULT_TRAIL_LENGTH: int = 15
DEFAULT_MAP_ZOOM: float = 9.2
NORMAL_POINT_RADIUS_M: int = 180
NORMAL_POINT_MIN_PX: int = 3
NORMAL_POINT_MAX_PX: int = 7

ANOMALY_RADIUS_M: int = 350
ANOMALY_MIN_PX: int = 10
ANOMALY_MAX_PX: int = 18
ANOMALY_HALO_MIN_PX: int = 15
ANOMALY_HALO_MAX_PX: int = 25

COLOR_ANOMALY_FILL: List[int] = [255, 38, 38, 240]      # Vivid alert crimson
COLOR_ANOMALY_BORDER: List[int] = [255, 230, 0, 255]    # Bright hazard yellow
COLOR_ANOMALY_HALO: List[int] = [255, 59, 48, 160]      # Danger pulse halo

SHAPE_CONFIGS: Dict[str, Tuple[int, int]] = {
    "Warning Triangle (▲)": (3, 30),   # disk_resolution=3, angle=30 (upright triangle)
    "Hazard Diamond (◆)": (4, 45),     # disk_resolution=4, angle=45 (diamond rhombus)
    "Alert Hexagon (⬡)": (6, 0),       # disk_resolution=6, angle=0 (hazard hexagon)
}

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


def get_truck_color(v_id: str) -> List[int]:
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


def enrich_map_telemetry(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich map coordinates with anomaly flags, status badges, and descriptions."""
    df = df.copy()
    df["is_anomaly"] = df.apply(
        lambda r: is_anomaly(
            float(r["Cargo_Temp"]),
            str(r["Door_Status"]),
            float(r["Vibration"]),
            bool(r.get("Condensation_Risk", False)),
        ),
        axis=1,
    )
    df["anomaly_desc"] = df.apply(
        lambda r: format_anomaly_description(
            float(r["Cargo_Temp"]),
            str(r["Door_Status"]),
            float(r["Vibration"]),
            bool(r.get("Condensation_Risk", False)),
        ),
        axis=1,
    )
    df["status_badge"] = df["is_anomaly"].apply(
        lambda a: '<span style="color: #ff4d4f; font-weight: bold;">🚨 ANOMALY</span>'
        if a
        else '<span style="color: #52c41a; font-weight: bold;">✔ NORMAL</span>'
    )
    return df


def render_geospatial_fleet_map(silver_df: pd.DataFrame):
    """Render Pydeck WebGL map with vehicle routes, distinct anomaly shapes, and legend."""
    st.markdown("---")
    st.subheader("Live Fleet Tracking & Transit Routes (Chicagoland)")

    if silver_df.empty:
        st.info("No data received in Silver table yet. Ensure simulator and Spark streaming are running.")
        return

    available_trucks = sorted(silver_df["Vehicle_ID"].dropna().unique().tolist())
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        selected_trucks = st.multiselect(
            "🚛 Vehicles to Display on Map:",
            options=available_trucks,
            default=available_trucks[:6] if len(available_trucks) > 6 else available_trucks,
        )
    with col2:
        trail_length = st.slider("Route Trail Length", min_value=5, max_value=50, value=DEFAULT_TRAIL_LENGTH)
    with col3:
        shape_choice = st.selectbox(
            "⚠️ Anomaly Shape:",
            list(SHAPE_CONFIGS.keys()),
            index=0,
            help="Distinct geometry used to instantly identify cold-chain anomalies on the map.",
        )

    disk_res, angle = SHAPE_CONFIGS[shape_choice]

    # Render vehicle color badges
    if selected_trucks:
        legend_badges = "".join([
            f'<span style="background-color: {get_truck_hex(v)}; color: #000; font-weight: bold; '
            f'padding: 3px 8px; border-radius: 4px; margin-right: 8px; font-size: 13px;">{v}</span>'
            for v in selected_trucks
        ])
        st.markdown(f"**Vehicle Colors:** {legend_badges}", unsafe_allow_html=True)

    # Visual shape indicator legend
    st.markdown(
        f'<div style="margin-top: 4px; margin-bottom: 12px; display: flex; gap: 10px; flex-wrap: wrap;">'
        f'<span style="background-color: #2d3748; color: #e2e8f0; padding: 4px 10px; border-radius: 4px; font-size: 13px;">'
        f'● <b>Normal Breadcrumbs:</b> Circular Dots (Vehicle Colors)</span>'
        f'<span style="background-color: #742a2a; color: #fff5f5; border: 1.5px solid #ecc94b; padding: 4px 10px; border-radius: 4px; font-size: 13px;">'
        f'▲ <b>Active Anomaly:</b> {shape_choice.split()[0]} {shape_choice.split()[1]} (Vivid Red + Yellow Border + Halo)</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    map_df = silver_df.sort_values(["Vehicle_ID", "Timestamp"], ascending=[True, False])
    if selected_trucks:
        map_df = map_df[map_df["Vehicle_ID"].isin(selected_trucks)]

    map_df = map_df.groupby("Vehicle_ID").head(trail_length).reset_index(drop=True)
    required_cols = [
        "Latitude", "Longitude", "Vehicle_ID", "Cargo_Temp",
        "Door_Status", "Vibration", "Condensation_Risk", "Timestamp"
    ]
    map_df = map_df[[c for c in required_cols if c in map_df.columns]].dropna()

    if map_df.empty:
        st.info("No coordinates available for the selected vehicle(s).")
        return

    map_df = map_df.rename(columns={"Latitude": "lat", "Longitude": "lon"})
    map_df["Timestamp"] = map_df["Timestamp"].astype(str)
    map_df["color"] = map_df["Vehicle_ID"].apply(get_truck_color)
    map_df = enrich_map_telemetry(map_df)

    normal_df = map_df[~map_df["is_anomaly"]]
    anomaly_df = map_df[map_df["is_anomaly"]]

    layers = []

    # 1. Normal points layer (circular dots)
    if not normal_df.empty:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=normal_df,
                get_position=["lon", "lat"],
                get_color="color",
                get_radius=NORMAL_POINT_RADIUS_M,
                radius_min_pixels=NORMAL_POINT_MIN_PX,
                radius_max_pixels=NORMAL_POINT_MAX_PX,
                pickable=True,
                auto_highlight=True,
            )
        )

    # 2. Anomaly points layer (distinct geometry: Triangle / Diamond / Hexagon)
    if not anomaly_df.empty:
        # Pulsing warning outer halo ring
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=anomaly_df,
                get_position=["lon", "lat"],
                get_line_color=COLOR_ANOMALY_HALO,
                line_width_min_pixels=2,
                stroked=True,
                filled=False,
                radius_min_pixels=ANOMALY_HALO_MIN_PX,
                radius_max_pixels=ANOMALY_HALO_MAX_PX,
                pickable=False,
            )
        )
        # Prominent warning polygon (ColumnLayer with disk_resolution=3 for triangle)
        layers.append(
            pdk.Layer(
                "ColumnLayer",
                data=anomaly_df,
                get_position=["lon", "lat"],
                disk_resolution=disk_res,
                angle=angle,
                radius=ANOMALY_RADIUS_M,
                radius_min_pixels=ANOMALY_MIN_PX,
                radius_max_pixels=ANOMALY_MAX_PX,
                get_fill_color=COLOR_ANOMALY_FILL,
                get_line_color=COLOR_ANOMALY_BORDER,
                stroked=True,
                line_width_min_pixels=2,
                pickable=True,
                auto_highlight=True,
            )
        )

    view_state = pdk.ViewState(
        latitude=float(map_df["lat"].mean()),
        longitude=float(map_df["lon"].mean()),
        zoom=DEFAULT_MAP_ZOOM,
        pitch=0,
    )

    st.pydeck_chart(
        pdk.Deck(
            layers=layers,
            initial_view_state=view_state,
            tooltip={
                "html": "<b>Truck: {Vehicle_ID}</b> {status_badge}<br/>"
                        "<b>Time:</b> {Timestamp}<br/>"
                        "<b>Cargo Temp:</b> {Cargo_Temp}°C<br/>"
                        "<b>Door:</b> {Door_Status}<br/>"
                        "<b>Vibration:</b> {Vibration}G<br/>"
                        "<b>Status:</b> {anomaly_desc}",
                "style": {"backgroundColor": "#1a202c", "color": "white", "fontSize": "12px", "padding": "8px"},
            },
            map_style="dark",
        ),
        use_container_width=True,
    )

