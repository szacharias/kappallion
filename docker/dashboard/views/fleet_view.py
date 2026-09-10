"""
fleet_view.py
Presentation component rendering the Fleet Manager controls in the sidebar.
"""

import streamlit as st
from services.fleet_manager_srv import get_active_fleet_size, scale_active_fleet


def render_fleet_manager():
    """Render sidebar expander for live fleet scaling."""
    with st.sidebar.expander("🚛 Fleet Manager (Add Trucks)", expanded=True):
        current_size = get_active_fleet_size()
        st.markdown(f"**Active Fleet Target:** `{current_size} Trucks`")

        f_col1, f_col2 = st.columns(2)
        with f_col1:
            if st.button("➕ Add 1 Truck", key="btn_add_1", use_container_width=True):
                new_size = scale_active_fleet(1)
                st.success(f"Added truck! Fleet is now {new_size}.")
                st.rerun()

        with f_col2:
            if st.button("➕ Add 3 Trucks", key="btn_add_3", use_container_width=True):
                new_size = scale_active_fleet(3)
                st.success(f"Added 3 trucks! Fleet is now {new_size}.")
                st.rerun()

        st.caption("Simulator reads pipeline.conf dynamically and injects new vehicles onto designated routes.")
