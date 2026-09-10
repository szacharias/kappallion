"""
styles.py
Custom styling and dark theme injection for Streamlit presentation layer.
"""

import streamlit as st


def inject_custom_styles():
    """Inject polished dark mode styles and typography."""
    st.markdown(
        """
        <style>
            .main {
                background-color: #0f1116;
                color: #f0f2f6;
            }
            .stCard {
                background-color: #1b202c;
                border-radius: 8px;
                padding: 15px;
                border: 1px solid #2d3748;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }
            h1, h2, h3 {
                color: #63b3ed !important;
                font-family: 'Outfit', sans-serif;
            }
            .metric-value {
                font-size: 24px;
                font-weight: bold;
                color: #ffffff;
            }
            .metric-label {
                font-size: 14px;
                color: #a0aec0;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
