from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

# ── Color palette ─────────────────────────────────────────────────────────────
WHITE = "#FFFFFF"
INK_BLACK = "#00171F"
DEEP_BLUE = "#003459"
CERULEAN = "#007EA7"
FRESH_SKY = "#00A8E8"

# Extended palette — same cool aquatic vibe
LIGHT_BLUE = "#E0F4FB"  # very light sky — backgrounds, subtle fills
MID_BLUE = "#005F8A"  # between deep and cerulean
TEAL = "#00B4A6"  # warm teal accent
SLATE = "#2C4A52"  # muted dark — secondary text, borders
STEEL = "#8DB7C7"  # mid-tone for neutral chart fills
ICE = "#D6EFF7"  # lightest blue — card backgrounds

# Semantic colors
SUCCESS = "#00B894"  # green — safe / positive
WARNING = "#FDCB6E"  # amber — swarmp / caution
DANGER = "#D63031"  # red — unsafe / critical

# Background
BG = WHITE

# Color sequences for charts (ordered by visual weight)
COLOR_SEQUENCE = [DEEP_BLUE, CERULEAN, FRESH_SKY, TEAL, MID_BLUE, STEEL]

# Color scales
COLOR_SCALE = [[0, ICE], [0.5, CERULEAN], [1, INK_BLACK]]
COLOR_SCALE_WARM = [[0, LIGHT_BLUE], [0.5, FRESH_SKY], [1, DEEP_BLUE]]
COLOR_SCALE_RISK = [[0, ICE], [0.5, WARNING], [1, DANGER]]

# Facade status colors
FACADE_COLORS = {
    "SAFE": SUCCESS,
    "SWARMP": WARNING,
    "UNSAFE": DANGER,
    "NO REPORT FILED": STEEL,
    "UNKNOWN": STEEL,
}

# Analytics threshold
UNSAFE_THRESHOLD = 0.30


# ── CSS injection ─────────────────────────────────────────────────────────────
def apply_css() -> None:
    st.markdown(
        f"""
    <style>
        .stApp {{ background-color: {WHITE}; }}
        section[data-testid="stSidebar"] {{ background-color: {INK_BLACK}; }}
        section[data-testid="stSidebar"] * {{ color: {WHITE} !important; }}

        h1, h2, h3 {{ color: {INK_BLACK} !important; font-weight: 600; }}
        p, li {{ color: {SLATE}; }}

        /* Tab styling */
        .stTabs [data-baseweb="tab-list"] {{
            background-color: {LIGHT_BLUE};
            border-radius: 10px;
            padding: 4px 6px;
            gap: 4px;
        }}
        .stTabs [data-baseweb="tab"] {{
            color: {DEEP_BLUE};
            font-weight: 500;
            border-radius: 8px;
            padding: 6px 16px;
        }}
        .stTabs [aria-selected="true"] {{
            background-color: {DEEP_BLUE} !important;
            color: {WHITE} !important;
            border-radius: 8px;
        }}

        /* Metrics */
        [data-testid="stMetricValue"] {{ color: {INK_BLACK} !important; font-weight: 700; }}
        [data-testid="stMetricLabel"] {{ color: {CERULEAN} !important; font-size: 13px; }}
        [data-testid="metric-container"] {{
            background: {LIGHT_BLUE};
            border-radius: 10px;
            padding: 12px 16px;
            border-left: 4px solid {CERULEAN};
        }}

        /* Divider */
        hr {{ border-color: {LIGHT_BLUE}; }}

        /* Expander */
        .streamlit-expanderHeader {{
            background-color: {LIGHT_BLUE};
            border-radius: 8px;
            color: {DEEP_BLUE} !important;
            font-weight: 500;
        }}

        /* Caption */
        .stCaption {{ color: {STEEL} !important; font-size: 12px; }}

        /* Success/info boxes */
        .stSuccess {{ background-color: {LIGHT_BLUE}; border-left-color: {CERULEAN}; }}
        .stInfo {{ background-color: {LIGHT_BLUE}; border-left-color: {FRESH_SKY}; }}
    </style>
    """,
        unsafe_allow_html=True,
    )


# ── Chart theme ───────────────────────────────────────────────────────────────
def apply_chart_theme(fig) -> go.Figure:
    fig.update_layout(
        paper_bgcolor=WHITE,
        plot_bgcolor=WHITE,
        font_color=INK_BLACK,
        font_family="Arial, sans-serif",
        title_font_color=INK_BLACK,
        title_font_size=15,
        title_font_family="Arial, sans-serif",
        legend_bgcolor=WHITE,
        legend_bordercolor=LIGHT_BLUE,
        legend_borderwidth=1,
        margin={"r": 10, "t": 50, "l": 10, "b": 10},
    )
    fig.update_xaxes(
        gridcolor=LIGHT_BLUE,
        linecolor=ICE,
        tickfont_color=SLATE,
        title_font_color=SLATE,
    )
    fig.update_yaxes(
        gridcolor=LIGHT_BLUE,
        linecolor=ICE,
        tickfont_color=SLATE,
        title_font_color=SLATE,
    )
    return fig


# ── Alert / info boxes ────────────────────────────────────────────────────────
def warning_box(message: str) -> None:
    st.markdown(
        f"<div style='background:{DANGER};color:{WHITE};border-radius:10px;"
        f"padding:14px 18px;margin:10px 0;font-weight:500;font-size:14px'>"
        f"⚠️ {message}</div>",
        unsafe_allow_html=True,
    )


def caution_box(message: str) -> None:
    st.markdown(
        f"<div style='background:{WARNING};color:{INK_BLACK};border-radius:10px;"
        f"padding:14px 18px;margin:10px 0;font-weight:500;font-size:14px'>"
        f"⚠️ {message}</div>",
        unsafe_allow_html=True,
    )


def info_box(message: str) -> None:
    st.markdown(
        f"<div style='background:{LIGHT_BLUE};color:{INK_BLACK};border-radius:10px;"
        f"padding:14px 18px;margin:10px 0;border-left:4px solid {CERULEAN};font-size:14px'>"
        f"💡 {message}</div>",
        unsafe_allow_html=True,
    )


def success_box(message: str) -> None:
    st.markdown(
        f"<div style='background:#E8F8F5;color:{INK_BLACK};border-radius:10px;"
        f"padding:14px 18px;margin:10px 0;border-left:4px solid {SUCCESS};font-size:14px'>"
        f"✅ {message}</div>",
        unsafe_allow_html=True,
    )


# ── Page header ───────────────────────────────────────────────────────────────
def page_header(title: str, subtitle: str = "") -> None:
    st.markdown(
        f"<h1 style='color:{INK_BLACK};font-size:28px;font-weight:700;"
        f"border-bottom:3px solid {CERULEAN};padding-bottom:10px;margin-bottom:6px'>"
        f"{title}</h1>",
        unsafe_allow_html=True,
    )
    if subtitle:
        st.markdown(
            f"<p style='color:{SLATE};font-size:14px;margin-top:4px;margin-bottom:16px'>"
            f"{subtitle}</p>",
            unsafe_allow_html=True,
        )


# ── Section divider ───────────────────────────────────────────────────────────
def section_header(title: str) -> None:
    st.markdown(
        f"<h3 style='color:{DEEP_BLUE};font-size:17px;font-weight:600;"
        f"border-left:4px solid {CERULEAN};padding-left:10px;margin-top:24px'>"
        f"{title}</h3>",
        unsafe_allow_html=True,
    )
