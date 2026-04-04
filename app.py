import base64
import html
import importlib.util
import os
import time
from io import BytesIO
from pathlib import Path
import streamlit as st
import streamlit.components.v1 as components
from datetime import datetime, timedelta, timezone
import colorsys
import hashlib
import random
import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials
import pandas as pd
import json
from collections import defaultdict
import plotly.express as px
import plotly.graph_objects as go
from upstash_redis import Redis

# Same spreadsheet as CG Combined / Attendance (NWST Health)
NWST_HEALTH_SHEET_ID = "1uexbQinWl1r6NgmSrmOXPtWs-q4OJV3o1OwLywMWzzY"
# Processed Attendance grid for Cell Attendance charts (shared across instances via Upstash)
NWST_REDIS_ATTENDANCE_CHART_GRID_KEY = "nwst_attendance_chart_grid"

# CHECK IN attendance spreadsheet — used only for the Analytics tab (Attendance Analytics, Options, Key Values)
CHECKIN_ATTENDANCE_SHEET_ID = os.getenv("ATTENDANCE_SHEET_ID", "").strip()
if not CHECKIN_ATTENDANCE_SHEET_ID:
    try:
        if hasattr(st, "secrets") and "ATTENDANCE_SHEET_ID" in st.secrets:
            CHECKIN_ATTENDANCE_SHEET_ID = str(st.secrets["ATTENDANCE_SHEET_ID"]).strip()
    except FileNotFoundError:
        pass

NWST_KEY_VALUES_TAB = "Key Values"
NWST_ATTENDANCE_TAB = "Attendance"
NWST_OPTIONS_TAB = "Options"
NWST_ATTENDANCE_ANALYTICS_TAB = "Attendance Analytics"
NWST_STATUS_HISTORICAL_TAB = "Status Historical"
NWST_HISTORICAL_CELL_STATUS_TAB = "Historical Cell Status"

# Individual Attendance monthly matrix: trailing window of month columns (most recent in data).
MONTHLY_MEMBER_MATRIX_MAX_MONTHS = 4

# Map CG Combined **Cell** dropdown value → short tab name (matches Apps Script `tabNameToDisplayCellForHistory_` inverse).
_NWST_CELL_DISPLAY_TO_TAB = {
    "Anchor Street": "Anchor",
    "Aster Street": "Aster",
    "Crown Street": "Crown",
    "Street Fire": "Fire",
    "Fishers Street": "Fishers",
    "Street Forth": "Forth",
    "HIS Street": "HIS",
    "Home Street": "Home",
    "King Street": "King",
    "Life Street": "Life",
    "Meta Street": "Meta",
    "Royal Street": "Royal",
    "Street Runners": "Runners",
    "Shepherds Street": "Shepherds",
    "Street Lights": "Lights",
    "Via Dolorosa Street": "Via Dolorosa",
    "Narrowstreet Core Team": "Core Team",
}

# Shared by “Attendance rate by cell” (per-zone tabs) and “Zone Attendance Trend” in Analytics
NWST_ANALYTICS_MULTILINE_PALETTE = [
    "#FF2D95",
    "#00F0FF",
    "#FFE14A",
    "#B388FF",
    "#00FF94",
    "#FF6B2C",
    "#5EB8FF",
    "#FF4081",
]


def _nwst_analytics_palette_for_n(n_categories):
    """Repeat/cycle the analytics multiline palette so every series gets a color."""
    if n_categories <= 0:
        return []
    base = NWST_ANALYTICS_MULTILINE_PALETTE
    k = len(base)
    return [base[i % k] for i in range(n_categories)]


def _nwst_collapsible_section_css(primary_hex: str) -> str:
    """Style ``st.expander`` summary like CELL HEALTH section headers (green, uppercase, rule)."""
    c = html.escape(str(primary_hex or "#00ff00"), quote=True)
    return f"""<style>
div[data-testid="stExpander"] details {{
    background: transparent;
    border: none;
}}
/* Only the expander title row — not <summary> inside Cell/Member table trunc widgets */
div[data-testid="stExpander"] summary:not(.monthly-trunc-summary):not(.newcomer-trunc-summary) {{
    font-family: 'Inter', sans-serif !important;
    font-weight: 900 !important;
    font-size: 1.2rem !important;
    color: {c} !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
    list-style: none !important;
    cursor: pointer;
    padding: 0.5rem 0.75rem 0.6rem 0.75rem !important;
    margin: 0 0 0.35rem 0 !important;
    border-bottom: 3px solid {c} !important;
    background: #000000 !important;
}}
div[data-testid="stExpander"] summary:not(.monthly-trunc-summary):not(.newcomer-trunc-summary)::-webkit-details-marker {{
    display: none !important;
}}
</style>"""


def _render_cg_newcomer_section(newcomers_df, display_df, cell_filter, cell_columns, daily_colors):
    """Content for CG Health > Newcomer collapsible."""
    status_columns = [col for col in newcomers_df.columns if "status" in col.lower()]
    newcomer_df = newcomers_df.copy()
    if status_columns:
        newcomer_df = newcomer_df[newcomer_df[status_columns[0]] == "New"]

    if cell_filter != "All" and cell_columns:
        newcomer_df = newcomer_df[newcomer_df[cell_columns[0]] == cell_filter]

    if not newcomer_df.empty:
        newcomer_count = len(newcomer_df)

        if cell_filter != "All" and cell_columns:
            total_in_cell = len(display_df[display_df[cell_columns[0]] == cell_filter])
        else:
            total_in_cell = len(display_df)

        newcomer_pct = (newcomer_count / total_in_cell * 100) if total_in_cell > 0 else 0

        kpi_col1, kpi_col2 = st.columns(2)

        with kpi_col1:
            st.markdown(
                f"""
            <div class="kpi-card">
                <div class="kpi-label">Total Newcomers</div>
                <div class="kpi-number">{newcomer_count}</div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with kpi_col2:
            st.markdown(
                f"""
            <div class="kpi-card">
                <div class="kpi-label">Newcomers %</div>
                <div class="kpi-number" style="color: {daily_colors['primary']};">{newcomer_pct:.0f}%</div>
                <div class="kpi-subtitle">{newcomer_count} of {total_in_cell}</div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        available_cols = newcomer_df.columns.tolist()

        default_cols = []
        for col in available_cols:
            col_lower = col.lower()
            if any(x in col_lower for x in ["name", "member"]) and "last" not in col_lower:
                default_cols.append(col)
            elif any(x in col_lower for x in ["notes", "note"]):
                default_cols.append(col)
            elif "new since" in col_lower:
                default_cols.append(col)

        st.markdown("**Select columns to display:**")
        selected_cols = st.multiselect(
            "Columns",
            options=available_cols,
            default=default_cols,
            key="newcomer_columns",
            label_visibility="collapsed",
        )

        st.markdown("#### Newcomer List")

        if selected_cols:
            st.markdown(
                render_newcomer_list_html_table(newcomer_df, selected_cols),
                unsafe_allow_html=True,
            )
        else:
            st.warning("Please select at least one column to display.")
    else:
        st.info("No newcomers found.")


def _render_cg_leadership_section(display_df, cell_filter, cell_columns, daily_colors):
    """Content for CG Health > Leadership collapsible."""
    if not display_df.empty:
        leadership_data = get_leadership_by_role(display_df)

        if leadership_data:
            total_leaders = sum(len(members) for members in leadership_data.values())

            if cell_filter != "All" and cell_columns:
                total_in_cell = len(display_df[display_df[cell_columns[0]] == cell_filter])
            else:
                total_in_cell = len(display_df)

            leader_pct = (total_leaders / total_in_cell * 100) if total_in_cell > 0 else 0

            leader_kpi_col1, leader_kpi_col2 = st.columns(2)

            with leader_kpi_col1:
                st.markdown(
                    f"""
                <div class="kpi-card">
                    <div class="kpi-label">Total Leaders</div>
                    <div class="kpi-number">{total_leaders}</div>
                </div>
                """,
                    unsafe_allow_html=True,
                )

            with leader_kpi_col2:
                st.markdown(
                    f"""
                <div class="kpi-card">
                    <div class="kpi-label">Leaders %</div>
                    <div class="kpi-number" style="color: {daily_colors['primary']};">{leader_pct:.0f}%</div>
                    <div class="kpi-subtitle">{total_leaders} of {total_in_cell}</div>
                </div>
                """,
                    unsafe_allow_html=True,
                )

            st.markdown("")

            for role_name, members in leadership_data.items():
                st.markdown(
                    f"<h3 style='color: {daily_colors['primary']}; font-size: 1.1rem;'>{role_name}</h3>",
                    unsafe_allow_html=True,
                )

                for leader in members:
                    since_text = f"Since: {leader['since']}" if leader["since"] else "Since: Not available"
                    st.markdown(
                        f"""
                    <div style='padding: 1rem; background: #1a1a1a; border-left: 3px solid {daily_colors['primary']}; margin-bottom: 0.75rem;'>
                        <p style='font-weight: 600; margin: 0;'>{leader['name']}</p>
                        <p style='font-size: 0.85rem; color: #999; margin: 0.25rem 0 0 0;'>{since_text}</p>
                    </div>
                    """,
                        unsafe_allow_html=True,
                    )

                st.markdown("")
        else:
            st.info("No leadership roles assigned yet.")
    else:
        st.info("No leadership data available.")


def _render_cg_cell_health_section(display_df, daily_colors, cell_filter="All", attendance_stats=None):
    """Cell health — KPI column layout + Historical Cell Status WoW pills + expandable name tiles."""
    if attendance_stats is None:
        attendance_stats = {}

    prim_hex = str(daily_colors.get("primary", "#00ff00"))
    prim = html.escape(prim_hex, quote=True)

    if display_df.empty:
        st.info("No cell health data available.")
        return

    work_df = display_df.copy()
    status_columns = [col for col in work_df.columns if "status" in col.lower()]
    status_col = status_columns[0] if status_columns else None

    if status_col:
        work_df["status_type"] = work_df[status_col].apply(extract_cell_sheet_status_type)
        new_count = len(work_df[work_df["status_type"] == "New"])
        regular_count = len(work_df[work_df["status_type"] == "Regular"])
        irregular_count = len(work_df[work_df["status_type"] == "Irregular"])
        follow_up_count = len(work_df[work_df["status_type"] == "Follow Up"])
        red_count = len(work_df[work_df["status_type"] == "Red"])
        graduated_count = len(work_df[work_df["status_type"] == "Graduated"])
    else:
        total_members_fb = len(work_df)
        new_count = max(1, int(total_members_fb * 0.20))
        regular_count = max(1, int(total_members_fb * 0.40))
        irregular_count = max(1, int(total_members_fb * 0.20))
        follow_up_count = max(1, int(total_members_fb * 0.10))
        red_count = max(1, int(total_members_fb * 0.05))
        graduated_count = (
            total_members_fb - new_count - regular_count - irregular_count - follow_up_count - red_count
        )

    total_members = new_count + regular_count + irregular_count + follow_up_count + red_count + graduated_count

    _cell_scoped = (
        cell_filter is not None
        and str(cell_filter).strip()
        and str(cell_filter).strip().lower() != "all"
    )
    if _cell_scoped:
        mix_denom = new_count + regular_count + irregular_count + follow_up_count
        if mix_denom > 0:
            new_pct = new_count / mix_denom * 100
            regular_pct = regular_count / mix_denom * 100
            irregular_pct = irregular_count / mix_denom * 100
            follow_up_pct = follow_up_count / mix_denom * 100
        else:
            new_pct = regular_pct = irregular_pct = follow_up_pct = 0.0
        red_pct = 0.0
        graduated_pct = 0.0
    else:
        new_pct = (new_count / total_members * 100) if total_members > 0 else 0
        regular_pct = (regular_count / total_members * 100) if total_members > 0 else 0
        irregular_pct = (irregular_count / total_members * 100) if total_members > 0 else 0
        follow_up_pct = (follow_up_count / total_members * 100) if total_members > 0 else 0
        red_pct = (red_count / total_members * 100) if total_members > 0 else 0
        graduated_pct = (graduated_count / total_members * 100) if total_members > 0 else 0

    hist_df = load_historical_cell_status_dataframe()
    curr_agg, prev_agg = None, None
    if hist_df is not None and not hist_df.empty:
        curr_agg, prev_agg, _, _ = _nwst_hist_cell_wow_for_scope(hist_df, cell_filter)

    wow_new = _nwst_cell_health_wow_pill_html("new", curr_agg, prev_agg)
    wow_regular = _nwst_cell_health_wow_pill_html("regular", curr_agg, prev_agg)
    wow_irregular = _nwst_cell_health_wow_pill_html("irregular", curr_agg, prev_agg)
    wow_follow_up = _nwst_cell_health_wow_pill_html("follow_up", curr_agg, prev_agg)
    wow_red = _nwst_cell_health_wow_pill_html("red", curr_agg, prev_agg)
    wow_graduated = _nwst_cell_health_wow_pill_html("graduated", curr_agg, prev_agg)

    st.markdown(
        f"""
<style>
  .ch-head-nwst {{
    font-family: 'Inter', sans-serif;
    font-weight: 700;
    font-size: 0.82rem;
    color: {prim};
    text-transform: uppercase;
    letter-spacing: 0.16em;
    margin: 0 0 1.35rem 0;
    display: block;
  }}
  /* Streamlit often draws a grey frame around markdown HTML — strip it for cell-health KPI cards */
  [data-testid="stMarkdownContainer"]:has(.ch-kpi-card-embed),
  [data-testid="stMarkdownContainer"]:has(.ch-kpi-card-embed) > div,
  [data-testid="element-container"]:has(.ch-kpi-card-embed) {{
    border: none !important;
    background: transparent !important;
    box-shadow: none !important;
    outline: none !important;
  }}
  .ch-kpi-wow-row {{
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: flex-start;
    flex-wrap: wrap;
    gap: 0.45rem 0.85rem;
    margin: 0.35rem 0 0.2rem 0;
  }}
  .ch-kpi-wow-row .kpi-number {{
    margin: 0 !important;
    line-height: 1;
    flex: 0 1 auto;
  }}
  .ch-kpi-wow-row .ch-pill-wrap {{
    display: inline-flex !important;
    width: fit-content !important;
    max-width: 100%;
    margin: 0 !important;
    padding: 0 !important;
    flex: 0 0 auto;
    min-width: 0;
    align-self: center;
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
  }}
  .ch-pill.ch-pill--hero {{
    font-size: clamp(0.88rem, 1.55vw, 1.12rem);
    padding: 0.3rem 0.78rem 0.32rem;
    gap: 0.34rem;
    font-weight: 700;
    letter-spacing: 0.03em;
    border: none;
    outline: none;
  }}
  /* Hero WoW: glow only — no 1px ring (reads as a second “frame”) */
  .ch-pill.ch-pill--hero.ch-pill-good {{
    box-shadow: 0 0 28px rgba(94, 234, 212, 0.38);
    text-shadow: 0 0 12px rgba(94, 234, 212, 0.45);
  }}
  .ch-pill.ch-pill--hero.ch-pill-bad {{
    box-shadow: 0 0 24px rgba(253, 164, 175, 0.28);
    text-shadow: 0 0 10px rgba(253, 164, 175, 0.35);
  }}
  .ch-pill.ch-pill--hero.ch-pill-flat {{
    box-shadow: none;
    background: rgba(42, 42, 42, 0.95);
  }}
  .ch-pill.ch-pill--hero.ch-pill-na {{
    box-shadow: none;
    background: #2a2a2a;
  }}
  .ch-pill--hero .ch-pill-arrow {{
    font-size: 0.95em;
  }}
  .ch-pill-wrap {{ margin-top: 0.35rem; line-height: 1; max-width: 100%; }}
  .ch-pill {{
    display: inline-flex;
    align-items: center;
    gap: 0.2rem;
    padding: 0.14rem 0.42rem 0.15rem;
    border-radius: 9999px;
    font-family: 'Inter', sans-serif;
    font-size: 0.52rem;
    font-weight: 600;
    letter-spacing: 0.02em;
    white-space: nowrap;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
  }}
  .ch-pill-good {{
    background: #153729;
    color: #5eead4;
    box-shadow: 0 0 14px rgba(94, 234, 212, 0.22);
    text-shadow: 0 0 10px rgba(94, 234, 212, 0.35);
  }}
  .ch-pill-bad {{
    background: #351a22;
    color: #fda4af;
    box-shadow: 0 0 12px rgba(253, 164, 175, 0.18);
    text-shadow: 0 0 8px rgba(253, 164, 175, 0.3);
  }}
  .ch-pill-flat {{
    background: #2a2a2a;
    color: #c6c6c6;
    font-weight: 500;
    box-shadow: inset 0 0 0 1px rgba(255,255,255,0.06);
  }}
  .ch-pill-na {{
    background: #252525;
    color: #888;
    font-weight: 400;
    white-space: normal;
  }}
  .ch-pill-arrow {{
    font-size: 0.68em;
    font-weight: 700;
    line-height: 1;
    opacity: 0.95;
  }}
</style>
""",
        unsafe_allow_html=True,
    )

    _ch_head_l, _ch_head_r = st.columns([11, 1])
    with _ch_head_l:
        st.markdown(
            f'<p class="ch-head-nwst">Cell health</p>',
            unsafe_allow_html=True,
        )
    with _ch_head_r:
        try:
            _ch_b64 = _nwst_cell_health_kpi_png_b64(display_df, cell_filter, daily_colors)
            components.html(
                _nwst_ch_cell_health_copy_icon_html(_ch_b64, prim_hex),
                height=52,
                width=72,
            )
        except ImportError:
            st.caption("pip install Pillow to enable KPI copy")

    def _member_tiles(data_df, border_color):
        if data_df.empty:
            st.caption("No members in this bucket.")
            return
        if "name" in data_df.columns or "Name" in data_df.columns:
            name_col = "name" if "name" in data_df.columns else "Name"
            cell_col = None
            for col in data_df.columns:
                if col.lower().strip() in ["cell", "group"]:
                    cell_col = col
                    break
            bc = html.escape(border_color, quote=True)
            names = sorted(data_df[name_col].astype(str).unique().tolist())
            parts = []
            for name in names:
                person_cell = ""
                if cell_col:
                    person_row = data_df[data_df[name_col] == name]
                    if not person_row.empty:
                        person_cell = str(person_row[cell_col].iloc[0]).strip()
                tooltip_text = get_attendance_text(name, person_cell, attendance_stats)
                tip_e = html.escape(tooltip_text, quote=True)
                name_e = html.escape(str(name), quote=True)
                parts.append(
                    f'<span class="member-tile" style="border-color: {bc};" data-tooltip="{tip_e}">{name_e}</span> '
                )
            st.markdown("".join(parts), unsafe_allow_html=True)
        else:
            st.dataframe(data_df, use_container_width=True)

    def _cell_health_mix_card_html(accent_hex, kpi_label, pct_val, n_members, wow_fragment):
        ae = html.escape(accent_hex, quote=True)
        kl = html.escape(kpi_label, quote=True)
        return f"""
            <div class="kpi-card ch-kpi-card-embed" style="cursor: pointer;">
                <div class="kpi-label">{kl}</div>
                <div class="ch-kpi-wow-row">
                    <div class="kpi-number" style="color: {ae};">{pct_val:.0f}%</div>
                    {wow_fragment}
                </div>
                <div class="kpi-subtitle">{n_members} members</div>
            </div>
            """

    for _sk in (
        "expand_new",
        "expand_regular",
        "expand_irregular",
        "expand_follow_up",
        "expand_red",
        "expand_graduated",
    ):
        if _sk not in st.session_state:
            st.session_state[_sk] = False

    if _cell_scoped:
        col1, col2, col3, col4 = st.columns(4)
    else:
        col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("🔵 New", key="btn_new", use_container_width=True):
            st.session_state.expand_new = not st.session_state.expand_new
        st.markdown(
            _cell_health_mix_card_html("#3498db", "New Members", new_pct, new_count, wow_new),
            unsafe_allow_html=True,
        )
        if st.session_state.expand_new:
            st.markdown(
                "<p style='color: #3498db; font-weight: 600;'>New Members</p>",
                unsafe_allow_html=True,
            )
            if status_col:
                new_data = work_df[work_df["status_type"] == "New"].copy()
            else:
                new_data = work_df.head(new_count).copy()
            _member_tiles(new_data, "#3498db")

    with col2:
        if st.button("🟢 Regular", key="btn_regular", use_container_width=True):
            st.session_state.expand_regular = not st.session_state.expand_regular
        st.markdown(
            _cell_health_mix_card_html(
                "#2ecc71", "Regular Members", regular_pct, regular_count, wow_regular
            ),
            unsafe_allow_html=True,
        )
        if st.session_state.expand_regular:
            st.markdown(
                "<p style='color: #2ecc71; font-weight: 600;'>Regular Members (75% and above attendance)</p>",
                unsafe_allow_html=True,
            )
            if status_col:
                regular_data = work_df[work_df["status_type"] == "Regular"].copy()
            else:
                regular_data = work_df.iloc[new_count : new_count + regular_count].copy()
            _member_tiles(regular_data, "#2ecc71")

    with col3:
        if st.button("🟠 Irregular", key="btn_irregular", use_container_width=True):
            st.session_state.expand_irregular = not st.session_state.expand_irregular
        st.markdown(
            _cell_health_mix_card_html(
                "#e67e22",
                "Irregular Members",
                irregular_pct,
                irregular_count,
                wow_irregular,
            ),
            unsafe_allow_html=True,
        )
        if st.session_state.expand_irregular:
            st.markdown(
                "<p style='color: #e67e22; font-weight: 600;'>Irregular Members (Below 75% attendance)</p>",
                unsafe_allow_html=True,
            )
            if status_col:
                irregular_data = work_df[work_df["status_type"] == "Irregular"].copy()
            else:
                irregular_data = work_df.iloc[
                    new_count + regular_count : new_count + regular_count + irregular_count
                ].copy()
            _member_tiles(irregular_data, "#e67e22")

    if _cell_scoped:
        with col4:
            if st.button("🟡 Follow Up", key="btn_follow_up", use_container_width=True):
                st.session_state.expand_follow_up = not st.session_state.expand_follow_up
            st.markdown(
                _cell_health_mix_card_html(
                    "#f39c12", "Follow Up", follow_up_pct, follow_up_count, wow_follow_up
                ),
                unsafe_allow_html=True,
            )
            if st.session_state.expand_follow_up:
                st.markdown(
                    "<p style='color: #f39c12; font-weight: 600;'>Follow Up (0% attendance - past 2 months)</p>",
                    unsafe_allow_html=True,
                )
                if status_col:
                    follow_up_data = work_df[work_df["status_type"] == "Follow Up"].copy()
                else:
                    follow_up_data = work_df.iloc[
                        new_count
                        + regular_count
                        + irregular_count : new_count
                        + regular_count
                        + irregular_count
                        + follow_up_count
                    ].copy()
                _member_tiles(follow_up_data, "#f39c12")
    else:
        st.markdown("")
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("🟡 Follow Up", key="btn_follow_up", use_container_width=True):
                st.session_state.expand_follow_up = not st.session_state.expand_follow_up
            st.markdown(
                _cell_health_mix_card_html(
                    "#f39c12", "Follow Up", follow_up_pct, follow_up_count, wow_follow_up
                ),
                unsafe_allow_html=True,
            )
            if st.session_state.expand_follow_up:
                st.markdown(
                    "<p style='color: #f39c12; font-weight: 600;'>Follow Up (0% attendance - past 2 months)</p>",
                    unsafe_allow_html=True,
                )
                if status_col:
                    follow_up_data = work_df[work_df["status_type"] == "Follow Up"].copy()
                else:
                    follow_up_data = work_df.iloc[
                        new_count
                        + regular_count
                        + irregular_count : new_count
                        + regular_count
                        + irregular_count
                        + follow_up_count
                    ].copy()
                _member_tiles(follow_up_data, "#f39c12")

        with col2:
            if st.button("🔴 Red", key="btn_red", use_container_width=True):
                st.session_state.expand_red = not st.session_state.expand_red
            st.markdown(
                _cell_health_mix_card_html("#e74c3c", "Red", red_pct, red_count, wow_red),
                unsafe_allow_html=True,
            )
            if st.session_state.expand_red:
                st.markdown(
                    "<p style='color: #e74c3c; font-weight: 600;'>Red (Won't come to church anymore)</p>",
                    unsafe_allow_html=True,
                )
                if status_col:
                    red_data = work_df[work_df["status_type"] == "Red"].copy()
                else:
                    red_data = work_df.iloc[
                        new_count
                        + regular_count
                        + irregular_count
                        + follow_up_count : new_count
                        + regular_count
                        + irregular_count
                        + follow_up_count
                        + red_count
                    ].copy()
                _member_tiles(red_data, "#e74c3c")

        with col3:
            if st.button("⭐ Graduated", key="btn_graduated", use_container_width=True):
                st.session_state.expand_graduated = not st.session_state.expand_graduated
            st.markdown(
                _cell_health_mix_card_html(
                    "#9b59b6", "Graduated", graduated_pct, graduated_count, wow_graduated
                ),
                unsafe_allow_html=True,
            )
            if st.session_state.expand_graduated:
                st.markdown(
                    "<p style='color: #9b59b6; font-weight: 600;'>Graduated (Moved to leadership roles)</p>",
                    unsafe_allow_html=True,
                )
                if status_col:
                    graduated_data = work_df[work_df["status_type"] == "Graduated"].copy()
                else:
                    graduated_data = work_df.iloc[
                        new_count
                        + regular_count
                        + irregular_count
                        + follow_up_count
                        + red_count :
                    ].copy()
                _member_tiles(graduated_data, "#9b59b6")

    st.markdown("")


def _render_cg_detailed_members_section(display_df, daily_colors):
    """Content for CG Health > Detailed Members collapsible."""
    if not display_df.empty:
        # Display total count
        total_count = len(display_df)
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Total Members</div>
            <div class="kpi-number">{total_count}</div>
        </div>
        """, unsafe_allow_html=True)

        # Allowed columns for Detailed Members (only these as options)
        allowed_col_names = ["Name", "Cell", "Age", "Gender", "Role", "Status", "Birthday"]
        default_col_names = ["Name", "Age", "Birthday", "Gender"]

        # Map to actual column names in the dataframe (case-insensitive)
        col_map = {c.lower().strip(): c for c in display_df.columns}
        available_cols = [col_map[name.lower()] for name in allowed_col_names if name.lower() in col_map]
        default_cols = [col_map[name.lower()] for name in default_col_names if name.lower() in col_map]

        # Column selection widget
        st.markdown("**Select columns to display:**")
        selected_cols = st.multiselect(
            "Columns",
            options=available_cols,
            default=default_cols,
            key="detailed_columns",
            label_visibility="collapsed"
        )

        st.markdown("#### All Members")

        if selected_cols:
            # Create a display dataframe with selected columns
            display_detailed_df = display_df[selected_cols].copy()
            st.dataframe(display_detailed_df, use_container_width=True, hide_index=True)
        else:
            st.warning("Please select at least one column to display.")
    else:
        st.info("No members found.")


@st.cache_resource
def get_redis_client():
    """Initialize Upstash Redis client from Streamlit secrets."""
    try:
        redis_url = st.secrets.get("upstash_redis_url")
        redis_token = st.secrets.get("upstash_redis_token")

        if redis_url and redis_token:
            return Redis(url=redis_url, token=redis_token)
        return None
    except Exception:
        return None

@st.cache_resource
def get_google_sheet_client():
    """Initialize Google Sheets client using Streamlit secrets."""
    try:
        creds_dict = st.secrets["google"]
        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return gspread.authorize(creds)
    except Exception:
        return None

@st.cache_data(ttl=300)
def load_sheet_data():
    """Load data from Google Sheet 'CG Combined' tab or from Redis cache."""
    # Try Redis first
    redis = get_redis_client()
    if redis:
        try:
            cached_data = redis.get("nwst_cg_combined_data")
            if cached_data:
                data = json.loads(cached_data)
                df = pd.DataFrame(data["rows"], columns=data["columns"])
                return df
        except Exception:
            pass

    # Fall back to Google Sheets
    client = get_google_sheet_client()
    if not client:
        return pd.DataFrame()

    try:
        spreadsheet = client.open_by_key(NWST_HEALTH_SHEET_ID)
        worksheet = spreadsheet.worksheet("CG Combined")
        data = worksheet.get_all_values()

        if not data:
            return pd.DataFrame()

        # First row is headers
        df = pd.DataFrame(data[1:], columns=data[0])

        # Cache in Redis
        redis = get_redis_client()
        if redis:
            try:
                cache_data = {
                    "columns": df.columns.tolist(),
                    "rows": df.values.tolist()
                }
                redis.set("nwst_cg_combined_data", json.dumps(cache_data), ex=300)
            except Exception:
                pass

        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_ministries_sheet_data():
    """Load data from Google Sheet 'Ministries Combined' tab or from Redis cache."""
    # Try Redis first
    redis = get_redis_client()
    if redis:
        try:
            cached_data = redis.get("nwst_ministries_combined_data")
            if cached_data:
                data = json.loads(cached_data)
                df = pd.DataFrame(data["rows"], columns=data["columns"])
                return df
        except Exception:
            pass

    # Fall back to Google Sheets
    client = get_google_sheet_client()
    if not client:
        return pd.DataFrame()

    try:
        spreadsheet = client.open_by_key(NWST_HEALTH_SHEET_ID)
        worksheet = spreadsheet.worksheet("Ministries Combined")
        data = worksheet.get_all_values()

        if not data:
            return pd.DataFrame()

        # First row is headers
        df = pd.DataFrame(data[1:], columns=data[0])

        # Cache in Redis
        redis = get_redis_client()
        if redis:
            try:
                cache_data = {
                    "columns": df.columns.tolist(),
                    "rows": df.values.tolist()
                }
                redis.set("nwst_ministries_combined_data", json.dumps(cache_data), ex=300)
            except Exception:
                pass

        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_newcomers_data():
    """Load newcomers data from Google Sheet."""
    df = load_sheet_data()
    if df.empty:
        return pd.DataFrame()

    newcomers_df = df.copy()
    return newcomers_df

@st.cache_data(ttl=300)
def get_ministries_data():
    """Load ministries data from Google Sheet."""
    df = load_ministries_sheet_data()
    if df.empty:
        return pd.DataFrame()

    ministries_df = df.copy()
    return ministries_df

@st.cache_data(ttl=300)
def load_attendance_and_cg_dataframes():
    """Load Attendance + CG Combined sheets as DataFrames. Returns (att_df, cg_df) or (None, None)."""
    client = get_google_sheet_client()
    if not client:
        return None, None

    try:
        spreadsheet = client.open_by_key(NWST_HEALTH_SHEET_ID)
        att_worksheet = spreadsheet.worksheet("Attendance")
        att_data = att_worksheet.get_all_values()
        cg_worksheet = spreadsheet.worksheet("CG Combined")
        cg_data = cg_worksheet.get_all_values()

        if not att_data or len(att_data) < 2:
            return None, None
        if not cg_data or len(cg_data) < 2:
            return None, None

        att_df = pd.DataFrame(att_data[1:], columns=att_data[0])
        cg_df = pd.DataFrame(cg_data[1:], columns=cg_data[0])
        return att_df, cg_df
    except Exception:
        return None, None


@st.cache_data(ttl=300)
def load_status_historical_dataframe():
    """Load **Status Historical** tab (monthly Regular/Irregular/Follow Up per member)."""
    client = get_google_sheet_client()
    if not client:
        return None
    try:
        spreadsheet = client.open_by_key(NWST_HEALTH_SHEET_ID)
        ws = spreadsheet.worksheet(NWST_STATUS_HISTORICAL_TAB)
        data = ws.get_all_values()
        if not data or len(data) < 2:
            return None
        return pd.DataFrame(data[1:], columns=data[0])
    except WorksheetNotFound:
        return None
    except Exception:
        return None


@st.cache_data(ttl=300)
def load_historical_cell_status_dataframe():
    """Load **Historical Cell Status** tab (per-cell snapshots + WoW columns from NWST script)."""
    client = get_google_sheet_client()
    if not client:
        return None
    try:
        spreadsheet = client.open_by_key(NWST_HEALTH_SHEET_ID)
        ws = spreadsheet.worksheet(NWST_HISTORICAL_CELL_STATUS_TAB)
        data = ws.get_all_values()
        if not data or len(data) < 2:
            return None
        return pd.DataFrame(data[1:], columns=data[0])
    except WorksheetNotFound:
        return None
    except Exception:
        return None


def _nwst_hist_cell_col_lookup(df):
    """Map normalized header → actual column name (strip/lower)."""
    return {str(c).strip().lower(): c for c in df.columns}


def _nwst_hist_cell_get_col(lk, *names):
    for n in names:
        k = n.strip().lower()
        if k in lk:
            return lk[k]
    return None


def _nwst_hist_cell_parse_snapshot_dates(df):
    lk = _nwst_hist_cell_col_lookup(df)
    snap_c = _nwst_hist_cell_get_col(lk, "snapshot date", "snapshot")
    if not snap_c:
        return None
    s = pd.to_datetime(df[snap_c], errors="coerce")
    dates = sorted(s.dropna().unique(), reverse=True)
    return [pd.Timestamp(d).date() if hasattr(d, "date") else d for d in dates]


def _nwst_hist_cell_rows_for_scope(df, cell_filter):
    """Filter log rows: All = every row; else match Cell (long) or Tab (short) fallback."""
    if df is None or df.empty:
        return df
    lk = _nwst_hist_cell_col_lookup(df)
    cell_c = _nwst_hist_cell_get_col(lk, "cell")
    tab_c = _nwst_hist_cell_get_col(lk, "tab")
    snap_c = _nwst_hist_cell_get_col(lk, "snapshot date", "snapshot")
    if not snap_c:
        return df.iloc[0:0]

    out = df.copy()
    out["_snap_parsed"] = pd.to_datetime(out[snap_c], errors="coerce")
    out = out[out["_snap_parsed"].notna()]

    if cell_filter and str(cell_filter).strip() and str(cell_filter).strip().lower() != "all":
        cf = str(cell_filter).strip()
        mask = pd.Series(False, index=out.index)
        if cell_c:
            mask = mask | (out[cell_c].astype(str).str.strip() == cf)
        short = _NWST_CELL_DISPLAY_TO_TAB.get(cf)
        if short and tab_c:
            mask = mask | (out[tab_c].astype(str).str.strip() == short)
        out = out[mask]
    return out


def _nwst_hist_cell_aggregate_counts(sub_df):
    """Sum numeric bucket columns for snapshot rows in `sub_df` (already scoped + same snapshot)."""
    if sub_df is None or sub_df.empty:
        return None
    lk = _nwst_hist_cell_col_lookup(sub_df)
    buckets = [
        ("total", ["total"]),
        ("new", ["new"]),
        ("regular", ["regular"]),
        ("irregular", ["irregular"]),
        ("follow up", ["follow up", "follow_up", "followup"]),
        ("red", ["red"]),
        ("graduated", ["graduated"]),
        ("duplicate", ["duplicate"]),
        ("other", ["other"]),
    ]
    agg = {}
    for canon, aliases in buckets:
        coln = None
        for a in aliases:
            coln = _nwst_hist_cell_get_col(lk, a)
            if coln:
                break
        if not coln:
            agg[canon] = 0
        else:
            agg[canon] = int(pd.to_numeric(sub_df[coln], errors="coerce").fillna(0).sum())
    return agg


def _nwst_hist_cell_wow_for_scope(hist_df, cell_filter):
    """Return (curr_agg, prev_agg, snap_curr, snap_prev) from latest two snapshot dates; may be partial."""
    if hist_df is None or hist_df.empty:
        return None, None, None, None
    scoped = _nwst_hist_cell_rows_for_scope(hist_df, cell_filter)
    if scoped is None or scoped.empty:
        return None, None, None, None
    lk = _nwst_hist_cell_col_lookup(scoped)
    snap_c = _nwst_hist_cell_get_col(lk, "snapshot date", "snapshot")
    if not snap_c:
        return None, None, None, None

    dates = _nwst_hist_cell_parse_snapshot_dates(scoped)
    if not dates:
        return None, None, None, None

    snap_curr = dates[0]
    snap_prev = dates[1] if len(dates) > 1 else None

    def _norm_snap(val):
        t = pd.to_datetime(val, errors="coerce")
        if pd.isna(t):
            return None
        return t.date()

    scoped = scoped.copy()
    scoped["_d"] = scoped[snap_c].map(_norm_snap)
    curr_sub = scoped[scoped["_d"] == snap_curr]
    curr = _nwst_hist_cell_aggregate_counts(curr_sub)
    prev = None
    if snap_prev is not None:
        prev_sub = scoped[scoped["_d"] == snap_prev]
        prev = _nwst_hist_cell_aggregate_counts(prev_sub)
    return curr, prev, snap_curr, snap_prev


def _nwst_cell_health_wow_color_for_delta(bucket_key, delta_n):
    """Regular, graduated: more members = good (green). New: any non‑zero change = good (green); 0 = grey.
    Risk-style buckets: fewer = good (green). Graduated is always grey."""
    if delta_n is None or (isinstance(delta_n, float) and pd.isna(delta_n)):
        return "#aaaaaa"
    if bucket_key == "graduated":
        return "#aaaaaa"
    if bucket_key == "new":
        return "#2ecc71" if delta_n != 0 else "#aaaaaa"
    if delta_n == 0:
        return "#aaaaaa"
    if bucket_key == "regular":
        return "#2ecc71" if delta_n > 0 else "#e74c3c"
    return "#2ecc71" if delta_n < 0 else "#e74c3c"


def _nwst_cell_health_wow_pill_html(bucket_key, curr_agg, prev_agg):
    """WoW delta pill HTML (arrow + member delta + pp delta) for one cell-health bucket."""

    def _agg_n(agg, key):
        if not agg:
            return 0
        if key == "follow_up":
            return int(agg.get("follow up", 0) or 0)
        return int(agg.get(key, 0) or 0)

    d_mem = None
    d_pp = None
    if curr_agg and prev_agg:
        c = _agg_n(curr_agg, bucket_key)
        p = _agg_n(prev_agg, bucket_key)
        d_mem = c - p
        tot_c = _agg_n(curr_agg, "total")
        tot_p = _agg_n(prev_agg, "total")
        if tot_p > 0 and tot_c > 0:
            d_pp = (100.0 * c / tot_c) - (100.0 * p / tot_p)

    if curr_agg and prev_agg and d_mem is not None and d_pp is not None:
        pp_sh = float(d_pp)
        pp_str = f"{pp_sh:+.1f}%"
        mem_str = f"{d_mem:+d}"
        bubble_txt = html.escape(f"{mem_str} ({pp_str})", quote=True)
        flat = d_mem == 0 and abs(pp_sh) < 0.05
        if bucket_key == "new":
            if d_mem == 0:
                arrow = "·"
                pill_cls = "ch-pill-flat"
            else:
                arrow = "↑" if d_mem > 0 else "↓"
                pill_cls = "ch-pill-good"
        elif flat:
            arrow = "·"
            pill_cls = "ch-pill-flat"
        elif d_mem == 0:
            arrow = "·"
            tone = _nwst_cell_health_wow_color_for_delta(bucket_key, pp_sh)
            if tone == "#2ecc71":
                pill_cls = "ch-pill-good"
            elif tone == "#e74c3c":
                pill_cls = "ch-pill-bad"
            else:
                pill_cls = "ch-pill-flat"
        else:
            arrow = "↑" if d_mem > 0 else "↓"
            tone = _nwst_cell_health_wow_color_for_delta(bucket_key, d_mem)
            if tone == "#2ecc71":
                pill_cls = "ch-pill-good"
            elif tone == "#e74c3c":
                pill_cls = "ch-pill-bad"
            else:
                pill_cls = "ch-pill-flat"
        return (
            f'<div class="ch-pill-wrap"><span class="ch-pill ch-pill--hero {pill_cls}">'
            f'<span class="ch-pill-arrow">{html.escape(arrow, quote=True)}</span>'
            f"<span>{bubble_txt}</span>"
            f"</span></div>"
        )

    return (
        '<div class="ch-pill-wrap"><span class="ch-pill ch-pill--hero ch-pill-na">'
        "Need 2 log snapshots</span></div>"
    )


def _nwst_cell_health_wow_summary_text(bucket_key, curr_agg, prev_agg):
    """Plain-text WoW line for KPI snapshot images (mirrors ``_nwst_cell_health_wow_pill_html`` logic)."""

    def _agg_n(agg, key):
        if not agg:
            return 0
        if key == "follow_up":
            return int(agg.get("follow up", 0) or 0)
        return int(agg.get(key, 0) or 0)

    if not curr_agg or not prev_agg:
        return "Need 2 log snapshots"

    c = _agg_n(curr_agg, bucket_key)
    p = _agg_n(prev_agg, bucket_key)
    d_mem = c - p
    tot_c = _agg_n(curr_agg, "total")
    tot_p = _agg_n(prev_agg, "total")
    if tot_p <= 0 or tot_c <= 0:
        return "—"

    pp_sh = float((100.0 * c / tot_c) - (100.0 * p / tot_p))
    pp_str = f"{pp_sh:+.1f}%"
    mem_str = f"{d_mem:+d}"
    flat = d_mem == 0 and abs(pp_sh) < 0.05

    if bucket_key == "new":
        arrow = "·" if d_mem == 0 else ("↑" if d_mem > 0 else "↓")
    elif flat:
        arrow = "·"
    elif d_mem == 0:
        arrow = "·"
    else:
        arrow = "↑" if d_mem > 0 else "↓"

    return f"{arrow} {mem_str} ({pp_str})"


def _nwst_pil_font(size: int):
    from PIL import ImageFont

    for path in (
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/Library/Fonts/Arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        r"C:\Windows\Fonts\arial.ttf",
    ):
        if os.path.isfile(path):
            try:
                return ImageFont.truetype(path, size)
            except OSError:
                continue
    return ImageFont.load_default()


def _nwst_cell_health_kpi_png_b64(display_df, cell_filter, daily_colors) -> str:
    """Rasterize current Cell Health KPI grid (respects cell filter) for clipboard export."""
    from PIL import Image, ImageDraw

    if display_df is None or display_df.empty:
        raise ValueError("No cell health data to export.")

    work_df = display_df.copy()
    status_columns = [col for col in work_df.columns if "status" in col.lower()]
    status_col = status_columns[0] if status_columns else None

    if status_col:
        work_df["status_type"] = work_df[status_col].apply(extract_cell_sheet_status_type)
        new_count = len(work_df[work_df["status_type"] == "New"])
        regular_count = len(work_df[work_df["status_type"] == "Regular"])
        irregular_count = len(work_df[work_df["status_type"] == "Irregular"])
        follow_up_count = len(work_df[work_df["status_type"] == "Follow Up"])
        red_count = len(work_df[work_df["status_type"] == "Red"])
        graduated_count = len(work_df[work_df["status_type"] == "Graduated"])
    else:
        total_members_fb = len(work_df)
        new_count = max(1, int(total_members_fb * 0.20))
        regular_count = max(1, int(total_members_fb * 0.40))
        irregular_count = max(1, int(total_members_fb * 0.20))
        follow_up_count = max(1, int(total_members_fb * 0.10))
        red_count = max(1, int(total_members_fb * 0.05))
        graduated_count = (
            total_members_fb - new_count - regular_count - irregular_count - follow_up_count - red_count
        )

    total_members = new_count + regular_count + irregular_count + follow_up_count + red_count + graduated_count

    cell_scoped = (
        cell_filter is not None
        and str(cell_filter).strip()
        and str(cell_filter).strip().lower() != "all"
    )
    if cell_scoped:
        mix_denom = new_count + regular_count + irregular_count + follow_up_count
        if mix_denom > 0:
            new_pct = new_count / mix_denom * 100
            regular_pct = regular_count / mix_denom * 100
            irregular_pct = irregular_count / mix_denom * 100
            follow_up_pct = follow_up_count / mix_denom * 100
        else:
            new_pct = regular_pct = irregular_pct = follow_up_pct = 0.0
        red_pct = 0.0
        graduated_pct = 0.0
    else:
        new_pct = (new_count / total_members * 100) if total_members > 0 else 0
        regular_pct = (regular_count / total_members * 100) if total_members > 0 else 0
        irregular_pct = (irregular_count / total_members * 100) if total_members > 0 else 0
        follow_up_pct = (follow_up_count / total_members * 100) if total_members > 0 else 0
        red_pct = (red_count / total_members * 100) if total_members > 0 else 0
        graduated_pct = (graduated_count / total_members * 100) if total_members > 0 else 0

    hist_df = load_historical_cell_status_dataframe()
    curr_agg, prev_agg = None, None
    if hist_df is not None and not hist_df.empty:
        curr_agg, prev_agg, _, _ = _nwst_hist_cell_wow_for_scope(hist_df, cell_filter)

    ws = lambda k: _nwst_cell_health_wow_summary_text(k, curr_agg, prev_agg)
    cards = [
        ("NEW MEMBERS", new_pct, new_count, "#3498db", ws("new")),
        ("REGULAR MEMBERS", regular_pct, regular_count, "#2ecc71", ws("regular")),
        ("IRREGULAR MEMBERS", irregular_pct, irregular_count, "#e67e22", ws("irregular")),
        ("FOLLOW UP", follow_up_pct, follow_up_count, "#f39c12", ws("follow_up")),
        ("RED", red_pct, red_count, "#e74c3c", ws("red")),
        ("GRADUATED", graduated_pct, graduated_count, "#9b59b6", ws("graduated")),
    ]
    if cell_scoped:
        cards = cards[:4]

    accent = str(daily_colors.get("primary", "#00ff00")).lstrip("#")
    if len(accent) == 6:
        title_rgb = tuple(int(accent[i : i + 2], 16) for i in (0, 2, 4))
    else:
        title_rgb = (0, 255, 0)

    scope_label = (
        "All cells"
        if cell_filter is None or str(cell_filter).strip().lower() == "all"
        else str(cell_filter).strip()
    )

    # Larger typography than the original export (user request); palette matches the live app.
    W, pad, gap, radius = 1240, 40, 20, 14
    cols = 2 if len(cards) == 4 else 3
    rows = (len(cards) + cols - 1) // cols
    card_w = (W - pad * 2 - gap * (cols - 1)) // cols
    card_h = 198
    title_block = 96
    H = pad * 2 + title_block + rows * card_h + max(0, rows - 1) * gap

    img = Image.new("RGB", (W, H), (11, 11, 11))
    draw = ImageDraw.Draw(img)
    f_title = _nwst_pil_font(26)
    f_scope = _nwst_pil_font(19)
    f_lbl = _nwst_pil_font(15)
    f_pct = _nwst_pil_font(48)
    f_wow = _nwst_pil_font(16)
    f_sub = _nwst_pil_font(16)
    border_rgb = (198, 123, 79)

    draw.text((pad, pad), "CELL HEALTH", fill=title_rgb, font=f_title)
    draw.text((pad, pad + 38), scope_label, fill=(190, 190, 190), font=f_scope)

    def _parse_hex(h):
        h = h.lstrip("#")
        return tuple(int(h[i : i + 2], 16) for i in (0, 2, 4)) if len(h) == 6 else (255, 255, 255)

    for i, (label, pct, n_mem, col_hex, wow_txt) in enumerate(cards):
        r, c = divmod(i, cols)
        x0 = pad + c * (card_w + gap)
        y0 = pad + title_block + r * (card_h + gap)
        x1, y1 = x0 + card_w, y0 + card_h
        draw.rounded_rectangle([x0, y0, x1, y1], radius=radius, outline=border_rgb, width=2, fill=(24, 24, 24))
        col_rgb = _parse_hex(col_hex)
        tx = x0 + 16
        ty = y0 + 14
        draw.text((tx, ty), label, fill=(200, 200, 200), font=f_lbl)
        draw.text((tx, ty + 28), f"{pct:.0f}%", fill=col_rgb, font=f_pct)
        draw.text((tx, ty + 100), wow_txt[:44], fill=(160, 210, 200), font=f_wow)
        draw.text((tx, y1 - 34), f"{n_mem} members", fill=(170, 170, 170), font=f_sub)

    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _nwst_ch_cell_health_copy_icon_html(b64_png: str, accent_hex: str) -> str:
    acc = html.escape(accent_hex, quote=True)
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"/></head>
<body style="margin:0;padding:0;background:transparent;">
<button type="button" id="nwstChCopy" title="Copy Cell Health KPI image to clipboard"
 aria-label="Copy Cell Health KPI image to clipboard"
 style="margin:0;padding:2px 7px 4px 7px;font-size:1.05rem;line-height:1;cursor:pointer;background:transparent;color:{acc};border:2px solid {acc};border-radius:2px;">📋</button>
<script>
(function() {{
  const b64 = "{b64_png}";
  const btn = document.getElementById('nwstChCopy');
  btn.addEventListener('click', async function () {{
    try {{
      const blob = await (await fetch('data:image/png;base64,' + b64)).blob();
      await navigator.clipboard.write([new ClipboardItem({{ 'image/png': blob }})]);
      const prev = btn.innerHTML;
      btn.innerHTML = '✓';
      btn.disabled = true;
      setTimeout(function () {{ btn.innerHTML = prev; btn.disabled = false; }}, 1400);
    }} catch (e) {{
      window.alert('Could not copy image: ' + (e && e.message ? e.message : String(e)));
    }}
  }});
}})();
</script>
</body></html>"""


def _nwst_normalize_member_name(s):
    """Strip, lowercase, collapse spaces (and NBSP) for matching Attendance ↔ CG Combined."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    t = str(s).replace("\u00a0", " ").strip().lower()
    return " ".join(t.split())


def _nwst_detect_name_cell_columns_for_grid(header_row, sample_row):
    """Return (name_col_idx, sheet_cell_col_idx_or_None). Mirrors wide grids where Name is in B not A."""
    hr = [str(x).strip() if x is not None else "" for x in (header_row or [])]
    sr = []
    if sample_row:
        for i in range(max(len(header_row or []), len(sample_row))):
            v = sample_row[i] if i < len(sample_row) else ""
            sr.append(str(v).strip() if v is not None else "")

    h0s = hr[0].lower() if hr else ""
    h1s = hr[1].lower() if len(hr) > 1 else ""
    h2s = hr[2].lower() if len(hr) > 2 else ""

    # Snapshot layout: A="Date" (combined Name - Cell), B="Name", C="Cell", D+= week columns
    if h0s == "date" and "name" in h1s and ("cell" in h2s or "group" in h2s):
        return 1, 2

    # Prefer explicit "Name" / "Member" header (any column)
    for i, c in enumerate(hr):
        csl = c.lower()
        if "timestamp" in csl:
            continue
        if (
            ("name" in csl or csl in ("member", "full name"))
            and "last" not in csl
            and "leader" not in csl
        ):
            if i < len(header_row) and parse_attendance_column_date(header_row[i]) is None:
                cell_i = None
                for j in range(i + 1, min(len(hr), i + 5)):
                    if parse_attendance_column_date(header_row[j]) is not None:
                        break
                    jl = hr[j].lower()
                    if jl and any(k in jl for k in ("cell", "group", "cg")):
                        cell_i = j
                        break
                return i, cell_i

    h0 = h0s

    if h0 and "timestamp" in h0:
        # A = Timestamp, expect B = Name; C may be Cell before date columns
        name_i = 1
        cell_i = None
        if len(hr) > 2 and hr[2] and parse_attendance_column_date(hr[2]) is None:
            h2 = hr[2].lower()
            if any(k in h2 for k in ("cell", "group", "cg", "zone")):
                cell_i = 2
        return name_i, cell_i

    h1 = hr[1].lower() if len(hr) > 1 else ""
    samp0 = sr[0] if sr else ""
    samp1 = sr[1] if len(sr) > 1 else ""

    # Empty col A but populated B — classic "Name" in column B
    if not samp0 and samp1:
        if any(x in h1 for x in ("name", "member", "full")) or (len(hr) > 1 and hr[1] and not hr[0]):
            cell_i = None
            if len(hr) > 2 and hr[2] and parse_attendance_column_date(hr[2]) is None:
                h2 = hr[2].lower()
                if any(k in h2 for k in ("cell", "group", "cg")):
                    cell_i = 2
            return 1, cell_i

    # Header row says "Name" in second column
    if (
        h1
        and any(x in h1 for x in ("name", "member"))
        and h0 not in ("name", "member", "full name")
    ):
        cell_i = None
        if len(hr) > 2 and hr[2] and parse_attendance_column_date(hr[2]) is None:
            h2 = hr[2].lower()
            if any(k in h2 for k in ("cell", "group", "cg")):
                cell_i = 2
        return 1, cell_i

    return 0, None


def _nwst_sheet_api_transient(api_err):
    """True when retrying the same read may succeed (rate limit / Google blips)."""
    code = getattr(api_err.response, "status_code", None) or getattr(api_err, "code", None)
    return code in {429, 500, 502, 503, 504}


@st.cache_data(ttl=300)
def nwst_get_attendance_grid_for_charts(sheet_id):
    """Load **Attendance** tab — Saturday columns only; cell from sheet or **CG Combined** name lookup."""
    redis = get_redis_client()
    if redis:
        try:
            cached_raw = redis.get(NWST_REDIS_ATTENDANCE_CHART_GRID_KEY)
            if cached_raw:
                payload = json.loads(cached_raw)
                df = pd.DataFrame(payload["rows"], columns=payload["columns"])
                dates = payload.get("saturday_dates_short") or []
                return df, dates, None
        except Exception:
            pass

    client = get_google_sheet_client()
    if not client:
        return None, [], "Could not connect to Google Sheets."

    transient_attempts = 3
    for attempt in range(transient_attempts):
        try:
            spreadsheet = client.open_by_key(sheet_id)
            try:
                att_sheet = spreadsheet.worksheet(NWST_ATTENDANCE_TAB)
            except WorksheetNotFound:
                return None, [], f"Tab '{NWST_ATTENDANCE_TAB}' not found."

            try:
                cg_sheet = spreadsheet.worksheet("CG Combined")
            except WorksheetNotFound:
                return None, [], "Tab 'CG Combined' not found."

            all_values = att_sheet.get_all_values()
            cg_vals = cg_sheet.get_all_values()
            if len(all_values) < 2:
                return None, [], "No data in Attendance."
            if len(cg_vals) < 2:
                return None, [], "No data in CG Combined."

            cg_df = pd.DataFrame(cg_vals[1:], columns=cg_vals[0])
            cg_name_col, cg_cell_col = _resolve_cg_name_cell_columns(cg_df)
            if cg_cell_col is None:
                for col in cg_df.columns:
                    cl = str(col).lower().strip()
                    if ("cell" in cl or "group" in cl) and "leader" not in cl:
                        cg_cell_col = col
                        break

            header_row = all_values[0]
            sample_row = all_values[1] if len(all_values) > 1 else []
            name_col_idx, sheet_cell_col_idx = _nwst_detect_name_cell_columns_for_grid(
                header_row, sample_row
            )

            saturday_entries = []
            for col_idx in range(len(header_row)):
                if col_idx in (name_col_idx, sheet_cell_col_idx):
                    continue
                h = header_row[col_idx]
                d = parse_attendance_column_date(h)
                if d is None or d.weekday() != 5:
                    continue
                saturday_entries.append((d, col_idx))

            if not saturday_entries:
                return None, [], (
                    "No Saturday columns found in **Attendance** row 1. "
                    "Headers must be parseable dates (same as Monthly Health)."
                )

            saturday_entries.sort(key=lambda x: x[0])
            saturday_dates_short = [d.strftime("%d %b %Y") for d, _ in saturday_entries]
            col_indices = [idx for _, idx in saturday_entries]

            def _nwst_attendance_present(cell_val):
                if cell_val is None or (isinstance(cell_val, float) and pd.isna(cell_val)):
                    return False
                s = str(cell_val).strip().lower()
                if s in ("1", "yes", "y", "true", "x"):
                    return True
                try:
                    return int(float(str(cell_val).strip())) == 1
                except (TypeError, ValueError):
                    return False

            def _cell_from_cg(person_name):
                k = _nwst_normalize_member_name(person_name)
                if not k or not cg_name_col or not cg_cell_col:
                    return ""
                cg_match = cg_df[
                    cg_df[cg_name_col]
                    .astype(str)
                    .map(_nwst_normalize_member_name)
                    == k
                ]
                if cg_match.empty:
                    return ""
                return str(cg_match.iloc[0][cg_cell_col]).strip()

            data_rows = []

            for row in all_values[1:]:
                if not row:
                    continue
                if name_col_idx >= len(row):
                    continue
                name = str(row[name_col_idx]).strip() if row[name_col_idx] else ""
                if name.lower() == "name":
                    continue

                cell_group = ""
                if sheet_cell_col_idx is not None and sheet_cell_col_idx < len(row):
                    cell_group = str(row[sheet_cell_col_idx]).strip()

                combined_a = str(row[0]).strip() if row and len(row) > 0 and row[0] else ""
                if (" - " in combined_a) and (not name or not cell_group):
                    left, right = combined_a.split(" - ", 1)
                    if not name:
                        name = left.strip()
                    if not cell_group:
                        cell_group = right.strip()

                if not name:
                    continue

                if not cell_group:
                    cell_group = _cell_from_cg(name)
                if not cell_group:
                    continue

                attendance = {
                    saturday_dates_short[j]: (
                        1
                        if (
                            col_indices[j] < len(row)
                            and _nwst_attendance_present(row[col_indices[j]])
                        )
                        else 0
                    )
                    for j in range(len(col_indices))
                }
                data_rows.append(
                    {
                        "Name": name,
                        "Cell Group": cell_group,
                        "Name - Cell Group": f"{name} - {cell_group}",
                        **attendance,
                    }
                )

            if not data_rows:
                return None, [], (
                    "No attendance rows matched **CG Combined** (or a Cell column on Attendance). "
                    "Check Name is in column A or B, cell on sheet or same spellings as CG Combined."
                )

            df = pd.DataFrame(data_rows)
            df = df.drop_duplicates(subset=["Name - Cell Group"], keep="first")
            redis = get_redis_client()
            if redis:
                try:
                    payload = {
                        "columns": df.columns.tolist(),
                        "rows": df.values.tolist(),
                        "saturday_dates_short": saturday_dates_short,
                    }
                    redis.set(
                        NWST_REDIS_ATTENDANCE_CHART_GRID_KEY,
                        json.dumps(payload, default=str),
                        ex=300,
                    )
                except Exception:
                    pass
            return df, saturday_dates_short, None
        except APIError as e:
            if attempt < transient_attempts - 1 and _nwst_sheet_api_transient(e):
                time.sleep(1.0 * (2**attempt))
                continue
            hint = (
                " (Google Sheets often returns this briefly; wait a minute and use **Sync**, "
                "or check https://www.google.com/appsstatus )"
                if _nwst_sheet_api_transient(e)
                else ""
            )
            return None, [], f"Error loading Attendance for charts: {str(e)}{hint}"
        except Exception as e:
            return None, [], f"Error loading Attendance for charts: {str(e)}"


@st.cache_data(ttl=300)
def nwst_get_cell_zone_mapping(sheet_id):
    """Cell (col A) → zone (col C) from Key Values."""
    client = get_google_sheet_client()
    if not client:
        return {}
    try:
        spreadsheet = client.open_by_key(sheet_id)
        try:
            key_values_sheet = spreadsheet.worksheet(NWST_KEY_VALUES_TAB)
        except WorksheetNotFound:
            return {}
        all_values = key_values_sheet.get_all_values()
        if len(all_values) <= 1:
            return {}
        cell_to_zone = {}
        for row in all_values[1:]:
            if len(row) >= 3:
                cn = row[0].strip()
                zn = row[2].strip()
                if cn and zn:
                    cell_to_zone[cn.lower()] = zn
        return cell_to_zone
    except Exception:
        return {}


def parse_name_cell_group(name_cell_group_str):
    """Parse 'Name - Cell Group' format and return (name, cell_group)."""
    if not name_cell_group_str:
        return None, None
    parts = name_cell_group_str.split(" - ", 1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return parts[0].strip(), "Unknown"


@st.cache_data(ttl=300)
def nwst_get_options_roster_members(_sheet_id):
    """Cell roster sizes from Options tab column C (same format as CHECK IN attendance app)."""
    client = get_google_sheet_client()
    if not client:
        return {}, "Could not connect to Google Sheets."
    try:
        spreadsheet = client.open_by_key(_sheet_id)
        try:
            options_sheet = spreadsheet.worksheet(NWST_OPTIONS_TAB)
        except WorksheetNotFound:
            return {}, f"Tab '{NWST_OPTIONS_TAB}' not found."
        column_c_values = options_sheet.col_values(3)
        if not column_c_values:
            return {}, "Column C in Options sheet is empty."
        members_per_cell = {}
        for value in column_c_values[1:]:
            value = (value or "").strip()
            if not value:
                continue
            m_name, m_cell = parse_name_cell_group(value)
            if m_name and m_cell:
                members_per_cell.setdefault(m_cell, set()).add(m_name)
        if not members_per_cell:
            return {}, "No roster entries found in column C (from row 2)."
        return {k: len(v) for k, v in members_per_cell.items()}, None
    except Exception as e:
        return {}, str(e)


@st.cache_data(ttl=300)
def nwst_get_attendance_analytics_data(_sheet_id):
    """Fetch Saturday-only attendance matrix from the 'Attendance Analytics' tab (CHECK IN format)."""
    client = get_google_sheet_client()
    if not client:
        return None, [], "Could not connect to Google Sheets."
    try:
        spreadsheet = client.open_by_key(_sheet_id)
        try:
            analytics_sheet = spreadsheet.worksheet(NWST_ATTENDANCE_ANALYTICS_TAB)
        except WorksheetNotFound:
            return None, [], f"Tab '{NWST_ATTENDANCE_ANALYTICS_TAB}' not found in the Google Sheet."

        all_values = analytics_sheet.get_all_values()
        if len(all_values) < 2:
            return None, [], "No data found in the Attendance Analytics sheet."

        header_row = all_values[0]
        dates = []
        saturday_col_indices = []

        for col_idx, cell in enumerate(header_row[3:], start=3):
            if not cell or not str(cell).strip():
                continue
            cell_s = str(cell).strip()
            try:
                date_obj = datetime.strptime(cell_s, "%m/%d/%Y")
                if date_obj.weekday() == 5:
                    dates.append(date_obj)
                    saturday_col_indices.append(col_idx)
            except ValueError:
                try:
                    date_obj = datetime.strptime(cell_s, "%d/%m/%Y")
                    if date_obj.weekday() == 5:
                        dates.append(date_obj)
                        saturday_col_indices.append(col_idx)
                except ValueError:
                    continue

        if not dates:
            return None, [], "No Saturday dates found in the analytics data."

        sorted_pairs = sorted(zip(dates, saturday_col_indices), key=lambda x: x[0])
        dates = [pair[0] for pair in sorted_pairs]
        saturday_col_indices = [pair[1] for pair in sorted_pairs]
        saturday_dates_short = [d.strftime("%b %d") for d in dates]

        data_rows = []
        for row in all_values[1:]:
            if len(row) < 3:
                continue
            name = row[1].strip() if len(row) > 1 and row[1] else ""
            cell_group = row[2].strip() if len(row) > 2 and row[2] else ""
            if not name or name.lower() == "name":
                continue
            attendance = []
            for col_idx in saturday_col_indices:
                if col_idx < len(row):
                    val = row[col_idx].strip()
                    attendance.append(1 if val == "1" else 0)
                else:
                    attendance.append(0)
            data_rows.append({
                "Name": name,
                "Cell Group": cell_group,
                "Name - Cell Group": f"{name} - {cell_group}" if cell_group else name,
                **{saturday_dates_short[i]: attendance[i] for i in range(len(attendance))},
            })

        if not data_rows:
            return None, [], "No attendance records found."

        df = pd.DataFrame(data_rows)
        df = df.drop_duplicates(subset=["Name - Cell Group"], keep="first")
        return df, saturday_dates_short, None
    except Exception as e:
        return None, [], f"Error fetching analytics data: {str(e)}"


def _nwst_resolve_display_name_cell_cols(display_df):
    disp_name_col = None
    disp_cell_col = None
    for col in display_df.columns:
        col_lower = col.lower().strip()
        if col_lower in ["cell", "group"]:
            disp_cell_col = col
        if col_lower in ["name", "member name", "member"] or (
            any(x in col_lower for x in ["name", "member"]) and "last" not in col_lower
        ):
            if disp_name_col is None:
                disp_name_col = col
    if not disp_name_col:
        disp_name_col = display_df.columns[0]
    return disp_name_col, disp_cell_col


def _nwst_zone_for_cell_map(cg, cell_to_zone_map):
    return cell_to_zone_map.get(str(cg).lower(), cg) if cg else "Unknown"


def _nwst_exclude_rate_chart_cell(cg, zone_name):
    if not str(cg).strip():
        return True
    if str(zone_name).strip().lower() == "archive":
        return True
    n = str(cg).strip().lower().lstrip("*").strip()
    if n == "not sure yet" or n.startswith("not sure yet"):
        return True
    return False


def _nwst_weekly_contrasting_line_colors(primary_hex, n_series):
    """Distinct line colors anchored on the hue **opposite** this week's primary (Saturday‑locked accent).

    Multiple series step around the wheel (golden‑ratio hue steps) so lines stay separable on dark UI.
    """
    if n_series < 1:
        n_series = 1
    ph = str(primary_hex or "#888888").lstrip("#")
    if len(ph) != 6 or not all(c in "0123456789abcdefABCDEF" for c in ph):
        ph = "888888"
    r = int(ph[0:2], 16) / 255.0
    g = int(ph[2:4], 16) / 255.0
    b = int(ph[4:6], 16) / 255.0
    h, light, sat = colorsys.rgb_to_hls(r, g, b)
    h_comp = (h + 0.5) % 1.0
    phi = 0.618033988749895
    out = []
    for i in range(n_series):
        hi = (h_comp + i * phi) % 1.0
        li = min(0.78, max(0.48, 0.52 + (i % 4) * 0.05))
        si = min(1.0, max(0.72, 0.78 + (1.0 - sat) * 0.15))
        r2, g2, b2 = colorsys.hls_to_rgb(hi, li, si)
        out.append(
            "#{:02x}{:02x}{:02x}".format(
                int(max(0, min(255, round(r2 * 255)))),
                int(max(0, min(255, round(g2 * 255)))),
                int(max(0, min(255, round(b2 * 255)))),
            )
        )
    return out


def _nwst_ui_line_palette(primary_hex, n_series):
    """Distinct lines in the same hue family as the app primary (matches Streamlit accent, not complement)."""
    if n_series < 1:
        n_series = 1
    ph = str(primary_hex or "#888888").lstrip("#")
    if len(ph) != 6 or not all(c in "0123456789abcdefABCDEF" for c in ph):
        ph = "888888"
    r = int(ph[0:2], 16) / 255.0
    g = int(ph[2:4], 16) / 255.0
    b = int(ph[4:6], 16) / 255.0
    h, light, sat = colorsys.rgb_to_hls(r, g, b)
    out = []
    for i in range(n_series):
        if n_series == 1:
            li = min(0.82, max(0.42, light))
            si = min(1.0, max(0.62, sat))
        else:
            t = i / max(1, n_series - 1)
            li = 0.40 + t * 0.36
            si = min(1.0, max(0.55, sat * (0.9 + 0.1 * (i % 3))))
        r2, g2, b2 = colorsys.hls_to_rgb(h, li, si)
        out.append(
            "#{:02x}{:02x}{:02x}".format(
                int(max(0, min(255, round(r2 * 255)))),
                int(max(0, min(255, round(g2 * 255)))),
                int(max(0, min(255, round(b2 * 255)))),
            )
        )
    return out


NWST_ATTENDED_CELL_MEMBERS_COL = "Attended cell members"
# 0 = use every Saturday column from Attendance (widest time window on the chart).
NWST_SERVICE_ATTENDANCE_CHART_MAX_WEEKS = 0


def _nwst_count_y_axis_range(plot_df):
    """Vertical span from padded data min/max so counts use most of the chart (less empty space below)."""
    col = NWST_ATTENDED_CELL_MEMBERS_COL
    if plot_df.empty or col not in plot_df.columns:
        return 0.0, 5.0
    s = plot_df[col].astype(float)
    data_min = float(s.min())
    data_max = float(s.max())
    if data_max <= 0:
        return 0.0, 5.0
    span = data_max - data_min
    if span <= 1e-9:
        span = max(2.0, max(1.0, data_max) * 0.25)
    pad_below = max(0.5, span * 0.1)
    pad_above = max(0.75, span * 0.12)
    y_lo = max(0.0, data_min - pad_below)
    y_hi = data_max + pad_above
    return y_lo, y_hi


def _nwst_attendance_data_min_max_int(plot_df):
    col = NWST_ATTENDED_CELL_MEMBERS_COL
    if plot_df.empty or col not in plot_df.columns:
        return 0, 0
    s = plot_df[col].astype(float)
    return int(s.min()), int(s.max())


def _nwst_attendance_y_tick_labels(tickvals):
    """Whole-number y-axis tick text (no min/max annotations)."""
    return [str(int(v)) for v in tickvals]


def _nwst_make_attendance_rate_fig(
    plot_df, date_cols, colors, daily_colors, y_axis_range=None
):
    """Minimal line chart: attended cell members per Saturday (one line per cell group).

    ``y_axis_range`` — optional ``(y_lo, y_hi)`` for fixed vertical span (e.g. same scale
    across per-cell tabs when Cell filter is All). Tick labels stay derived from ``plot_df``
    (that cell's min, max, mean).
    """
    plot_df = plot_df.copy()
    if NWST_ATTENDED_CELL_MEMBERS_COL not in plot_df.columns:
        plot_df[NWST_ATTENDED_CELL_MEMBERS_COL] = 0

    n_lines = int(plot_df["Cell Group"].nunique())
    line_colors = _nwst_ui_line_palette(daily_colors["primary"], max(n_lines, 1))
    cell_order = sorted(plot_df["Cell Group"].unique(), key=str.lower)
    cg_to_color = {cg: line_colors[i] for i, cg in enumerate(cell_order)}

    if y_axis_range is not None:
        y_lo, y_hi = y_axis_range
    else:
        y_lo, y_hi = _nwst_count_y_axis_range(plot_df)
    y_mean = float(plot_df[NWST_ATTENDED_CELL_MEMBERS_COL].mean())
    fig = go.Figure()
    plot_bg = colors["background"]
    paper_bg = colors["card_bg"]

    for cg in cell_order:
        sub = plot_df[plot_df["Cell Group"] == cg]
        c = cg_to_color[cg]
        fig.add_trace(
            go.Scatter(
                x=sub["Saturday"],
                y=sub[NWST_ATTENDED_CELL_MEMBERS_COL],
                name=str(cg),
                legendgroup=str(cg),
                mode="lines",
                line=dict(width=2, color=c, shape="spline", smoothing=1.2),
                hovertemplate=(
                    "<b>%{fullData.name}</b><br>%{x}<br>"
                    "<b>%{y:.0f}</b> attended<extra></extra>"
                ),
            )
        )

    fig.add_hline(
        y=y_mean,
        line_width=1,
        line_dash="dash",
        line_color=colors["text_muted"],
        opacity=0.85,
        layer="below",
    )

    if len(date_cols) > 1:
        x_tickvals = [date_cols[0], date_cols[-1]]
        x_ticktext = [date_cols[0], date_cols[-1]]
    else:
        x_tickvals = list(date_cols)
        x_ticktext = list(date_cols)

    line_muted = "rgba(255,255,255,0.22)"
    fig.update_layout(
        height=260,
        legend_title_text="",
        plot_bgcolor=plot_bg,
        paper_bgcolor=paper_bg,
        font=dict(family="Inter, sans-serif", size=11, color=colors["text"]),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.22,
            xanchor="center",
            x=0.5,
            font=dict(size=10, color=colors["text_muted"], family="Inter"),
            bgcolor="rgba(0,0,0,0)",
            borderwidth=0,
        ),
        hovermode="x unified",
        hoverdistance=24,
        spikedistance=-1,
        hoverlabel=dict(
            bgcolor="#2a2a2a",
            font=dict(size=12, color=colors["text"], family="Inter"),
            bordercolor="rgba(255,255,255,0.2)",
            align="left",
        ),
        margin=dict(l=48, r=12, t=6, b=56),
    )
    fig.update_xaxes(
        title=dict(text=""),
        tickfont=dict(color=colors["text_muted"], family="Inter", size=10),
        showgrid=False,
        zeroline=False,
        linecolor=line_muted,
        linewidth=1,
        mirror=False,
        tickmode="array",
        tickvals=x_tickvals,
        ticktext=x_ticktext,
        categoryorder="array",
        categoryarray=date_cols,
        tickangle=0,
        showspikes=True,
        spikecolor="rgba(255,255,255,0.9)",
        spikesnap="cursor",
        spikemode="across",
        spikethickness=1,
        spikedash="solid",
    )
    data_min_i, data_max_i = _nwst_attendance_data_min_max_int(plot_df)
    lo_i, hi_i = max(0, data_min_i), max(0, data_max_i)
    mean_i = max(0, int(round(y_mean)))
    y_tickvals = sorted({lo_i, hi_i, mean_i})
    fig.update_yaxes(
        title=dict(
            text="Attended cell members",
            font=dict(size=10, color=colors["text_muted"]),
        ),
        tickfont=dict(color=colors["text_muted"], family="Inter", size=10),
        showgrid=False,
        zeroline=False,
        linecolor=line_muted,
        linewidth=1,
        range=[y_lo, y_hi],
        tickmode="array",
        tickvals=y_tickvals,
        ticktext=_nwst_attendance_y_tick_labels(y_tickvals),
    )
    return fig


def render_nwst_service_attendance_rate_charts(display_df, daily_colors, tab_each_cell_when_all=False):
    """Per-zone Saturday attendance (headcount lines) — filtered by current display_df (global Cell / Status).

    When ``tab_each_cell_when_all`` is True and multiple cell groups are shown, each cell gets its own tab
    instead of stacking tall charts.
    """
    if display_df is None or display_df.empty:
        return

    disp_name_col, disp_cell_col = _nwst_resolve_display_name_cell_cols(display_df)
    if not disp_cell_col:
        st.info("Add a Cell / Group column to CG data to show attendance rate by cell charts.")
        return

    ana_df, date_cols, err = nwst_get_attendance_grid_for_charts(NWST_HEALTH_SHEET_ID)
    if err:
        st.warning(err)
        return
    if ana_df is None or ana_df.empty or not date_cols:
        st.info("No Attendance sheet data to chart (need Saturday date columns from column D).")
        return

    cell_to_zone_map = nwst_get_cell_zone_mapping(NWST_HEALTH_SHEET_ID)

    keys = display_df[[disp_name_col, disp_cell_col]].copy()
    keys["_n"] = keys[disp_name_col].astype(str).str.strip()
    keys["_c"] = keys[disp_cell_col].astype(str).str.strip()
    keys = keys[["_n", "_c"]].drop_duplicates()

    work = ana_df.copy()
    work["_n"] = work["Name"].astype(str).str.strip()
    work["_c"] = work["Cell Group"].astype(str).str.strip()
    work_df = work.merge(keys, on=["_n", "_c"], how="inner").drop(columns=["_n", "_c"])

    if work_df.empty:
        st.info(
            "No matching rows between **Attendance** (Saturday 0/1) and the filtered member list. "
            "Check names and cells align with CG Combined."
        )
        return

    _mdf = display_df.dropna(subset=[disp_cell_col, disp_name_col]).copy()
    _mdf["_c"] = _mdf[disp_cell_col].astype(str).str.strip()
    members_per_cell = _mdf.groupby("_c")[disp_name_col].nunique().to_dict()

    colors = {
        "primary": daily_colors["primary"],
        "background": daily_colors["background"],
        "card_bg": "#1a1a1a",
        "text": "#ffffff",
        "text_muted": "#999999",
    }

    zone_to_cells = defaultdict(list)
    for cg in sorted(work_df["Cell Group"].dropna().unique(), key=str.lower):
        z = _nwst_zone_for_cell_map(cg, cell_to_zone_map)
        if _nwst_exclude_rate_chart_cell(cg, z):
            continue
        zone_to_cells[z].append(cg)

    chart_date_cols = list(date_cols)
    if (
        NWST_SERVICE_ATTENDANCE_CHART_MAX_WEEKS
        and len(chart_date_cols) > NWST_SERVICE_ATTENDANCE_CHART_MAX_WEEKS
    ):
        chart_date_cols = chart_date_cols[-NWST_SERVICE_ATTENDANCE_CHART_MAX_WEEKS :]

    zone_plots = {}
    for zone in sorted(zone_to_cells.keys(), key=str.lower):
        cells = sorted(zone_to_cells[zone], key=str.lower)
        long_rows = []
        for cg in cells:
            sub = work_df[work_df["Cell Group"] == cg]
            mc = members_per_cell.get(str(cg).strip(), 0)
            if mc == 0 and not sub.empty:
                mc = sub["Name"].nunique()
            if mc == 0:
                continue
            for dc in chart_date_cols:
                attended = int(sub[dc].sum()) if dc in sub.columns else 0
                pct = 100.0 * attended / mc
                long_rows.append(
                    {
                        "Saturday": dc,
                        "Cell Group": cg,
                        "Attendance rate %": round(pct, 1),
                        NWST_ATTENDED_CELL_MEMBERS_COL: attended,
                    }
                )
        plot_df = pd.DataFrame(long_rows)
        if plot_df.empty:
            continue
        zone_plots[zone] = plot_df

    if not zone_plots:
        st.info("No cells to chart after filters.")
        return

    if tab_each_cell_when_all:
        cell_entries = []
        for zone in sorted(zone_to_cells.keys(), key=str.lower):
            for cg in sorted(zone_to_cells[zone], key=str.lower):
                sub = work_df[work_df["Cell Group"] == cg]
                mc = members_per_cell.get(str(cg).strip(), 0)
                if mc == 0 and not sub.empty:
                    mc = sub["Name"].nunique()
                if mc == 0:
                    continue
                long_rows = []
                for dc in chart_date_cols:
                    attended = int(sub[dc].sum()) if dc in sub.columns else 0
                    pct = 100.0 * attended / mc
                    long_rows.append(
                        {
                            "Saturday": dc,
                            "Cell Group": cg,
                            "Attendance rate %": round(pct, 1),
                            NWST_ATTENDED_CELL_MEMBERS_COL: attended,
                        }
                    )
                plot_df_one = pd.DataFrame(long_rows)
                if plot_df_one.empty:
                    continue
                cell_entries.append((cg, plot_df_one))
        cell_entries.sort(key=lambda x: str(x[0]).lower())
        if len(cell_entries) > 1:
            _combined_tab_range = pd.concat(
                [df for _, df in cell_entries], ignore_index=True
            )
            _y_shared_lo, _y_shared_hi = _nwst_count_y_axis_range(_combined_tab_range)
            _shared_range = (_y_shared_lo, _y_shared_hi)
            _tab_labels = [str(cg) for cg, _ in cell_entries]
            _cg_tabs = st.tabs(_tab_labels)
            for _i, (_, plot_df_one) in enumerate(cell_entries):
                with _cg_tabs[_i]:
                    fig = _nwst_make_attendance_rate_fig(
                        plot_df_one,
                        chart_date_cols,
                        colors,
                        daily_colors,
                        y_axis_range=_shared_range,
                    )
                    st.plotly_chart(fig, use_container_width=True)
            return

    zone_tab_names = sorted(zone_plots.keys(), key=str.lower)
    for zone in zone_tab_names:
        plot_df = zone_plots[zone]
        if len(zone_tab_names) > 1:
            st.markdown(
                f"<p style='color: {daily_colors['primary']}; font-weight: 700; font-size: 1rem; margin: 0.75rem 0 0.35rem 0;'>"
                f"{zone}</p>",
                unsafe_allow_html=True,
            )
        fig = _nwst_make_attendance_rate_fig(plot_df, chart_date_cols, colors, daily_colors)
        st.plotly_chart(fig, use_container_width=True)


def _resolve_cg_name_cell_columns(cg_df):
    cg_name_col = None
    cg_cell_col = None
    for col in cg_df.columns:
        if col.lower().strip() in ['name', 'member name', 'member']:
            cg_name_col = col
        if col.lower().strip() in ['cell', 'group']:
            cg_cell_col = col
    if not cg_name_col:
        cg_name_col = cg_df.columns[0]
    return cg_name_col, cg_cell_col


def _compute_attendance_stats_from_frames(att_df, cg_df):
    """Build attendance_stats dict (Name + Cell key) from raw sheet frames."""
    attendance_stats = {}
    cg_name_col, cg_cell_col = _resolve_cg_name_cell_columns(cg_df)
    att_name_col = att_df.columns[0] if len(att_df.columns) > 0 else None

    if not att_name_col:
        return attendance_stats

    for att_name in att_df[att_name_col].unique():
        if pd.isna(att_name) or att_name == "":
            continue

        att_name_str = str(att_name).strip()
        member_att_data = att_df[att_df[att_name_col] == att_name]

        attendance_count = 0
        total_services = 0

        for col_idx, col in enumerate(att_df.columns):
            if col_idx >= 3:
                total_services += 1
                values = member_att_data[col].values
                if len(values) > 0 and str(values[0]).strip() == "1":
                    attendance_count += 1

        cell_info = ""
        if cg_name_col and cg_cell_col:
            cg_match = cg_df[cg_df[cg_name_col].str.strip().str.lower() == att_name_str.lower()]
            if not cg_match.empty:
                cell_info = " - " + str(cg_match[cg_cell_col].iloc[0]).strip()

        if total_services > 0:
            key = att_name_str + cell_info
            attendance_stats[key] = {
                "attendance": attendance_count,
                "total": total_services,
                "percentage": round(attendance_count / total_services * 100) if total_services > 0 else 0,
            }

    return attendance_stats


@st.cache_data(ttl=300)
def get_attendance_data():
    """Load attendance rollup from Redis cache or recompute from Attendance + CG Combined."""
    redis = get_redis_client()
    if redis:
        try:
            cached_data = redis.get("nwst_attendance_stats")
            if cached_data:
                return json.loads(cached_data)
        except Exception:
            pass

    att_df, cg_df = load_attendance_and_cg_dataframes()
    if att_df is None or cg_df is None:
        return {}

    attendance_stats = _compute_attendance_stats_from_frames(att_df, cg_df)

    if redis:
        try:
            redis.set("nwst_attendance_stats", json.dumps(attendance_stats), ex=300)
        except Exception:
            pass

    return attendance_stats


def get_attendance_text(name, cell, attendance_stats):
    """Attendance summary for tooltips (Name + Cell key), or name only if unknown."""
    if not attendance_stats:
        return name

    name_stripped = str(name).strip()
    cell_stripped = str(cell).strip() if cell else ""

    if cell_stripped:
        key = f"{name_stripped} - {cell_stripped}"
    else:
        key = name_stripped

    if key in attendance_stats:
        stats = attendance_stats[key]
        return f"{name} - {stats['attendance']}/{stats['total']} ({stats['percentage']}%)"

    key_lower = key.lower()
    for dict_key, stats in attendance_stats.items():
        if dict_key.lower() == key_lower:
            return f"{name} - {stats['attendance']}/{stats['total']} ({stats['percentage']}%)"

    return name


def categorize_member_status(attendance_count, total_possible):
    """Categorize member as Regular, Irregular, or Follow Up based on attendance."""
    if attendance_count >= (total_possible * 0.75):  # 75% and above attendance = Regular
        return "Regular"
    elif attendance_count > 0:  # Below 75% = Irregular
        return "Irregular"
    else:  # 0% attendance = Follow Up
        return "Follow Up"


def _qp_first(val, default="cg"):
    """Normalize ``st.query_params`` values (string or single-element list)."""
    if val is None:
        return default
    if isinstance(val, list):
        return val[0] if val else default
    return str(val)


def extract_cell_sheet_status_type(status_val):
    """Same labels as CELL HEALTH member tiles (Status column prefixes on the sheet)."""
    if isinstance(status_val, str):
        if status_val.startswith("Regular:"):
            return "Regular"
        if status_val.startswith("Irregular:"):
            return "Irregular"
        if status_val.startswith("New"):
            return "New"
        if status_val.startswith("Follow Up:"):
            return "Follow Up"
        if status_val.startswith("Red:"):
            return "Red"
        if status_val.startswith("Graduated:"):
            return "Graduated"
    return None


def parse_attendance_column_date(cell_val):
    """Parse a single Attendance sheet header cell into a date, or None."""
    if cell_val is None or (isinstance(cell_val, float) and pd.isna(cell_val)):
        return None
    s = str(cell_val).strip()
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_status_historical_month_header(cell_val):
    """Parse **Status Historical** column headers like 'Jan 2026' into (year, month), or None."""
    if cell_val is None or (isinstance(cell_val, float) and pd.isna(cell_val)):
        return None
    s = str(cell_val).strip()
    if not s:
        return None
    for fmt in ("%b %Y", "%B %Y", "%b %y", "%B %y", "%Y-%m", "%m/%Y", "%Y/%m"):
        try:
            dt = datetime.strptime(s, fmt)
            return (dt.year, dt.month)
        except ValueError:
            continue
    return None


def _resolve_status_historical_name_columns(df):
    """Detect composite (Name - Cell), Name, and Cell columns on Status Historical."""
    composite_col = name_col = cell_col = None
    for c in df.columns:
        s = str(c).strip()
        sl = s.lower().replace("-", " ")
        if composite_col is None and "name" in sl and ("cell" in sl or "group" in sl):
            composite_col = c
        elif name_col is None and sl in ("name", "member", "member name", "full name"):
            name_col = c
        elif cell_col is None and sl in ("cell name", "cell", "group", "cg", "cell/group"):
            cell_col = c
        elif cell_col is None and "cell" in sl and "name" not in sl:
            cell_col = c
    if name_col is None and len(df.columns) >= 2:
        c0 = df.columns[0]
        if composite_col == c0 and len(df.columns) >= 3:
            name_col, cell_col = df.columns[1], df.columns[2]
        elif composite_col is None:
            name_col = df.columns[0]
            if len(df.columns) >= 2:
                cell_col = df.columns[1]
    return composite_col, name_col, cell_col


def _status_historical_row_norm_keys(row, composite_col, name_col, cell_col):
    keys = []
    if composite_col is not None and composite_col in row.index:
        v = row.get(composite_col)
        if v is not None and not (isinstance(v, float) and pd.isna(v)) and str(v).strip():
            keys.append(_nwst_normalize_member_name(str(v).strip()))
    n, c = "", ""
    if name_col is not None and name_col in row.index:
        nv = row.get(name_col)
        if nv is not None and not (isinstance(nv, float) and pd.isna(nv)):
            n = str(nv).strip()
    if cell_col is not None and cell_col in row.index:
        cv = row.get(cell_col)
        if cv is not None and not (isinstance(cv, float) and pd.isna(cv)):
            c = str(cv).strip()
    if n and c:
        keys.append(_nwst_normalize_member_name(f"{n} - {c}"))
    if n:
        keys.append(_nwst_normalize_member_name(n))
    return keys


def _parse_status_historical_for_monthly(status_hist_df):
    """Build lookup and month axis from Status Historical, or None if unusable."""
    if status_hist_df is None or status_hist_df.empty:
        return None
    composite_col, name_col, cell_col = _resolve_status_historical_name_columns(status_hist_df)
    if not name_col:
        return None
    myt_today = datetime.now(timezone(timedelta(hours=8))).date()
    cur_ym = (myt_today.year, myt_today.month)
    ym_to_col = {}
    for col in status_hist_df.columns:
        ym = parse_status_historical_month_header(col)
        if ym and ym <= cur_ym and ym not in ym_to_col:
            ym_to_col[ym] = col
    if not ym_to_col:
        return None
    month_keys = sorted(ym_to_col.keys())
    lookup = {}
    for _, row in status_hist_df.iterrows():
        for nk in _status_historical_row_norm_keys(row, composite_col, name_col, cell_col):
            if nk:
                lookup[nk] = row
    return {
        "lookup": lookup,
        "month_keys": month_keys,
        "ym_to_col": ym_to_col,
    }


def _month_status_from_historical_cell(raw):
    """Map sheet cell text to Regular / Irregular / Follow Up for the matrix."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip()
    if not s:
        return None
    st = extract_cell_sheet_status_type(s)
    if st:
        return st
    sl = s.lower()
    if sl.startswith("regular"):
        return "Regular"
    if sl.startswith("irregular"):
        return "Irregular"
    if sl.startswith("follow"):
        return "Follow Up"
    return None


def _attendance_row_lookup_key(row, att_name_col, cg_df, cg_name_col, cg_cell_col):
    att_name_str = str(row[att_name_col]).strip()
    cell_info = ""
    if cg_name_col and cg_cell_col:
        cg_match = cg_df[cg_df[cg_name_col].str.strip().str.lower() == att_name_str.lower()]
        if not cg_match.empty:
            cell_info = " - " + str(cg_match[cg_cell_col].iloc[0]).strip()
    return att_name_str + cell_info


def build_monthly_member_status_table(display_df, att_df, cg_df, status_hist_df=None):
    """
    One row per member in display_df; columns Cell, Member, Health (present/total + rate %),
    then each Month (MMM YY) with Regular / Irregular / Follow Up.
    Month labels come from **Status Historical** when that tab loads; missing cells show "—".
    If Status Historical is missing or has no month columns, months are derived from **Attendance**
    (75% rule on weekly 1/0 columns).
    Health always uses **Attendance** weekly marks aggregated over the same month keys shown.
    Only the latest MONTHLY_MEMBER_MATRIX_MAX_MONTHS month keys (within data, not after current month)
    are included so the table stays a fixed rolling window.
    Internal column _tile_status stores CG Combined status for Health cell coloring.
    Rows are sorted alphabetically by member name (case-insensitive), then by cell for stable ties.
    """
    if display_df is None or display_df.empty:
        return pd.DataFrame()

    hist_ctx = _parse_status_historical_for_monthly(status_hist_df)

    att_df = att_df if att_df is not None else pd.DataFrame()
    cg_df = cg_df if cg_df is not None else pd.DataFrame()

    month_to_colnames = {}
    att_name_col = None
    if not att_df.empty:
        att_name_col = att_df.columns[0] if len(att_df.columns) > 0 else None
        if att_name_col:
            for col_idx, col in enumerate(att_df.columns):
                if col_idx < 3:
                    continue
                d = parse_attendance_column_date(col)
                if d is None:
                    continue
                ym = (d.year, d.month)
                month_to_colnames.setdefault(ym, []).append(col)

    myt_today = datetime.now(timezone(timedelta(hours=8))).date()
    cur_ym = (myt_today.year, myt_today.month)

    if hist_ctx:
        month_keys = [ym for ym in hist_ctx["month_keys"] if ym <= cur_ym]
        ym_to_hist_col = hist_ctx["ym_to_col"]
        hist_lookup = hist_ctx["lookup"]
    else:
        month_keys = sorted(ym for ym in month_to_colnames if ym <= cur_ym)
        ym_to_hist_col = {}
        hist_lookup = {}

    if not month_keys:
        return pd.DataFrame()

    if len(month_keys) > MONTHLY_MEMBER_MATRIX_MAX_MONTHS:
        month_keys = month_keys[-MONTHLY_MEMBER_MATRIX_MAX_MONTHS:]

    month_labels = [datetime(y, m, 1).strftime("%b %y") for y, m in month_keys]

    cg_name_col, cg_cell_col = (None, None)
    if not cg_df.empty:
        cg_name_col, cg_cell_col = _resolve_cg_name_cell_columns(cg_df)

    key_to_row = {}
    if att_name_col and not att_df.empty and cg_name_col is not None:
        for _, row in att_df.iterrows():
            if pd.isna(row[att_name_col]) or str(row[att_name_col]).strip() == '':
                continue
            k = _attendance_row_lookup_key(row, att_name_col, cg_df, cg_name_col, cg_cell_col)
            key_to_row[k] = row

    disp_name_col = None
    disp_cell_col = None
    for col in display_df.columns:
        col_lower = col.lower().strip()
        if col_lower in ['cell', 'group']:
            disp_cell_col = col
        if col_lower in ['name', 'member name', 'member'] or (
            any(x in col_lower for x in ['name', 'member']) and 'last' not in col_lower
        ):
            if disp_name_col is None:
                disp_name_col = col
    if not disp_name_col:
        disp_name_col = display_df.columns[0]

    def display_row_key(nm, cl):
        ns = str(nm).strip() if pd.notna(nm) else ""
        cs = str(cl).strip() if cl is not None and pd.notna(cl) else ""
        if cs:
            return f"{ns} - {cs}"
        return ns

    status_col = None
    for col in display_df.columns:
        if "status" in col.lower():
            status_col = col
            break

    rows_out = []
    seen = set()
    for _, dr in display_df.iterrows():
        nm = dr.get(disp_name_col)
        cl = dr.get(disp_cell_col) if disp_cell_col else ""
        mk = display_row_key(nm, cl)
        if mk in seen:
            continue
        seen.add(mk)

        tile_status = extract_cell_sheet_status_type(dr.get(status_col)) if status_col else None

        att_row = key_to_row.get(mk)
        if att_row is None and disp_cell_col:
            att_row = key_to_row.get(str(nm).strip() if pd.notna(nm) else "")

        cl_str = str(cl).strip() if cl is not None and pd.notna(cl) else ""
        nm_str = str(nm).strip() if pd.notna(nm) else ""
        out = {"Cell": cl_str, "Member": nm_str, "_tile_status": tile_status}

        nk_full = _nwst_normalize_member_name(mk)
        nk_name = _nwst_normalize_member_name(nm_str)
        hist_row = None
        if hist_lookup:
            if nk_full in hist_lookup:
                hist_row = hist_lookup[nk_full]
            elif nk_name in hist_lookup:
                hist_row = hist_lookup[nk_name]

        use_hist_months = hist_ctx is not None and hist_row is not None

        for ym, lbl in zip(month_keys, month_labels):
            if use_hist_months:
                hcol = ym_to_hist_col.get(ym)
                raw = hist_row.get(hcol) if hcol is not None else None
                mapped = _month_status_from_historical_cell(raw)
                out[lbl] = mapped if mapped else "—"
            elif att_row is not None:
                cols_m = month_to_colnames.get(ym, [])
                present = 0
                total = 0
                for c in cols_m:
                    total += 1
                    v = att_row.get(c)
                    if v is not None and str(v).strip() == '1':
                        present += 1
                if total == 0:
                    out[lbl] = "—"
                else:
                    out[lbl] = categorize_member_status(present, total)
            else:
                out[lbl] = "—"

        if att_row is not None:
            all_present = 0
            all_total = 0
            for ym in month_keys:
                for c in month_to_colnames.get(ym, []):
                    all_total += 1
                    v = att_row.get(c)
                    if v is not None and str(v).strip() == '1':
                        all_present += 1
            if all_total == 0:
                out["Health"] = "—"
            else:
                att_pct = round(100.0 * all_present / all_total, 1)
                out["Health"] = f"{all_present}/{all_total} ({att_pct}%)"
        else:
            out["Health"] = "—"
        rows_out.append(out)

    result = pd.DataFrame(rows_out)
    if result.empty:
        return result
    col_order = ["Cell", "Member", "Health"] + month_labels
    build_cols = [c for c in col_order if c in result.columns]
    if "_tile_status" in result.columns:
        build_cols.append("_tile_status")
    result = result[build_cols]

    result["_member_key"] = result["Member"].fillna("").astype(str).str.strip().str.lower()
    result["_cell_key"] = result["Cell"].fillna("").astype(str).str.strip().str.lower()
    return (
        result.sort_values(["_member_key", "_cell_key"])
        .drop(columns=["_member_key", "_cell_key"])
        .reset_index(drop=True)
    )


def _monthly_table_month_columns(df):
    """Columns after Cell / Member / Health (chronological month labels)."""
    fixed = {"Cell", "Member", "Health", "_tile_status"}
    return [c for c in df.columns if c not in fixed]


def _worst_status_last_three_months(row, month_cols):
    """
    Fallback Health coloring when sheet tile status is missing: worst of the last
    3 month columns (Follow Up > Irregular > Regular). Ignores '—' and unknown values.
    """
    if not month_cols:
        return None
    lookback = month_cols[-3:]
    rank = {"Follow Up": 0, "Irregular": 1, "Regular": 2}
    worst_label = None
    worst_r = 99
    for c in lookback:
        raw = row.get(c)
        s = "" if pd.isna(raw) else str(raw).strip()
        if s not in rank:
            continue
        r = rank[s]
        if r < worst_r:
            worst_r = r
            worst_label = s
    return worst_label


def _monthly_trunc_expand_cell(value: str) -> str:
    """Narrow Cell/Member columns: summary truncates with CSS ellipsis; click opens full text below."""
    full = (value or "").strip()
    esc_full = html.escape(full)
    if not full:
        return '<td class="monthly-trunc-cell"></td>'
    inner = (
        f'<details class="monthly-trunc-details">'
        f'<summary class="monthly-trunc-summary" title="Click to show full text">{esc_full}</summary>'
        f'<span class="monthly-trunc-full">{esc_full}</span>'
        f"</details>"
    )
    return f'<td class="monthly-trunc-cell">{inner}</td>'


def _newcomer_trunc_expand_cell(value: str) -> str:
    """Long text columns: ellipsis in summary; click to expand (same UX as monthly table)."""
    full = (value or "").strip()
    esc_full = html.escape(full)
    if not full:
        return '<td class="newcomer-trunc-cell"></td>'
    inner = (
        f'<details class="newcomer-trunc-details">'
        f'<summary class="newcomer-trunc-summary" title="Click to show full text">{esc_full}</summary>'
        f'<span class="newcomer-trunc-full">{esc_full}</span>'
        f"</details>"
    )
    return f'<td class="newcomer-trunc-cell">{inner}</td>'


def _newcomer_column_should_truncate(col_name: str) -> bool:
    cl = str(col_name).lower()
    if any(x in cl for x in ["name", "member"]) and "last" not in cl:
        return True
    if any(x in cl for x in ["notes", "note"]):
        return True
    return False


def render_newcomer_list_html_table(df: pd.DataFrame, columns: list) -> str:
    """HTML table for newcomer list with truncating cells like monthly attendance."""
    if df is None or df.empty or not columns:
        return ""
    header_cells = "".join(f"<th>{html.escape(str(c))}</th>" for c in columns)
    body_rows = []
    view = df[columns].copy()
    for _, row in view.iterrows():
        cells = []
        for col in columns:
            raw = row[col]
            sval = "" if pd.isna(raw) else str(raw).strip()
            if _newcomer_column_should_truncate(col):
                cells.append(_newcomer_trunc_expand_cell(sval))
            else:
                cells.append(f"<td>{html.escape(sval)}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    return (
        '<div class="newcomer-list-table-wrap">'
        '<table class="newcomer-list-table">'
        f"<thead><tr>{header_cells}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table></div>"
    )


def render_monthly_status_html_table(df):
    """Render monthly status matrix as HTML with bold status labels (tile-matching colors)."""
    if df is None or df.empty:
        return ""

    status_span = {
        "Regular": "monthly-status-regular",
        "Irregular": "monthly-status-irregular",
        "Follow Up": "monthly-status-followup",
    }
    health_tile_classes = {
        "Regular": "monthly-status-regular",
        "Irregular": "monthly-status-irregular",
        "Follow Up": "monthly-status-followup",
        "New": "monthly-health-tile-new",
        "Red": "monthly-health-tile-red",
        "Graduated": "monthly-health-tile-graduated",
    }

    month_cols = _monthly_table_month_columns(df)
    display_columns = [c for c in df.columns if c != "_tile_status"]

    header_cells = "".join(
        f"<th>{html.escape(str(c))}</th>" for c in display_columns
    )
    body_rows = []
    has_tile_col = "_tile_status" in df.columns
    for _, row in df.iterrows():
        cells = []
        eff_health_status = None
        if has_tile_col:
            tile_raw = row.get("_tile_status")
            if tile_raw is not None and not (isinstance(tile_raw, float) and pd.isna(tile_raw)):
                ts = str(tile_raw).strip()
                if ts in health_tile_classes:
                    eff_health_status = ts
        if eff_health_status is None:
            eff_health_status = _worst_status_last_three_months(row, month_cols)

        for col in display_columns:
            raw = row[col]
            sval = "" if pd.isna(raw) else str(raw).strip()
            if col == "Health":
                att_cls = health_tile_classes.get(eff_health_status, "")
                if att_cls:
                    cells.append(
                        f"<td class=\"monthly-attendance-rate-cell\"><span class=\"{att_cls}\">{html.escape(sval)}</span></td>"
                    )
                else:
                    cells.append(
                        f"<td class=\"monthly-attendance-rate-cell\">{html.escape(sval)}</td>"
                    )
            elif col == "Member":
                cells.append(_monthly_trunc_expand_cell(sval))
            elif col == "Cell":
                cells.append(_monthly_trunc_expand_cell(sval))
            else:
                mo_span = status_span.get(sval, "")
                if mo_span:
                    cells.append(
                        f"<td><span class=\"{mo_span}\">{html.escape(sval)}</span></td>"
                    )
                else:
                    cells.append(f"<td>{html.escape(sval)}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")

    return (
        '<div class="monthly-attendance-table-wrap">'
        '<table class="monthly-attendance-table">'
        f"<thead><tr>{header_cells}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table></div>"
    )


def get_member_category_color(category):
    """Return color based on member category."""
    colors = {
        "New": "#3498db",      # Blue
        "Regular": "#2ecc71",   # Green
        "Irregular": "#e67e22"  # Orange
    }
    return colors.get(category, "#95a5a6")

def get_leadership_by_role(df):
    """
    Extract leadership members grouped by role hierarchy.
    Returns a dict with role display names as keys and list of members as values.
    """
    # Define the role hierarchy with exact values and display order
    role_hierarchy = {
        1: "1. CG Leader",
        2: "2. Assistant CG Leader",
        3: "3. CG Core",
        4: "4. Potential CG Core",
        5: "5. Ministry Leader",
        6: "6. Assistant Ministry Leader",
        7: "7. Ministry Core",
        8: "8. Potential Ministry Core",
        9: "9. Zone Leader"
    }

    # Find the Role column (case-insensitive)
    role_col = None
    for col in df.columns:
        if col.lower().strip() == 'role':
            role_col = col
            break

    if not role_col:
        return {}

    # Find the "Since" column (case-insensitive)
    since_col = None
    for col in df.columns:
        if 'since' in col.lower():
            since_col = col
            break

    # Find the Name column
    name_col = None
    for col in df.columns:
        col_lower = col.lower()
        if (any(x in col_lower for x in ['name', 'member']) and 'last' not in col_lower):
            name_col = col
            break

    if not name_col:
        name_col = df.columns[0]

    # Group members by role
    leadership_groups = {}
    for _, row in df.iterrows():
        role_val = str(row[role_col]).strip() if pd.notna(row[role_col]) else ""

        # Check if this role matches any in our hierarchy
        matching_role = None
        for order, role_name in role_hierarchy.items():
            if role_val == role_name:
                matching_role = role_name
                break

        if matching_role:
            if matching_role not in leadership_groups:
                leadership_groups[matching_role] = []

            # Get member info
            member_name = str(row[name_col]).strip() if pd.notna(row[name_col]) else "Unknown"
            since_info = ""
            if since_col and pd.notna(row[since_col]):
                since_val = str(row[since_col]).strip()
                if since_val:
                    since_info = since_val

            leadership_groups[matching_role].append({
                "name": member_name,
                "since": since_info
            })

    # Sort by hierarchy order and return (removes numbers from display)
    sorted_leadership = {}
    for order, role_name in role_hierarchy.items():
        if role_name in leadership_groups:
            # Remove the number prefix for display (e.g., "1. " becomes "")
            display_role = role_name[role_name.find('. ') + 2:] if '. ' in role_name else role_name
            sorted_leadership[display_role] = leadership_groups[role_name]

    return sorted_leadership

def get_today_myt_date():
    """Get today's date in MYT timezone as a string (YYYY-MM-DD)"""
    myt = timezone(timedelta(hours=8))
    return datetime.now(myt).strftime("%Y-%m-%d")


def _normalize_primary_hex(hex_str):
    h = (hex_str or "").strip()
    if not h:
        return None
    if not h.startswith("#"):
        h = "#" + h
    if len(h) != 7:
        return None
    try:
        int(h[1:], 16)
    except ValueError:
        return None
    return h.lower()


def theme_from_primary_hex(primary_hex):
    """Build the same daily_colors shape as generate_colors_for_date from a fixed primary."""
    p = _normalize_primary_hex(primary_hex)
    if not p:
        raise ValueError("Invalid primary hex")
    r = int(p[1:3], 16) / 255.0
    g = int(p[3:5], 16) / 255.0
    b = int(p[5:7], 16) / 255.0
    h, light, sat = colorsys.rgb_to_hls(r, g, b)
    rgb_light = colorsys.hls_to_rgb(h, min(light + 0.2, 0.9), sat)
    light_color = "#{:02x}{:02x}{:02x}".format(
        int(rgb_light[0] * 255),
        int(rgb_light[1] * 255),
        int(rgb_light[2] * 255),
    )
    return {
        "primary": p,
        "light": light_color,
        "background": "#000000",
        "accent": p,
    }


_nwst_accent_cfg_mod = None


def _accent_overrides_from_project_config():
    """Load nwst_accent_config.py from this folder or an ancestor (next to nwst_accent_*.py)."""
    global _nwst_accent_cfg_mod
    if _nwst_accent_cfg_mod is None:
        p = Path(__file__).resolve().parent
        for _ in range(15):
            cfg = p / "nwst_accent_config.py"
            if cfg.is_file():
                spec = importlib.util.spec_from_file_location("_nwst_accent_cfg", cfg)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                _nwst_accent_cfg_mod = mod
                break
            if p.parent == p:
                break
            p = p.parent
    if _nwst_accent_cfg_mod is None:
        return {}
    return _nwst_accent_cfg_mod.get_accent_override_by_date()


def _theme_overrides_from_redis():
    """Theme Override rows from Upstash (refreshed on Sync from Google Sheets or CHECK IN Update names)."""
    redis_client = get_redis_client()
    if _nwst_accent_cfg_mod is None:
        _accent_overrides_from_project_config()
    if _nwst_accent_cfg_mod:
        try:
            return _nwst_accent_cfg_mod.read_theme_override_from_redis(redis_client)
        except Exception:
            return {}
    return {}


def resolve_theme_override_row_for_today(from_sheet=None):
    """Latest-dated row from the Theme Override Upstash snapshot (+ JSON merge for that date).

    If the snapshot is empty, returns ``{}`` so callers use ``banner.gif`` and generated colors.
    """
    from_file = _accent_overrides_from_project_config()
    if from_sheet is None:
        from_sheet = _theme_overrides_from_redis()
    if not from_sheet:
        return {}
    if _nwst_accent_cfg_mod:
        row = _nwst_accent_cfg_mod.resolve_latest_cached_theme_row(from_file, from_sheet)
    else:
        latest = max(from_sheet.keys())
        keys = set(from_file) | set(from_sheet)
        merged = {
            k: {**(from_file.get(k) or {}), **(from_sheet.get(k) or {})}
            for k in keys
            if {**(from_file.get(k) or {}), **(from_sheet.get(k) or {})}
        }
        row = dict(merged.get(latest) or {})
    today = get_today_myt_date()
    if not row.get("primary"):
        env_d = os.getenv("ATTENDANCE_ACCENT_OVERRIDE_DATE", "").strip()
        env_h = os.getenv("ATTENDANCE_ACCENT_OVERRIDE_HEX", "").strip()
        if env_d == today and env_h:
            row["primary"] = env_h.strip()
        else:
            try:
                if hasattr(st, "secrets"):
                    sd = str(st.secrets.get("ATTENDANCE_ACCENT_OVERRIDE_DATE", "")).strip()
                    sh = str(st.secrets.get("ATTENDANCE_ACCENT_OVERRIDE_HEX", "")).strip()
                    if sd == today and sh:
                        row["primary"] = sh.strip()
            except Exception:
                pass
    return row


def generate_colors_for_date(date_str):
    """Generate random colors based on a specific date (consistent for that date)

    Args:
        date_str: Date string in format "YYYY-MM-DD"

    Returns:
        dict with 'primary', 'light', 'background', 'accent' colors
    """
    seed = int(hashlib.md5(date_str.encode()).hexdigest(), 16)
    random.seed(seed)

    hue = random.random()
    saturation = random.uniform(0.7, 1.0)
    lightness = random.uniform(0.45, 0.65)

    rgb = colorsys.hls_to_rgb(hue, lightness, saturation)
    primary_color = '#{:02x}{:02x}{:02x}'.format(
        int(rgb[0] * 255),
        int(rgb[1] * 255),
        int(rgb[2] * 255)
    )

    rgb_light = colorsys.hls_to_rgb(hue, min(lightness + 0.2, 0.9), saturation)
    light_color = '#{:02x}{:02x}{:02x}'.format(
        int(rgb_light[0] * 255),
        int(rgb_light[1] * 255),
        int(rgb_light[2] * 255)
    )

    return {
        'primary': primary_color,
        'light': light_color,
        'background': '#000000',
        'accent': primary_color
    }

def generate_daily_colors():
    """Weekly Saturday-locked palette (MYT), unless Theme Override is cached in Upstash (latest sheet row)."""
    today = datetime.strptime(get_today_myt_date(), "%Y-%m-%d")
    days_since_saturday = (today.weekday() - 5) % 7
    last_saturday = today - timedelta(days=days_since_saturday)
    from_sheet = _theme_overrides_from_redis()
    row = resolve_theme_override_row_for_today(from_sheet=from_sheet)
    hex_override = row.get("primary")
    base = None
    if hex_override:
        pn = _normalize_primary_hex(hex_override)
        if pn:
            base = theme_from_primary_hex(pn)
    if base is None:
        base = generate_colors_for_date(last_saturday.strftime("%Y-%m-%d"))
    b_raw = row.get("banner")
    if b_raw:
        if _nwst_accent_cfg_mod is None:
            _accent_overrides_from_project_config()
        if _nwst_accent_cfg_mod:
            safe = _nwst_accent_cfg_mod.sanitize_banner_filename(b_raw)
            if safe:
                base = {**base, "banner": safe}
    if not from_sheet:
        if _nwst_accent_cfg_mod is None:
            _accent_overrides_from_project_config()
        if _nwst_accent_cfg_mod:
            safe = _nwst_accent_cfg_mod.sanitize_banner_filename("banner.gif")
            if safe:
                base = {**base, "banner": safe}
    return base


def _render_nwst_analytics_individual_attendance(colors, cell_to_zone_map):
    """NWST monthly member matrix (same data as CG Individual Attendance), tabs grouped by zone from Key Values."""
    display_df = get_newcomers_data()
    if display_df is None or display_df.empty:
        st.info("No **CG Combined** roster — sync NWST Health to load members.")
        return

    att_df_m, cg_df_m = load_attendance_and_cg_dataframes()
    if cg_df_m is None:
        st.info("Could not load NWST **CG Combined** for this table.")
        return
    if att_df_m is None:
        att_df_m = pd.DataFrame()
    status_hist_df = load_status_historical_dataframe()
    monthly_status_df = build_monthly_member_status_table(
        display_df, att_df_m, cg_df_m, status_hist_df
    )
    if monthly_status_df is None or monthly_status_df.empty:
        st.info(
            "No individual attendance breakdown yet. Check NWST **Status Historical** month headers "
            "(e.g. Jan 2026) or **Attendance** row 1 from column D for parseable dates."
        )
        return

    def _zone_for_cell(cell_val):
        c = str(cell_val).strip() if cell_val is not None and pd.notna(cell_val) else ""
        if not c:
            return "Unknown"
        return cell_to_zone_map.get(c.lower(), c)

    monthly_status_df = monthly_status_df.copy()
    monthly_status_df["_zone"] = monthly_status_df["Cell"].apply(_zone_for_cell)

    p = html.escape(str(colors.get("primary", "#00ff00")), quote=True)
    bg = html.escape(str(colors.get("background", "#000000")), quote=True)
    st.markdown(
        f"""
        <style>
            [data-testid="stMultiSelect"] {{
                font-family: 'Inter', sans-serif !important;
            }}
            [data-testid="stMultiSelect"] > div {{
                border: 2px solid {p} !important;
                border-radius: 0px !important;
                background: {bg} !important;
            }}
            [data-testid="stMultiSelect"] span {{
                font-family: 'Inter', sans-serif !important;
                color: #ffffff !important;
            }}
            [data-testid="stMultiSelect"] svg {{
                fill: {p} !important;
            }}
            [data-testid="stMultiSelect"] [data-baseweb="tag"] {{
                background: {p} !important;
                border-radius: 0px !important;
            }}
            [data-testid="stMultiSelect"] [data-baseweb="tag"] span {{
                color: {bg} !important;
                font-weight: 600 !important;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    if "analytics_ia_clear_filter_counter" not in st.session_state:
        st.session_state.analytics_ia_clear_filter_counter = 0
    _ia_fc = st.session_state.analytics_ia_clear_filter_counter

    _all_names = sorted(
        monthly_status_df["Member"].dropna().astype(str).str.strip().unique().tolist(),
        key=str.lower,
    )
    _all_names = [n for n in _all_names if n]

    _ncol1, _ncol2 = st.columns([3, 1])
    with _ncol1:
        _sel_names = st.multiselect(
            "Search by Name...",
            options=_all_names,
            default=[],
            key=f"analytics_ia_name_multiselect_{_ia_fc}",
            placeholder="Search and select names...",
            label_visibility="collapsed",
        )
    with _ncol2:
        if st.button(
            "Clear All",
            type="secondary",
            use_container_width=True,
            key="analytics_ia_clear_filters",
        ):
            st.session_state.analytics_ia_clear_filter_counter += 1
            st.rerun()

    _filtered = monthly_status_df.copy()
    if _sel_names:
        _mem_f = _filtered["Member"].dropna().astype(str).str.strip()
        _filtered = _filtered[_mem_f.isin(_sel_names)]

    _parts = []
    if _sel_names:
        _parts.append(f"{len(_sel_names)} name(s)")
    _ftext = f" from {' and '.join(_parts)}" if _parts else ""
    tm = html.escape(str(colors.get("text_muted", "#999999")), quote=True)

    st.markdown(
        f"<p style='color: {tm}; font-family: Inter, sans-serif; font-size: 0.9rem; margin: 1rem 0 0.5rem 0;'>"
        f"Showing <b style=\"color: {p}\">{len(_filtered)}</b> members{_ftext}</p>",
        unsafe_allow_html=True,
    )

    if _filtered.empty:
        st.info("No members match the current filters.")
        return

    _mwf = _filtered.copy()
    if _sel_names:
        _show = _mwf.drop(columns=["_zone"], errors="ignore")
        st.markdown(
            render_monthly_status_html_table(_show),
            unsafe_allow_html=True,
        )
    else:
        _zones = sorted(_mwf["_zone"].unique().tolist(), key=str.lower)
        if len(_zones) > 1:
            _ztabs = st.tabs(_zones)
            for _ti, zname in enumerate(_zones):
                with _ztabs[_ti]:
                    _sub = _mwf[_mwf["_zone"] == zname].drop(columns=["_zone"])
                    st.markdown(
                        render_monthly_status_html_table(_sub),
                        unsafe_allow_html=True,
                    )
        else:
            _show = _mwf.drop(columns=["_zone"])
            st.markdown(
                render_monthly_status_html_table(_show),
                unsafe_allow_html=True,
            )


def render_nwst_analytics_page(colors):
    """Saturday attendance trends from the Attendance Analytics sheet (same as CHECK IN attendance_app)."""
    analytics_sheet_id = (CHECKIN_ATTENDANCE_SHEET_ID or "").strip()
    if not analytics_sheet_id:
        st.error(
            "Analytics uses the CHECK IN spreadsheet. Set **ATTENDANCE_SHEET_ID** in your environment "
            "or in `.streamlit/secrets.toml` (same as the CHECK IN app)."
        )
        return

    df, saturday_dates, error = nwst_get_attendance_analytics_data(analytics_sheet_id)

    if error:
        st.error(error)
        return

    if df is None or df.empty:
        st.info("No analytics data available.")
        return

    cell_to_zone_map = nwst_get_cell_zone_mapping(analytics_sheet_id)
    df["Zone"] = df["Cell Group"].apply(
        lambda x: cell_to_zone_map.get(x.lower(), x) if x else "Unknown"
    )

    date_cols = [col for col in df.columns if col not in ["Name", "Cell Group", "Name - Cell Group", "Zone"]]

    def _analytics_zone_for_cell(cg):
        return cell_to_zone_map.get(cg.lower(), cg) if cg else "Unknown"

    def _analytics_exclude_from_rate_charts(cg):
        if not str(cg).strip():
            return True
        if str(_analytics_zone_for_cell(cg)).strip().lower() == "archive":
            return True
        n = str(cg).strip().lower().lstrip("*").strip()
        if n == "not sure yet" or n.startswith("not sure yet"):
            return True
        return False

    st.markdown(
        f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

        .analytics-container * {{
            font-family: 'Inter', sans-serif !important;
        }}

        [data-testid="stDataFrame"] {{
            font-family: 'Inter', sans-serif !important;
        }}
        [data-testid="stDataFrame"] * {{
            font-family: 'Inter', sans-serif !important;
        }}
        .stDataFrame th {{
            font-family: 'Inter', sans-serif !important;
            font-weight: 700 !important;
            text-transform: uppercase !important;
            letter-spacing: 1px !important;
        }}
        .stDataFrame td {{
            font-family: 'Inter', sans-serif !important;
        }}

        .analytics-kpi-container {{
            display: flex;
            justify-content: center;
            gap: 2rem;
            margin: 2rem 0;
            flex-wrap: wrap;
        }}
        .analytics-kpi-card {{
            background: {colors['card_bg']};
            border: 2px solid {colors['primary']};
            padding: 1.5rem 2rem;
            text-align: center;
            min-width: 180px;
        }}
        .analytics-kpi-label {{
            font-family: 'Inter', sans-serif !important;
            font-size: 0.8rem;
            font-weight: 700;
            color: {colors['text_muted']};
            text-transform: uppercase;
            letter-spacing: 2px;
            margin-bottom: 0.5rem;
        }}
        .analytics-kpi-number {{
            font-family: 'Inter', sans-serif !important;
            font-size: 3rem;
            font-weight: 900;
            color: {colors['primary']};
            line-height: 1;
        }}
        .analytics-section-title {{
            font-family: 'Inter', sans-serif !important;
            font-size: 1.5rem;
            font-weight: 900;
            color: {colors['primary']};
            text-transform: uppercase;
            letter-spacing: 3px;
            margin: 2rem 0 1rem 0;
            border-bottom: 3px solid {colors['primary']};
            padding-bottom: 0.5rem;
            display: inline-block;
        }}
    </style>
    """,
        unsafe_allow_html=True,
    )

    st.markdown(_nwst_collapsible_section_css(colors["primary"]), unsafe_allow_html=True)

    total_unique_attendees = len(df)
    total_saturdays = len(date_cols)
    if total_saturdays > 0:
        avg_attendance = df[date_cols].sum().mean()
        latest_attendance = df[date_cols[-1]].sum() if date_cols else 0
    else:
        avg_attendance = 0
        latest_attendance = 0

    st.markdown(
        f"""
    <div class="analytics-kpi-container">
        <div class="analytics-kpi-card">
            <div class="analytics-kpi-label">Total Saturdays</div>
            <div class="analytics-kpi-number">{total_saturdays}</div>
        </div>
        <div class="analytics-kpi-card">
            <div class="analytics-kpi-label">Unique Attendees</div>
            <div class="analytics-kpi-number">{total_unique_attendees}</div>
        </div>
        <div class="analytics-kpi-card">
            <div class="analytics-kpi-label">Avg Attendance</div>
            <div class="analytics-kpi-number">{avg_attendance:.0f}</div>
        </div>
        <div class="analytics-kpi-card">
            <div class="analytics-kpi-label">Latest ({date_cols[-1] if date_cols else 'N/A'})</div>
            <div class="analytics-kpi-number">{latest_attendance}</div>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    with st.expander("📈 ATTENDANCE TREND (SATURDAYS)", expanded=False):
        attendance_per_date = df[date_cols].sum()
        trend_df = pd.DataFrame({"Date": date_cols, "Attendance": attendance_per_date.values})

        fig_trend = px.line(
            trend_df,
            x="Date",
            y="Attendance",
            markers=True,
            title="",
            labels={"Attendance": "Total Attendance", "Date": "Saturday Date"},
            height=350,
        )

        fig_trend.update_traces(
            line=dict(color=colors["primary"], width=3),
            marker=dict(color=colors["primary"], size=10, line=dict(color=colors["background"], width=2)),
            hovertemplate="<b>%{x}</b><br>Attendance: %{y}<extra></extra>",
        )

        fig_trend.update_layout(
            plot_bgcolor=colors["background"],
            paper_bgcolor=colors["card_bg"],
            font=dict(family="Inter, sans-serif", size=12, color=colors["primary"]),
            xaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter"),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
                showgrid=True,
                gridwidth=1,
            ),
            yaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter"),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
                showgrid=True,
                gridwidth=1,
            ),
            hoverlabel=dict(bgcolor=colors["background"], font=dict(color=colors["primary"], family="Inter")),
            margin=dict(l=50, r=50, t=30, b=50),
        )

        st.plotly_chart(fig_trend, use_container_width=True)

    with st.expander("📊 AVERAGE ATTENDANCE BY ZONE", expanded=False):
        zone_attendance = (
            df.groupby("Zone")[date_cols].sum().sum(axis=1) / len(date_cols) if date_cols else pd.Series()
        )
        zone_df = pd.DataFrame({"Zone": zone_attendance.index, "Avg Attendance": zone_attendance.values}).sort_values(
            "Avg Attendance", ascending=False
        )

        fig_zone = px.bar(
            zone_df,
            x="Zone",
            y="Avg Attendance",
            color="Avg Attendance",
            color_continuous_scale=[colors["background"], colors["primary"]],
            text="Avg Attendance",
            height=350,
        )

        fig_zone.update_traces(
            texttemplate="%{text:.0f}",
            textfont=dict(size=12, color=colors["background"], family="Inter", weight="bold"),
            textposition="inside",
            marker=dict(line=dict(color=colors["primary"], width=2)),
            hovertemplate="<b>%{x}</b><br>Avg: %{y:.1f}<extra></extra>",
        )

        fig_zone.update_layout(
            plot_bgcolor=colors["background"],
            paper_bgcolor=colors["card_bg"],
            font=dict(family="Inter, sans-serif", size=12, color=colors["primary"]),
            xaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter"),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
                categoryorder="total descending",
            ),
            yaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter"),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
            ),
            coloraxis_showscale=False,
            showlegend=False,
            hoverlabel=dict(bgcolor=colors["background"], font=dict(color=colors["primary"], family="Inter")),
            margin=dict(l=50, r=50, t=30, b=50),
        )

        st.plotly_chart(fig_zone, use_container_width=True)

    with st.expander("👤 INDIVIDUAL ATTENDANCE", expanded=False):
        st.markdown(
            f"<p style='color: {colors['text_muted']}; font-family: Inter, sans-serif; "
            f"font-size: 0.85rem; margin: 0 0 1rem 0;'>"
            f"Monthly status columns mirror NWST <b>Status Historical</b>; "
            f"<b>Health</b> uses <b>Attendance</b> + <b>CG Combined</b>. "
            f"Each tab is one <b>zone</b> (Key Values on this spreadsheet).</p>",
            unsafe_allow_html=True,
        )
        _render_nwst_analytics_individual_attendance(colors, cell_to_zone_map)

    with st.expander("📉 ATTENDANCE RATE BY CELL", expanded=False):
        st.markdown(
            f"<p style='color: {colors['text_muted']}; font-family: Inter, sans-serif; "
            f"font-size: 0.85rem; margin: 0 0 1rem 0;'>"
            f"<b style=\"color: {colors['primary']}\">How to read:</b> pick a <b>zone tab</b> — one big chart, no endless scroll. "
            f"Saturdays run left → right; <b>Y</b> = that week&apos;s check-ins ÷ cell roster (Options), as %. "
            f"Bright line colors so each cell group is obvious.</p>",
            unsafe_allow_html=True,
        )

        members_per_cell, options_err = nwst_get_options_roster_members(analytics_sheet_id)
        if not members_per_cell and options_err:
            st.warning(
                f"Could not load Options tab for roster sizes ({options_err}). "
                f"Denominator falls back to unique names seen in analytics per cell."
            )

        st.markdown(
            f"""
        <style>
            [data-testid="stMultiSelect"] {{
                font-family: 'Inter', sans-serif !important;
            }}
            [data-testid="stMultiSelect"] > div {{
                border: 2px solid {colors['primary']} !important;
                border-radius: 0px !important;
                background: {colors['background']} !important;
            }}
            [data-testid="stMultiSelect"] span {{
                font-family: 'Inter', sans-serif !important;
                color: {colors['text']} !important;
            }}
            [data-testid="stMultiSelect"] svg {{
                fill: {colors['primary']} !important;
            }}
            [data-testid="stMultiSelect"] [data-baseweb="tag"] {{
                background: {colors['primary']} !important;
                border-radius: 0px !important;
            }}
            [data-testid="stMultiSelect"] [data-baseweb="tag"] span {{
                color: {colors['background']} !important;
                font-weight: 600 !important;
            }}
        </style>
        """,
            unsafe_allow_html=True,
        )

        if "clear_filter_counter" not in st.session_state:
            st.session_state.clear_filter_counter = 0

        cell_groups = sorted(
            [c for c in df["Cell Group"].unique() if c and not _analytics_exclude_from_rate_charts(c)]
        )
        filter_col1, filter_col2 = st.columns([3, 1])
        with filter_col1:
            selected_cell_groups = st.multiselect(
                "Filter by Cell Group...",
                options=cell_groups,
                default=[],
                key=f"analytics_cell_multiselect_{st.session_state.clear_filter_counter}",
                placeholder="Select cell groups...",
                label_visibility="collapsed",
            )
        with filter_col2:
            if st.button("Clear All", type="secondary", use_container_width=True, key="nwst_clear_cell_filter"):
                st.session_state.clear_filter_counter += 1
                st.rerun()

        work_df = df.copy()
        if selected_cell_groups:
            work_df = work_df[work_df["Cell Group"].isin(selected_cell_groups)]

        roster_cells = set(members_per_cell.keys())
        analytics_cells = set(work_df["Cell Group"].dropna().unique())
        all_cells = roster_cells | analytics_cells
        if selected_cell_groups:
            all_cells = set(selected_cell_groups) & all_cells

        zone_to_cells = defaultdict(list)
        for cg in all_cells:
            if not str(cg).strip() or _analytics_exclude_from_rate_charts(cg):
                continue
            zone_to_cells[_analytics_zone_for_cell(cg)].append(cg)

        zone_plots = {}
        for zone in sorted(zone_to_cells.keys(), key=str.lower):
            cells = sorted(zone_to_cells[zone], key=str.lower)
            long_rows = []
            for cg in cells:
                sub = work_df[work_df["Cell Group"] == cg]
                mc = members_per_cell.get(cg, 0)
                if mc == 0 and not sub.empty:
                    mc = sub["Name"].nunique()
                if mc == 0:
                    continue
                for dc in date_cols:
                    attended = int(sub[dc].sum()) if dc in sub.columns else 0
                    pct = 100.0 * attended / mc
                    long_rows.append({"Saturday": dc, "Cell Group": cg, "Attendance rate %": round(pct, 1)})

            plot_df = pd.DataFrame(long_rows)
            if plot_df.empty:
                continue
            ymax = max(105.0, plot_df["Attendance rate %"].max() * 1.08)
            zone_plots[zone] = (plot_df, ymax)

        if zone_plots:
            zone_tab_names = sorted(zone_plots.keys(), key=str.lower)
            zone_tabs = st.tabs(zone_tab_names)
            for i, zone in enumerate(zone_tab_names):
                plot_df, ymax = zone_plots[zone]
                with zone_tabs[i]:
                    fig_zone_cells = px.line(
                        plot_df,
                        x="Saturday",
                        y="Attendance rate %",
                        color="Cell Group",
                        markers=True,
                        title="",
                        height=460,
                        color_discrete_sequence=NWST_ANALYTICS_MULTILINE_PALETTE,
                    )
                    fig_zone_cells.update_traces(
                        line=dict(width=3.5),
                        marker=dict(
                            size=5,
                            line=dict(width=1, color="#FFFFFF"),
                            opacity=1,
                        ),
                        hovertemplate=(
                            "<b>%{fullData.name}</b><br>"
                            "%{x}<br>"
                            "<b>%{y:.1f}%</b> of cell showed up<extra></extra>"
                        ),
                    )
                    fig_zone_cells.add_hline(
                        y=50,
                        line_dash="dot",
                        line_color=colors["text_muted"],
                        line_width=1,
                        opacity=0.55,
                        annotation_text="50%",
                        annotation_position="right",
                        annotation_font_color=colors["text_muted"],
                        annotation_font_size=11,
                    )
                    fig_zone_cells.update_layout(
                        plot_bgcolor=colors["background"],
                        paper_bgcolor=colors["card_bg"],
                        font=dict(family="Inter, sans-serif", size=13, color=colors["primary"]),
                        xaxis=dict(
                            title=dict(text="Saturday service", font=dict(size=12)),
                            tickfont=dict(color=colors["text"], family="Inter", size=11),
                            gridcolor=colors["text_muted"],
                            gridwidth=1,
                            linecolor=colors["primary"],
                            linewidth=2,
                            tickangle=-30,
                            categoryorder="array",
                            categoryarray=date_cols,
                        ),
                        yaxis=dict(
                            title=dict(text="How much of the cell came?", font=dict(size=12)),
                            tickfont=dict(color=colors["text"], family="Inter", size=11),
                            ticksuffix="%",
                            gridcolor=colors["text_muted"],
                            gridwidth=1,
                            linecolor=colors["primary"],
                            linewidth=2,
                            range=[0, ymax],
                        ),
                        legend=dict(
                            title=dict(text="Cell groups", font=dict(size=11, color=colors["primary"])),
                            orientation="h",
                            yanchor="top",
                            y=-0.28,
                            xanchor="center",
                            x=0.5,
                            font=dict(size=12, color=colors["text"], family="Inter"),
                            bgcolor="rgba(0,0,0,0)",
                            borderwidth=0,
                        ),
                        hoverlabel=dict(
                            bgcolor=colors["card_bg"],
                            font=dict(size=13, color=colors["primary"], family="Inter"),
                            bordercolor=colors["primary"],
                        ),
                        margin=dict(l=55, r=50, t=28, b=150),
                    )
                    st.plotly_chart(fig_zone_cells, use_container_width=True)
                    st.caption("Tip: follow **one color** across the weeks — rightmost dot is the latest Saturday.")

    with st.expander("📊 ATTENDANCE BY CELL GROUP", expanded=False):
        cell_group_attendance = (
            df.groupby("Cell Group")[date_cols].sum().sum(axis=1) / len(date_cols) if date_cols else pd.Series()
        )
        cell_group_df = pd.DataFrame(
            {"Cell Group": cell_group_attendance.index, "Avg Attendance": cell_group_attendance.values}
        ).sort_values("Avg Attendance", ascending=False).head(20)

        fig_cell = px.bar(
            cell_group_df,
            x="Cell Group",
            y="Avg Attendance",
            color="Avg Attendance",
            color_continuous_scale=[colors["background"], colors["primary"]],
            text="Avg Attendance",
            height=400,
        )

        fig_cell.update_traces(
            texttemplate="%{text:.1f}",
            textfont=dict(size=11, color=colors["background"], family="Inter", weight="bold"),
            textposition="inside",
            marker=dict(line=dict(color=colors["primary"], width=2)),
            hovertemplate="<b>%{x}</b><br>Avg: %{y:.1f}<extra></extra>",
        )

        fig_cell.update_layout(
            plot_bgcolor=colors["background"],
            paper_bgcolor=colors["card_bg"],
            font=dict(family="Inter, sans-serif", size=12, color=colors["primary"]),
            xaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter", size=9),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
                categoryorder="total descending",
                tickangle=-45,
            ),
            yaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter"),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
            ),
            coloraxis_showscale=False,
            showlegend=False,
            hoverlabel=dict(bgcolor=colors["background"], font=dict(color=colors["primary"], family="Inter")),
            margin=dict(l=50, r=50, t=30, b=100),
        )

        st.plotly_chart(fig_cell, use_container_width=True)

    with st.expander("📈 ZONE ATTENDANCE TREND", expanded=False):
        zones = df["Zone"].dropna().unique()
        zones = [z for z in zones if str(z).strip()]
        zone_order = sorted(zones, key=lambda z: str(z).lower())
        zone_palette = _nwst_analytics_palette_for_n(len(zone_order))

        zone_trend_data = []
        for date_col in date_cols:
            for zone in zones:
                zone_attendance_on_date = df[df["Zone"] == zone][date_col].sum()
                zone_trend_data.append({"Date": date_col, "Zone": zone, "Attendance": zone_attendance_on_date})

        zone_trend_df = pd.DataFrame(zone_trend_data)

        fig_zone_trend = px.line(
            zone_trend_df,
            x="Date",
            y="Attendance",
            color="Zone",
            markers=True,
            height=400,
            category_orders={"Zone": zone_order},
            color_discrete_sequence=zone_palette,
        )

        fig_zone_trend.update_traces(
            line=dict(width=3.5),
            marker=dict(size=8, line=dict(width=1, color="#FFFFFF"), opacity=1),
            hovertemplate="<b>%{fullData.name}</b><br>%{x}: %{y}<extra></extra>",
        )

        fig_zone_trend.update_layout(
            plot_bgcolor=colors["background"],
            paper_bgcolor=colors["card_bg"],
            font=dict(family="Inter, sans-serif", size=12, color=colors["primary"]),
            xaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter"),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
            ),
            yaxis=dict(
                tickfont=dict(color=colors["text_muted"], family="Inter"),
                gridcolor=colors["text_muted"],
                linecolor=colors["primary"],
                linewidth=2,
            ),
            legend=dict(
                font=dict(color=colors["text_muted"], family="Inter"),
                bgcolor=colors["card_bg"],
                bordercolor=colors["primary"],
                borderwidth=1,
            ),
            hoverlabel=dict(bgcolor=colors["background"], font=dict(color=colors["primary"], family="Inter")),
            margin=dict(l=50, r=50, t=30, b=50),
        )

        st.plotly_chart(fig_zone_trend, use_container_width=True)


# Page configuration
st.set_page_config(
    page_title="NWST Health",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Weekly accent theme (locked to most recent Saturday MYT, same as CHECK IN attendance app)
daily_colors = generate_daily_colors()

# Optional banner image from Theme Override (file in NWST HEALTH folder, not .streamlit/)
_nwst_banner = daily_colors.get("banner")
if _nwst_banner:
    _nwst_banner_path = Path(__file__).resolve().parent.parent / _nwst_banner
    if _nwst_banner_path.is_file():
        st.image(str(_nwst_banner_path), use_container_width=True)

# Convert hex color to RGB for rgba shadows
def hex_to_rgb(hex_color):
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

primary_rgb = hex_to_rgb(daily_colors['primary'])

# Add CSS to reduce Streamlit default spacing and style with daily color theme
st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* Base theme colors */
    .stApp {{
        background-color: {daily_colors['background']} !important;
    }}

    .element-container {{
        margin-top: 0rem !important;
        margin-bottom: 0rem !important;
        padding-top: 0rem !important;
        padding-bottom: 0rem !important;
    }}
    [data-testid="stVerticalBlock"] {{
        gap: 0rem !important;
    }}
    [data-testid="stVerticalBlock"] > [style*="flex-direction: column"] {{
        gap: 0rem !important;
    }}
    .stMarkdown {{
        margin-top: 0rem !important;
        margin-bottom: 0rem !important;
        padding-top: 0rem !important;
        padding-bottom: 0rem !important;
    }}
    [data-testid="column"] {{
        padding-top: 0rem !important;
    }}

    /* Equal height columns */
    [data-testid="column"] > div {{
        height: 100%;
    }}

    /* Style all buttons with daily color theme */
    .stButton > button {{
        background-color: transparent !important;
        color: {daily_colors['primary']} !important;
        border: 2px solid {daily_colors['primary']} !important;
        border-radius: 0px !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        letter-spacing: 0.5px !important;
        transition: all 0.2s ease !important;
    }}
    .stButton > button:hover {{
        background-color: {daily_colors['primary']} !important;
        color: {daily_colors['background']} !important;
        transform: scale(1.02) !important;
    }}

    /* Primary buttons */
    .stButton > button[kind="primary"] {{
        background-color: {daily_colors['primary']} !important;
        color: {daily_colors['background']} !important;
        border: 2px solid {daily_colors['primary']} !important;
    }}
    .stButton > button[kind="primary"]:hover {{
        background-color: {daily_colors['light']} !important;
        border-color: {daily_colors['light']} !important;
    }}

    /* Form submit button */
    .stFormSubmitButton > button {{
        background-color: {daily_colors['primary']} !important;
        color: {daily_colors['background']} !important;
        border: 2px solid {daily_colors['primary']} !important;
        border-radius: 0px !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 700 !important;
        letter-spacing: 1px !important;
    }}
    .stFormSubmitButton > button:hover {{
        background-color: {daily_colors['light']} !important;
        border-color: {daily_colors['light']} !important;
        transform: scale(1.02) !important;
    }}

    /* Multiselect styling */
    .stMultiSelect [data-baseweb="tag"] {{
        background-color: {daily_colors['primary']} !important;
        color: {daily_colors['background']} !important;
    }}
    .stMultiSelect [data-baseweb="select"] > div {{
        border-color: {daily_colors['primary']} !important;
    }}

    /* KPI Card styling */
    .kpi-card {{
        background: #1a1a1a !important;
        padding: 2rem 2.5rem;
        border-radius: 0px !important;
        border-left: 6px solid {daily_colors['primary']};
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        transition: all 0.3s ease;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        min-height: 180px;
    }}
    .kpi-card:hover {{
        transform: translateY(-4px);
        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.5);
        border-left-width: 8px;
    }}
    .kpi-label {{
        font-family: 'Inter', sans-serif;
        font-size: 0.9rem;
        font-weight: 700;
        color: #999999;
        text-transform: uppercase;
        letter-spacing: 2px;
        margin-bottom: 0.5rem;
    }}
    .kpi-number {{
        font-family: 'Inter', sans-serif;
        font-size: 5.5rem;
        font-weight: 900;
        color: {daily_colors['primary']};
        line-height: 1;
        margin: 0.5rem 0;
        text-shadow: 0 0 20px rgba({primary_rgb[0]}, {primary_rgb[1]}, {primary_rgb[2]}, 0.3);
    }}
    .kpi-subtitle {{
        font-family: 'Inter', sans-serif;
        font-size: 0.85rem;
        color: #cccccc;
        margin-top: 0.5rem;
    }}

    @media (max-width: 768px) {{
        .kpi-card {{
            padding: 1rem 1.25rem;
            margin-bottom: 1rem;
            min-height: 140px;
        }}
        .kpi-label {{
            font-size: 0.75rem;
            letter-spacing: 1px;
            margin-bottom: 0.25rem;
        }}
        .kpi-number {{
            font-size: 2.5rem;
            margin: 0.25rem 0;
        }}
        .kpi-subtitle {{
            font-size: 0.7rem;
            margin-top: 0.25rem;
        }}
    }}

    /* Member tile styling with CSS tooltip */
    .member-tile {{
        display: inline-block;
        padding: 0.5rem 1rem;
        margin: 0.25rem;
        border: 1px solid;
        border-radius: 4px;
        font-size: 0.85rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        cursor: pointer;
        position: relative;
        transition: all 0.2s ease;
    }}

    .member-tile:hover {{
        transform: scale(1.05);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
    }}

    /* Tooltip styling */
    .member-tile::after {{
        content: attr(data-tooltip);
        position: absolute;
        bottom: 125%;
        left: 50%;
        transform: translateX(-50%);
        background-color: #2a2a2a;
        color: #ffffff;
        padding: 0.5rem 0.75rem;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 400;
        text-transform: none;
        letter-spacing: normal;
        white-space: nowrap;
        border: 1px solid #444;
        opacity: 0;
        visibility: hidden;
        transition: opacity 0.2s ease, visibility 0.2s ease;
        pointer-events: none;
        z-index: 1000;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.5);
    }}

    .member-tile::before {{
        content: '';
        position: absolute;
        bottom: 115%;
        left: 50%;
        transform: translateX(-50%);
        border: 5px solid transparent;
        border-top-color: #2a2a2a;
        opacity: 0;
        visibility: hidden;
        transition: opacity 0.2s ease, visibility 0.2s ease;
        pointer-events: none;
        z-index: 1000;
    }}

    .member-tile:hover::after,
    .member-tile:hover::before {{
        opacity: 1;
        visibility: visible;
    }}

    /* Monthly attendance matrix — status colors match KPI / member-tile accents */
    .monthly-attendance-table-wrap {{
        overflow-x: auto;
        margin: 0.35rem 0 1.25rem 0;
        width: 100%;
    }}
    .monthly-attendance-table {{
        width: 100%;
        border-collapse: collapse;
        font-family: 'Inter', sans-serif;
        font-size: 0.9rem;
    }}
    .monthly-attendance-table th {{
        text-align: left;
        padding: 0.65rem 0.75rem;
        border-bottom: 2px solid rgba(255, 255, 255, 0.12);
        color: #999;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-size: 0.72rem;
        white-space: nowrap;
    }}
    .monthly-attendance-table td {{
        padding: 0.55rem 0.75rem;
        border-bottom: 1px solid rgba(255, 255, 255, 0.06);
        color: #e8e8e8;
    }}
    .monthly-attendance-table th:nth-child(1),
    .monthly-attendance-table td:nth-child(1),
    .monthly-attendance-table th:nth-child(2),
    .monthly-attendance-table td:nth-child(2) {{
        max-width: 7.5rem;
        width: 1%;
        overflow: hidden;
        vertical-align: top;
    }}
    .monthly-attendance-table .monthly-trunc-details {{
        max-width: 100%;
    }}
    .monthly-attendance-table .monthly-trunc-summary {{
        cursor: pointer;
        list-style: none;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        max-width: 100%;
        color: #e0e0e0;
        font-weight: 400;
        font-size: inherit;
        text-transform: none;
        border-bottom: none;
        letter-spacing: normal;
    }}
    .monthly-attendance-table .monthly-trunc-summary::-webkit-details-marker {{
        display: none;
    }}
    .monthly-attendance-table .monthly-trunc-full {{
        display: block;
        margin-top: 0.35rem;
        padding-top: 0.35rem;
        border-top: 1px solid rgba(255, 255, 255, 0.1);
        color: #e0e0e0;
        font-weight: 400;
        white-space: normal;
        word-break: break-word;
        line-height: 1.3;
        text-transform: none;
    }}
    .monthly-attendance-table th:nth-child(3),
    .monthly-attendance-table td:nth-child(3) {{
        max-width: 5.75rem;
        width: 1%;
        white-space: nowrap;
        padding-left: 0.45rem;
        padding-right: 0.45rem;
        font-size: 0.82rem;
    }}
    .monthly-attendance-table th:nth-child(n+4),
    .monthly-attendance-table td:nth-child(n+4) {{
        max-width: 3.75rem;
        width: 1%;
        white-space: nowrap;
        text-align: center;
        padding: 0.45rem 0.35rem;
        font-size: 0.82rem;
    }}
    .monthly-attendance-table .monthly-attendance-rate-cell span {{
        font-weight: 700;
    }}
    .monthly-status-regular {{
        color: #2ecc71;
        font-weight: 700;
    }}
    .monthly-status-irregular {{
        color: #e67e22;
        font-weight: 700;
    }}
    .monthly-status-followup {{
        color: #f39c12;
        font-weight: 700;
    }}
    /* Health column: sheet / tile statuses (match member-tile border colors) */
    .monthly-health-tile-new {{
        color: #3498db;
        font-weight: 700;
    }}
    .monthly-health-tile-red {{
        color: #e74c3c;
        font-weight: 700;
    }}
    .monthly-health-tile-graduated {{
        color: #9b59b6;
        font-weight: 700;
    }}

    /* Newcomer list — same expand/ellipsis pattern as monthly Cell/Member columns */
    .newcomer-list-table-wrap {{
        overflow-x: auto;
        margin: 0.35rem 0 1.25rem 0;
        width: 100%;
    }}
    .newcomer-list-table {{
        width: 100%;
        border-collapse: collapse;
        font-family: 'Inter', sans-serif;
        font-size: 0.9rem;
    }}
    .newcomer-list-table th {{
        text-align: left;
        padding: 0.65rem 0.75rem;
        border-bottom: 2px solid rgba(255, 255, 255, 0.12);
        color: #999;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-size: 0.72rem;
        white-space: nowrap;
    }}
    .newcomer-list-table td {{
        padding: 0.55rem 0.75rem;
        border-bottom: 1px solid rgba(255, 255, 255, 0.06);
        color: #e8e8e8;
        vertical-align: top;
    }}
    .newcomer-list-table td.newcomer-trunc-cell {{
        max-width: 10rem;
        width: 1%;
        overflow: hidden;
    }}
    .newcomer-list-table .newcomer-trunc-details {{
        max-width: 100%;
    }}
    .newcomer-list-table .newcomer-trunc-summary {{
        cursor: pointer;
        list-style: none;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        max-width: 100%;
        color: #e0e0e0;
        font-weight: 400;
        font-size: inherit;
        text-transform: none;
        border-bottom: none;
        letter-spacing: normal;
    }}
    .newcomer-list-table .newcomer-trunc-summary::-webkit-details-marker {{
        display: none;
    }}
    .newcomer-list-table .newcomer-trunc-full {{
        display: block;
        margin-top: 0.35rem;
        padding-top: 0.35rem;
        border-top: 1px solid rgba(255, 255, 255, 0.1);
        color: #e0e0e0;
        font-weight: 400;
        white-space: normal;
        word-break: break-word;
        line-height: 1.3;
        text-transform: none;
    }}

</style>
""", unsafe_allow_html=True)

# Main app content
st.title("🏥 NWST Health")

# Get page from query parameters
query_params = st.query_params
current_page = _qp_first(query_params.get("page"), "cg")

# Page navigation buttons
st.markdown(f"""
<style>
    .health-tabs {{
        display: flex;
        gap: 0;
        margin-bottom: 1rem;
    }}
    .health-tab-btn {{
        flex: 1;
    }}
</style>
""", unsafe_allow_html=True)

tab_col1, tab_col2, tab_col3 = st.columns(3)
with tab_col1:
    cg_active = current_page == "cg"
    if st.button(
        "CG Health",
        type="primary" if cg_active else "secondary",
        use_container_width=True,
        key="tab_cg",
        disabled=cg_active
    ):
        st.query_params["page"] = "cg"
        st.rerun()

with tab_col2:
    ministry_active = current_page == "ministry"
    if st.button(
        "Ministry Health",
        type="primary" if ministry_active else "secondary",
        use_container_width=True,
        key="tab_ministry",
        disabled=ministry_active
    ):
        st.query_params["page"] = "ministry"
        st.rerun()

with tab_col3:
    analytics_active = current_page == "analytics"
    if st.button(
        "Analytics",
        type="primary" if analytics_active else "secondary",
        use_container_width=True,
        key="tab_analytics",
        disabled=analytics_active
    ):
        st.query_params["page"] = "analytics"
        st.rerun()

st.markdown("---")

# ========== CG HEALTH PAGE ==========
if current_page == "cg":
    # Sync button and status
    sync_col1, sync_col2, sync_col3 = st.columns([1, 2, 1])
    with sync_col2:
        if st.button("🔄 Sync from Google Sheets", use_container_width=True):
            client = get_google_sheet_client()
            if not client:
                st.error("❌ Google credentials not configured. Please add 'google' to your Streamlit secrets.")
            else:
                try:
                    spreadsheet = client.open_by_key("1uexbQinWl1r6NgmSrmOXPtWs-q4OJV3o1OwLywMWzzY")

                    # Sync CG Combined data
                    worksheet = spreadsheet.worksheet("CG Combined")
                    data = worksheet.get_all_values()

                    if data:
                        df = pd.DataFrame(data[1:], columns=data[0])

                        # Cache in Redis
                        redis = get_redis_client()
                        if redis:
                            cache_data = {
                                "columns": df.columns.tolist(),
                                "rows": df.values.tolist()
                            }
                            redis.set("nwst_cg_combined_data", json.dumps(cache_data), ex=300)

                        # Sync Ministries Combined data
                        try:
                            ministries_worksheet = spreadsheet.worksheet("Ministries Combined")
                            ministries_data = ministries_worksheet.get_all_values()

                            if ministries_data:
                                ministries_df = pd.DataFrame(ministries_data[1:], columns=ministries_data[0])

                                # Cache in Redis
                                redis = get_redis_client()
                                if redis:
                                    cache_data = {
                                        "columns": ministries_df.columns.tolist(),
                                        "rows": ministries_df.values.tolist()
                                    }
                                    redis.set("nwst_ministries_combined_data", json.dumps(cache_data), ex=300)
                        except Exception as e:
                            st.warning(f"⚠️ Could not sync Ministries data: {e}")

                        # Sync Attendance data
                        try:
                            att_worksheet = spreadsheet.worksheet("Attendance")
                            att_data = att_worksheet.get_all_values()

                            if att_data and len(att_data) >= 2:
                                att_headers = att_data[0]
                                att_df = pd.DataFrame(att_data[1:], columns=att_headers)

                                # Load CG Combined to get Name and Cell mapping
                                cg_worksheet = spreadsheet.worksheet("CG Combined")
                                cg_data = cg_worksheet.get_all_values()
                                if cg_data and len(cg_data) >= 2:
                                    cg_headers = cg_data[0]
                                    cg_df = pd.DataFrame(cg_data[1:], columns=cg_headers)

                                    # Find name and cell columns in CG Combined
                                    cg_name_col = None
                                    cg_cell_col = None
                                    for col in cg_df.columns:
                                        if col.lower().strip() in ['name', 'member name', 'member']:
                                            cg_name_col = col
                                        if col.lower().strip() in ['cell', 'group']:
                                            cg_cell_col = col

                                    if not cg_name_col:
                                        cg_name_col = cg_df.columns[0]

                                    # Calculate attendance stats using Name + Cell key
                                    attendance_stats = {}

                                    # Find name column in attendance (usually column A)
                                    att_name_col = att_df.columns[0] if len(att_df.columns) > 0 else None

                                    # Create a mapping of attendance names from column A only
                                    if att_name_col:
                                        for att_name in att_df[att_name_col].unique():
                                            if pd.isna(att_name) or att_name == '':
                                                continue

                                            att_name_str = str(att_name).strip()
                                            member_att_data = att_df[att_df[att_name_col] == att_name]

                                            # Count attendance only from columns D onwards (skip A, B, C)
                                            attendance_count = 0
                                            total_services = 0

                                            for col_idx, col in enumerate(att_df.columns):
                                                if col_idx >= 3:  # Skip columns A (0), B (1), C (2)
                                                    total_services += 1
                                                    values = member_att_data[col].values
                                                    if len(values) > 0 and str(values[0]).strip() == '1':
                                                        attendance_count += 1

                                            # Find the cell for this person from CG Combined
                                            cell_info = ""
                                            if cg_name_col and cg_cell_col:
                                                cg_match = cg_df[cg_df[cg_name_col].str.strip().str.lower() == att_name_str.lower()]
                                                if not cg_match.empty:
                                                    cell_info = " - " + str(cg_match[cg_cell_col].iloc[0]).strip()

                                            # Use Name + Cell as key
                                            if total_services > 0:
                                                key = att_name_str + cell_info
                                                attendance_stats[key] = {
                                                    'attendance': attendance_count,
                                                    'total': total_services,
                                                    'percentage': round(attendance_count / total_services * 100) if total_services > 0 else 0
                                                }

                                # Cache attendance stats in Redis
                                if redis:
                                    redis.set("nwst_attendance_stats", json.dumps(attendance_stats), ex=300)
                        except Exception as e:
                            st.warning(f"⚠️ Could not sync Attendance data: {e}")

                        if redis:
                            st.success("✅ Attendance updated successfully!")

                            # Store last sync time in Malaysian time
                            myt = timezone(timedelta(hours=8))
                            sync_time_myt = datetime.now(myt)
                            sync_time_str = sync_time_myt.strftime("%Y-%m-%d %H:%M:%S MYT")
                            redis.set("nwst_last_sync_time", sync_time_str)
                            checkin_sid = (CHECKIN_ATTENDANCE_SHEET_ID or "").strip()
                            if checkin_sid and client:
                                try:
                                    if _nwst_accent_cfg_mod is None:
                                        _accent_overrides_from_project_config()
                                    if _nwst_accent_cfg_mod:
                                        _nwst_accent_cfg_mod.refresh_theme_override_shared_cache(
                                            redis, client, checkin_sid
                                        )
                                except Exception:
                                    pass
                        else:
                            st.warning("⚠️ Redis not configured, but data loaded from Google Sheets.")

                        if redis:
                            try:
                                redis.delete(NWST_REDIS_ATTENDANCE_CHART_GRID_KEY)
                            except Exception:
                                pass

                        # Clear cache to force reload
                        st.cache_data.clear()
                    else:
                        st.error(
                            "No data found in the **CG Combined** tab. "
                            "Check that: (1) the tab exists and is named exactly 'CG Combined', "
                            "(2) it has at least a header row and data rows, "
                            "(3) the service account has Editor access to the spreadsheet."
                        )
                except Exception as e:
                    st.error(f"Error syncing data: {e}")

    st.markdown("---")

    # Display last sync time
    redis = get_redis_client()
    if redis:
        try:
            last_sync = redis.get("nwst_last_sync_time")
            if last_sync:
                st.markdown(f"<p style='text-align: center; color: #999; font-size: 0.85rem; margin-top: -0.5rem;'>Last synced: {last_sync}</p>", unsafe_allow_html=True)
        except Exception:
            pass

    st.markdown("")
    try:
        newcomers_df = get_newcomers_data()
        attendance_stats = get_attendance_data()

        if not newcomers_df.empty:
            # Get unique cell names for filtering
            cell_columns = [col for col in newcomers_df.columns if 'cell' in col.lower() or 'group' in col.lower()]

            # Build cell filter options
            cell_options = ["All"]
            if cell_columns:
                unique_cells = sorted(newcomers_df[cell_columns[0]].unique().tolist())
                cell_options.extend(unique_cells)

            # Filter section with dynamic options
            cell_filter = st.selectbox(
                "Cell",
                options=cell_options,
                key="global_cell_filter",
            )

            st.markdown("---")

            # Apply filters
            display_df = newcomers_df.copy()

            # Apply cell filter
            if cell_filter != "All" and cell_columns:
                display_df = display_df[display_df[cell_columns[0]] == cell_filter]

            # CELL HEALTH — quick view (Historical Cell Status WoW + live CG Combined mix)
            _render_cg_cell_health_section(display_df, daily_colors, cell_filter, attendance_stats)

            with st.expander("👤 INDIVIDUAL ATTENDANCE", expanded=False):
                if not display_df.empty:
                    st.markdown("")
                    att_df_m, cg_df_m = load_attendance_and_cg_dataframes()
                    if cg_df_m is not None:
                        if att_df_m is None:
                            att_df_m = pd.DataFrame()
                        status_hist_df = load_status_historical_dataframe()
                        monthly_status_df = build_monthly_member_status_table(
                            display_df, att_df_m, cg_df_m, status_hist_df
                        )
                        if monthly_status_df is not None and not monthly_status_df.empty:
                            st.markdown(
                                f"""
                                <style>
                                    [data-testid="stMultiSelect"] {{
                                        font-family: 'Inter', sans-serif !important;
                                    }}
                                    [data-testid="stMultiSelect"] > div {{
                                        border: 2px solid {daily_colors['primary']} !important;
                                        border-radius: 0px !important;
                                        background: {daily_colors['background']} !important;
                                    }}
                                    [data-testid="stMultiSelect"] span {{
                                        font-family: 'Inter', sans-serif !important;
                                        color: #ffffff !important;
                                    }}
                                    [data-testid="stMultiSelect"] svg {{
                                        fill: {daily_colors['primary']} !important;
                                    }}
                                    [data-testid="stMultiSelect"] [data-baseweb="tag"] {{
                                        background: {daily_colors['primary']} !important;
                                        border-radius: 0px !important;
                                    }}
                                    [data-testid="stMultiSelect"] [data-baseweb="tag"] span {{
                                        color: {daily_colors['background']} !important;
                                        font-weight: 600 !important;
                                    }}
                                </style>
                                """,
                                unsafe_allow_html=True,
                            )

                            if "monthly_health_clear_filter_counter" not in st.session_state:
                                st.session_state.monthly_health_clear_filter_counter = 0
                            _mh_fc = st.session_state.monthly_health_clear_filter_counter

                            _all_names_mh = sorted(
                                monthly_status_df["Member"].dropna().astype(str).str.strip().unique().tolist(),
                                key=str.lower,
                            )
                            _all_names_mh = [n for n in _all_names_mh if n]

                            _mh_ncol1, _mh_ncol2 = st.columns([3, 1])
                            with _mh_ncol1:
                                _sel_names_mh = st.multiselect(
                                    "Search by Name...",
                                    options=_all_names_mh,
                                    default=[],
                                    key=f"monthly_health_name_multiselect_{_mh_fc}",
                                    placeholder="Search and select names...",
                                    label_visibility="collapsed",
                                )
                            with _mh_ncol2:
                                if st.button(
                                    "Clear All",
                                    type="secondary",
                                    use_container_width=True,
                                    key="monthly_health_clear_filters",
                                ):
                                    st.session_state.monthly_health_clear_filter_counter += 1
                                    st.session_state.cg_cell_health_tile_filter = None
                                    st.rerun()

                            _filtered_monthly = monthly_status_df.copy()
                            if _sel_names_mh:
                                _mem_f = _filtered_monthly["Member"].dropna().astype(str).str.strip()
                                _filtered_monthly = _filtered_monthly[
                                    _mem_f.isin(_sel_names_mh)
                                ]

                            _ch_tile_f = st.session_state.get("cg_cell_health_tile_filter")
                            if _ch_tile_f and "_tile_status" in _filtered_monthly.columns:
                                _filtered_monthly = _filtered_monthly[
                                    _filtered_monthly["_tile_status"] == _ch_tile_f
                                ]

                            _mh_filter_parts = []
                            if _sel_names_mh:
                                _mh_filter_parts.append(f"{len(_sel_names_mh)} name(s)")
                            if _ch_tile_f:
                                _mh_filter_parts.append(f"status: {_ch_tile_f}")
                            _mh_filter_text = (
                                f" from {' and '.join(_mh_filter_parts)}" if _mh_filter_parts else ""
                            )
                            st.markdown(
                                f"<p style='color: #999999; font-family: Inter, sans-serif; font-size: 0.9rem; margin: 1rem 0 0.5rem 0;'>Showing <b style=\"color: {daily_colors['primary']}\">{len(_filtered_monthly)}</b> members{_mh_filter_text}</p>",
                                unsafe_allow_html=True,
                            )

                            if _filtered_monthly.empty:
                                st.info("No members match the current filters.")
                            else:
                                _mwf = _filtered_monthly.copy()
                                _mwf["_monthly_tab_cell"] = (
                                    _mwf["Cell"]
                                    .fillna("")
                                    .astype(str)
                                    .str.strip()
                                    .replace("", "(no cell)")
                                )
                                _cells_for_tabs = sorted(
                                    _mwf["_monthly_tab_cell"].unique().tolist(),
                                    key=str.lower,
                                )
                                # Name or Cell Health tile filter: one table so matches are not split across tabs.
                                if _sel_names_mh or _ch_tile_f:
                                    st.markdown(
                                        render_monthly_status_html_table(_filtered_monthly),
                                        unsafe_allow_html=True,
                                    )
                                elif cell_filter == "All" and len(_cells_for_tabs) > 1:
                                    _mh_cell_tabs = st.tabs(_cells_for_tabs)
                                    for _ti, _cell_name in enumerate(_cells_for_tabs):
                                        with _mh_cell_tabs[_ti]:
                                            _sub = _mwf[_mwf["_monthly_tab_cell"] == _cell_name].drop(
                                                columns=["_monthly_tab_cell"]
                                            )
                                            st.markdown(
                                                render_monthly_status_html_table(_sub),
                                                unsafe_allow_html=True,
                                            )
                                else:
                                    st.markdown(
                                        render_monthly_status_html_table(_filtered_monthly),
                                        unsafe_allow_html=True,
                                    )
                        else:
                            st.info(
                                "No individual attendance breakdown yet. Check that Attendance row 1 from column D has parseable dates "
                                "(e.g. DD/MM/YYYY or MM/DD/YYYY)."
                            )
                    else:
                        st.info("Could not load the Attendance sheet for the individual attendance table.")
                else:
                    st.info("No member data to show individual attendance.")

            with st.expander("📈 CELL ATTENDANCE", expanded=False):
                st.markdown("")
                if display_df is None or display_df.empty:
                    st.info("No member data to show cell attendance charts.")
                else:
                    render_nwst_service_attendance_rate_charts(
                        display_df,
                        daily_colors,
                        tab_each_cell_when_all=(cell_filter == "All"),
                    )


            with st.expander("👥 NEWCOMER", expanded=False):
                _render_cg_newcomer_section(newcomers_df, display_df, cell_filter, cell_columns, daily_colors)

            with st.expander("👔 LEADERSHIP", expanded=False):
                _render_cg_leadership_section(display_df, cell_filter, cell_columns, daily_colors)

            # DETAILED MEMBERS SECTION
            with st.expander("📋 DETAILED MEMBERS", expanded=False):
                _render_cg_detailed_members_section(display_df, daily_colors)

        else:
            st.warning("No data found. Click 'Sync from Google Sheets' to load data.")

    except Exception as e:
        st.error(f"Error loading data: {e}")

# ========== MINISTRY HEALTH PAGE ==========
elif current_page == "ministry":
    st.markdown("")
    try:
        ministries_df = get_ministries_data()

        if not ministries_df.empty:
            # Get unique ministry names for filtering
            ministry_columns = [col for col in ministries_df.columns if 'ministry' in col.lower() or 'department' in col.lower()]

            # Extract base ministry names (part before colon, or full value if no colon)
            base_ministries = set()
            if ministry_columns:
                for entry in ministries_df[ministry_columns[0]]:
                    if pd.notna(entry):
                        entry_str = str(entry).strip()
                        # Extract base ministry name (before the colon)
                        base_ministry = entry_str.split(":", 1)[0].strip()
                        if base_ministry:
                            base_ministries.add(base_ministry)

            # Build ministry filter options
            ministry_options = ["All"] + sorted(list(base_ministries))

            # Filter section with dynamic options
            filter_col1, filter_col2 = st.columns(2)

            with filter_col1:
                ministry_filter = st.selectbox(
                    "Ministry",
                    options=ministry_options,
                    key="global_ministry_filter"
                )

            # If Worship is selected, show department filter
            department_filter = "All"
            if ministry_filter == "Worship":
                with filter_col2:
                    # Extract departments from Worship entries (format: "Worship: Department Name")
                    worship_entries = ministries_df[ministries_df[ministry_columns[0]].str.contains("Worship", na=False, case=False)][ministry_columns[0]]
                    departments = set()
                    for entry in worship_entries:
                        if ":" in str(entry):
                            dept = str(entry).split(":", 1)[1].strip()
                            departments.add(dept)

                    department_options = ["All"] + sorted(list(departments))
                    department_filter = st.selectbox(
                        "Department",
                        options=department_options,
                        key="department_filter"
                    )
            else:
                filter_col2.write("")

            st.markdown("---")

            # Apply filters
            display_ministry_df = ministries_df.copy()

            # Apply ministry filter
            if ministry_filter != "All" and ministry_columns:
                if ministry_filter == "Worship":
                    # For Worship, include all entries that start with "Worship"
                    display_ministry_df = display_ministry_df[display_ministry_df[ministry_columns[0]].str.contains("^Worship", na=False, case=False, regex=True)]
                    # Apply department filter if specified
                    if department_filter != "All":
                        display_ministry_df = display_ministry_df[display_ministry_df[ministry_columns[0]].str.contains(f"Worship: {department_filter}", na=False, case=False)]
                else:
                    # For other ministries, match entries that start with the ministry name but have no department
                    display_ministry_df = display_ministry_df[display_ministry_df[ministry_columns[0]].str.match(f"^{ministry_filter}$", na=False, case=False)]

            # LEADERSHIP SECTION
            st.markdown("")
            st.markdown(f"<h2 style='color: {daily_colors['primary']}; font-weight: 900;'>👔 LEADERSHIP</h2>", unsafe_allow_html=True)
            st.markdown(f"<div style='height: 3px; background: {daily_colors['primary']}; margin-bottom: 1.5rem;'></div>", unsafe_allow_html=True)

            if not display_ministry_df.empty:
                # Get leadership members from data
                leadership_data_m = get_leadership_by_role(display_ministry_df)

                if leadership_data_m:
                    # Calculate total leaders and percentage
                    total_leaders_m = sum(len(members) for members in leadership_data_m.values())

                    # Use the already filtered dataframe
                    total_in_ministry = len(display_ministry_df)

                    leader_pct_m = (total_leaders_m / total_in_ministry * 100) if total_in_ministry > 0 else 0

                    # Display two cards in columns
                    leader_kpi_col1_m, leader_kpi_col2_m = st.columns(2)

                    with leader_kpi_col1_m:
                        st.markdown(f"""
                        <div class="kpi-card">
                            <div class="kpi-label">Total Leaders</div>
                            <div class="kpi-number">{total_leaders_m}</div>
                        </div>
                        """, unsafe_allow_html=True)

                    with leader_kpi_col2_m:
                        st.markdown(f"""
                        <div class="kpi-card">
                            <div class="kpi-label">Leaders %</div>
                            <div class="kpi-number" style="color: {daily_colors['primary']};">{leader_pct_m:.0f}%</div>
                            <div class="kpi-subtitle">{total_leaders_m} of {total_in_ministry}</div>
                        </div>
                        """, unsafe_allow_html=True)

                    st.markdown("")

                    # Display leadership organized by role
                    for role_name, members in leadership_data_m.items():
                        st.markdown(f"<h3 style='color: {daily_colors['primary']}; font-size: 1.1rem;'>{role_name}</h3>", unsafe_allow_html=True)

                        for leader in members:
                            since_text = f"Since: {leader['since']}" if leader['since'] else "Since: Not available"
                            st.markdown(f"""
                            <div style='padding: 1rem; background: #1a1a1a; border-left: 3px solid {daily_colors['primary']}; margin-bottom: 0.75rem;'>
                                <p style='font-weight: 600; margin: 0;'>{leader['name']}</p>
                                <p style='font-size: 0.85rem; color: #999; margin: 0.25rem 0 0 0;'>{since_text}</p>
                            </div>
                            """, unsafe_allow_html=True)

                        st.markdown("")
                else:
                    st.info("No leadership roles assigned yet.")
            else:
                st.info("No ministry data available.")
        else:
            st.warning("No ministries data found. Click 'Sync from Google Sheets' on the CG Health tab to load data.")

    except Exception as e:
        st.error(f"Error loading ministry data: {e}")

elif current_page == "analytics":
    nwst_analytics_colors = {
        "primary": daily_colors["primary"],
        "light": daily_colors["light"],
        "background": "#000000",
        "text": "#ffffff",
        "text_muted": "#999999",
        "card_bg": "#0a0a0a",
        "border": daily_colors["primary"],
    }
    st.markdown(
        f"""
    <div style="text-align: center; margin-bottom: 1.5rem;">
        <h1 style="font-family: 'Inter', sans-serif; font-weight: 900; font-size: 2.5rem;
                   color: {nwst_analytics_colors['primary']}; text-transform: uppercase; letter-spacing: 3px;
                   margin: 0; padding: 1rem 0;">
            Attendance Analytics
        </h1>
        <p style="color: {nwst_analytics_colors['text_muted']}; font-size: 0.9rem; margin: 0;">Saturday Service Attendance Trends</p>
    </div>
    """,
        unsafe_allow_html=True,
    )
    render_nwst_analytics_page(nwst_analytics_colors)
