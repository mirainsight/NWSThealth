import html
import streamlit as st
from datetime import datetime, timedelta, timezone
import colorsys
import hashlib
import random
import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials
import pandas as pd
import json
from collections import defaultdict
import plotly.express as px
from upstash_redis import Redis

# Same spreadsheet as CG Combined / Attendance (NWST Health)
NWST_HEALTH_SHEET_ID = "1uexbQinWl1r6NgmSrmOXPtWs-q4OJV3o1OwLywMWzzY"
NWST_KEY_VALUES_TAB = "Key Values"
NWST_ATTENDANCE_TAB = "Attendance"

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


@st.cache_data(ttl=300)
def nwst_get_attendance_grid_for_charts(sheet_id):
    """Load **Attendance** tab — Saturday columns only; cell from sheet or **CG Combined** name lookup."""
    client = get_google_sheet_client()
    if not client:
        return None, [], "Could not connect to Google Sheets."

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

            # Column A "Date" often holds "Full Name - Cell"; use if name/cell missing
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
                    1 if (col_indices[j] < len(row) and _nwst_attendance_present(row[col_indices[j]])) else 0
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
        return df, saturday_dates_short, None
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


def render_nwst_service_attendance_rate_charts(display_df, daily_colors):
    """Per-zone Saturday attendance rate lines — filtered by current display_df (global Cell / Status)."""
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
            for dc in date_cols:
                attended = int(sub[dc].sum()) if dc in sub.columns else 0
                pct = 100.0 * attended / mc
                long_rows.append(
                    {"Saturday": dc, "Cell Group": cg, "Attendance rate %": round(pct, 1)}
                )
        plot_df = pd.DataFrame(long_rows)
        if plot_df.empty:
            continue
        ymax = max(105.0, plot_df["Attendance rate %"].max() * 1.08)
        zone_plots[zone] = (plot_df, ymax)

    if not zone_plots:
        st.info("No cells to chart after filters.")
        return

    st.markdown(
        f"<h3 style='color: {daily_colors['primary']}; font-weight: 800; font-size: 1.15rem;'>"
        f"Cell Attendance</h3>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<p style='color: #999999; font-family: Inter, sans-serif; font-size: 0.85rem; margin: 0 0 0.75rem 0;'>"
        f"Uses your <b>Cell</b> and <b>Status</b> picks. Each line is one cell — each dot is "
        f"<b>the % of that cell who came</b> that Saturday, out of everyone in that cell on your list.</p>",
        unsafe_allow_html=True,
    )

    zone_tab_names = sorted(zone_plots.keys(), key=str.lower)
    for zone in zone_tab_names:
        plot_df, ymax = zone_plots[zone]
        if len(zone_tab_names) > 1:
            st.markdown(
                f"<p style='color: {daily_colors['primary']}; font-weight: 700; font-size: 1rem; margin: 0.75rem 0 0.35rem 0;'>"
                f"{zone}</p>",
                unsafe_allow_html=True,
            )
        n_lines = int(plot_df["Cell Group"].nunique())
        line_colors = _nwst_weekly_contrasting_line_colors(
            daily_colors["primary"], max(n_lines, 1)
        )
        fig = px.line(
            plot_df,
            x="Saturday",
            y="Attendance rate %",
            color="Cell Group",
            markers=True,
            title="",
            height=460,
            color_discrete_sequence=line_colors,
        )
        fig.update_traces(
            line=dict(width=3.5),
            marker=dict(size=5, line=dict(width=1, color="#FFFFFF"), opacity=1),
            hovertemplate=(
                "<b>%{fullData.name}</b><br>%{x}<br><b>%{y:.1f}%</b> of filtered cell showed up<extra></extra>"
            ),
        )
        fig.add_hline(
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
        fig.update_layout(
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
        st.plotly_chart(fig, use_container_width=True)

    st.caption(
        "Tip: follow one color across the weeks — rightmost dot is the latest Saturday."
    )


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
        if pd.isna(att_name) or att_name == '':
            continue

        att_name_str = str(att_name).strip()
        member_att_data = att_df[att_df[att_name_col] == att_name]

        attendance_count = 0
        total_services = 0

        for col_idx, col in enumerate(att_df.columns):
            if col_idx >= 3:
                total_services += 1
                values = member_att_data[col].values
                if len(values) > 0 and str(values[0]).strip() == '1':
                    attendance_count += 1

        cell_info = ""
        if cg_name_col and cg_cell_col:
            cg_match = cg_df[cg_df[cg_name_col].str.strip().str.lower() == att_name_str.lower()]
            if not cg_match.empty:
                cell_info = " - " + str(cg_match[cg_cell_col].iloc[0]).strip()

        if total_services > 0:
            key = att_name_str + cell_info
            attendance_stats[key] = {
                'attendance': attendance_count,
                'total': total_services,
                'percentage': round(attendance_count / total_services * 100) if total_services > 0 else 0
            }

    return attendance_stats


def categorize_member_status(attendance_count, total_possible):
    """Categorize member as Regular, Irregular, or Follow Up based on attendance."""
    if attendance_count >= (total_possible * 0.75):  # 75% and above attendance = Regular
        return "Regular"
    elif attendance_count > 0:  # Below 75% = Irregular
        return "Irregular"
    else:  # 0% attendance = Follow Up
        return "Follow Up"


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


def _attendance_row_lookup_key(row, att_name_col, cg_df, cg_name_col, cg_cell_col):
    att_name_str = str(row[att_name_col]).strip()
    cell_info = ""
    if cg_name_col and cg_cell_col:
        cg_match = cg_df[cg_df[cg_name_col].str.strip().str.lower() == att_name_str.lower()]
        if not cg_match.empty:
            cell_info = " - " + str(cg_match[cg_cell_col].iloc[0]).strip()
    return att_name_str + cell_info


def build_monthly_member_status_table(display_df, att_df, cg_df):
    """
    One row per member in display_df; columns Cell, Member, Health (present/total + rate %),
    then each Month (MMM YY) with Regular / Irregular / Follow Up.
    Months are those present in Attendance headers (cols D+), up to current calendar month (MYT).
    Health aggregates the same dated columns as the month grid (weeks marked 1 = present).
    Internal column _tile_status stores sheet status for Health cell coloring (same as tiles above).
    Rows are sorted alphabetically by member name (case-insensitive), then by cell for stable ties.
    """
    if display_df is None or display_df.empty or att_df is None or att_df.empty:
        return pd.DataFrame()

    cg_name_col, cg_cell_col = _resolve_cg_name_cell_columns(cg_df)
    att_name_col = att_df.columns[0] if len(att_df.columns) > 0 else None
    if not att_name_col:
        return pd.DataFrame()

    month_to_colnames = {}
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
    month_keys = sorted(ym for ym in month_to_colnames if ym <= cur_ym)
    if not month_keys:
        return pd.DataFrame()

    month_labels = []
    for y, m in month_keys:
        month_labels.append(datetime(y, m, 1).strftime("%b %y"))

    # Map stats key -> attendance row (first match)
    key_to_row = {}
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
        if att_row is None:
            for lbl in month_labels:
                out[lbl] = "—"
            out["Health"] = "—"
            rows_out.append(out)
            continue

        for ym, lbl in zip(month_keys, month_labels):
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


@st.cache_data(ttl=300)
def get_attendance_data():
    """Load attendance data from Google Sheet 'Attendance' tab using only column A."""
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

    redis = get_redis_client()
    if redis:
        try:
            redis.set("nwst_attendance_stats", json.dumps(attendance_stats), ex=300)
        except Exception:
            pass

    return attendance_stats


def get_attendance_text(name, cell, attendance_stats):
    """Get attendance text for a member from attendance_stats dict using Name + Cell."""
    if not attendance_stats:
        return name

    name_stripped = str(name).strip()
    cell_stripped = str(cell).strip() if cell else ""

    # Build the key: "Name - Cell"
    if cell_stripped:
        key = f"{name_stripped} - {cell_stripped}"
    else:
        key = name_stripped

    # Try exact match
    if key in attendance_stats:
        stats = attendance_stats[key]
        return f"{name} - {stats['attendance']}/{stats['total']} ({stats['percentage']}%)"

    # Try case-insensitive match
    key_lower = key.lower()
    for dict_key, stats in attendance_stats.items():
        if dict_key.lower() == key_lower:
            return f"{name} - {stats['attendance']}/{stats['total']} ({stats['percentage']}%)"

    return name

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
    """Generate random colors based on the most recent Saturday (MYT).
    Colors change every Saturday and stay the same throughout the week."""
    today = datetime.strptime(get_today_myt_date(), "%Y-%m-%d")
    days_since_saturday = (today.weekday() - 5) % 7
    last_saturday = today - timedelta(days=days_since_saturday)
    return generate_colors_for_date(last_saturday.strftime("%Y-%m-%d"))

# Page configuration
st.set_page_config(
    page_title="NWST Health",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Weekly accent theme (locked to most recent Saturday MYT, same as CHECK IN attendance app)
daily_colors = generate_daily_colors()

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

    /* Mobile responsive - smaller cards on small screens */
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
        color: #e8e8e8;
        font-weight: 500;
    }}
    .monthly-attendance-table .monthly-trunc-summary::-webkit-details-marker {{
        display: none;
    }}
    .monthly-attendance-table .monthly-trunc-full {{
        display: block;
        margin-top: 0.35rem;
        padding-top: 0.35rem;
        border-top: 1px solid rgba(255, 255, 255, 0.1);
        color: #ffffff;
        font-weight: 600;
        white-space: normal;
        word-break: break-word;
        line-height: 1.3;
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

</style>
""", unsafe_allow_html=True)

# Main app content
st.title("🏥 NWST Health")

# Get page from query parameters
query_params = st.query_params
current_page = query_params.get("page", "cg")

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

tab_col1, tab_col2 = st.columns(2)
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
                            st.success("✅ Data synced successfully! Cached for 5 minutes.")

                            # Store last sync time in Malaysian time
                            myt = timezone(timedelta(hours=8))
                            sync_time_myt = datetime.now(myt)
                            sync_time_str = sync_time_myt.strftime("%Y-%m-%d %H:%M:%S MYT")
                            redis.set("nwst_last_sync_time", sync_time_str)
                        else:
                            st.warning("⚠️ Redis not configured, but data loaded from Google Sheets.")

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
        attendance_stats = get_attendance_data()  # Load attendance data

        if not newcomers_df.empty:
            # Get unique cell names for filtering
            cell_columns = [col for col in newcomers_df.columns if 'cell' in col.lower() or 'group' in col.lower()]

            # Build cell filter options
            cell_options = ["All"]
            if cell_columns:
                unique_cells = sorted(newcomers_df[cell_columns[0]].unique().tolist())
                cell_options.extend(unique_cells)

            # Filter section with dynamic options
            st.markdown("#### Global Filters")
            filter_col1, filter_col2 = st.columns(2)

            with filter_col1:
                cell_filter = st.selectbox(
                    "Cell",
                    options=cell_options,
                    key="global_cell_filter"
                )

            with filter_col2:
                status_filter = st.selectbox(
                    "Status",
                    options=["All", "Active", "Inactive"],
                    key="status_filter"
                )

            st.markdown("---")

            # Apply filters
            display_df = newcomers_df.copy()

            # Apply cell filter
            if cell_filter != "All" and cell_columns:
                display_df = display_df[display_df[cell_columns[0]] == cell_filter]

            # Apply status filter if available
            if status_filter != "All":
                # Filter by status if there's a status column
                status_columns = [col for col in display_df.columns if 'status' in col.lower()]
                if status_columns:
                    display_df = display_df[display_df[status_columns[0]] == status_filter]

            # NEWCOMER SECTION
            st.markdown("")
            st.markdown(f"<h2 style='color: {daily_colors['primary']}; font-weight: 900;'>👥 NEWCOMER</h2>", unsafe_allow_html=True)
            st.markdown(f"<div style='height: 3px; background: {daily_colors['primary']}; margin-bottom: 1.5rem;'></div>", unsafe_allow_html=True)

            # Filter for New status
            status_columns = [col for col in newcomers_df.columns if 'status' in col.lower()]
            newcomer_df = newcomers_df.copy()
            if status_columns:
                newcomer_df = newcomer_df[newcomer_df[status_columns[0]] == "New"]

            # Apply cell filter to newcomers
            if cell_filter != "All" and cell_columns:
                newcomer_df = newcomer_df[newcomer_df[cell_columns[0]] == cell_filter]

            if not newcomer_df.empty:
                # Display count card first
                newcomer_count = len(newcomer_df)

                # Calculate total members and newcomer percentage
                if cell_filter != "All" and cell_columns:
                    total_in_cell = len(display_df[display_df[cell_columns[0]] == cell_filter])
                else:
                    total_in_cell = len(display_df)

                newcomer_pct = (newcomer_count / total_in_cell * 100) if total_in_cell > 0 else 0

                # Display two cards in columns
                kpi_col1, kpi_col2 = st.columns(2)

                with kpi_col1:
                    st.markdown(f"""
                    <div class="kpi-card">
                        <div class="kpi-label">Total Newcomers</div>
                        <div class="kpi-number">{newcomer_count}</div>
                    </div>
                    """, unsafe_allow_html=True)

                with kpi_col2:
                    st.markdown(f"""
                    <div class="kpi-card">
                        <div class="kpi-label">Newcomers %</div>
                        <div class="kpi-number" style="color: {daily_colors['primary']};">{newcomer_pct:.0f}%</div>
                        <div class="kpi-subtitle">{newcomer_count} of {total_in_cell}</div>
                    </div>
                    """, unsafe_allow_html=True)

                # Display newcomers list with: Name, Joined Date, Friend/Referrer, Source
                available_cols = newcomer_df.columns.tolist()

                # Find columns by type - only Name, Notes, and New Since by default
                default_cols = []
                for col in available_cols:
                    col_lower = col.lower()
                    # Name column - exclude columns with 'last' in them
                    if (any(x in col_lower for x in ['name', 'member']) and 'last' not in col_lower):
                        default_cols.append(col)
                    # Notes column
                    elif any(x in col_lower for x in ['notes', 'note']):
                        default_cols.append(col)
                    # New Since column - must explicitly contain "new since"
                    elif 'new since' in col_lower:
                        default_cols.append(col)

                # Column selection widget
                st.markdown("**Select columns to display:**")
                selected_cols = st.multiselect(
                    "Columns",
                    options=available_cols,
                    default=default_cols,
                    key="newcomer_columns",
                    label_visibility="collapsed"
                )

                st.markdown("#### Newcomer List")

                if selected_cols:
                    st.dataframe(newcomer_df[selected_cols], use_container_width=True, hide_index=True)
                else:
                    st.warning("Please select at least one column to display.")
            else:
                st.info("No newcomers found.")

            # CELL HEALTH SECTION
            st.markdown("")
            st.markdown(f"<h2 style='color: {daily_colors['primary']}; font-weight: 900;'>🏥 CELL HEALTH</h2>", unsafe_allow_html=True)
            st.markdown(f"<div style='height: 3px; background: {daily_colors['primary']}; margin-bottom: 1.5rem;'></div>", unsafe_allow_html=True)

            if not display_df.empty:
                # Find status column
                status_columns = [col for col in display_df.columns if 'status' in col.lower()]
                status_col = status_columns[0] if status_columns else None

                # Calculate counts by status - extracting the prefix from the descriptive status
                if status_col:
                    # Create a mapped status column (same rules as Monthly Health _tile_status)
                    display_df['status_type'] = display_df[status_col].apply(extract_cell_sheet_status_type)

                    new_count = len(display_df[display_df['status_type'] == "New"])
                    regular_count = len(display_df[display_df['status_type'] == "Regular"])
                    irregular_count = len(display_df[display_df['status_type'] == "Irregular"])
                    follow_up_count = len(display_df[display_df['status_type'] == "Follow Up"])
                    red_count = len(display_df[display_df['status_type'] == "Red"])
                    graduated_count = len(display_df[display_df['status_type'] == "Graduated"])
                else:
                    # Fallback if no status column
                    total_members = len(display_df)
                    new_count = max(1, int(total_members * 0.20))
                    regular_count = max(1, int(total_members * 0.40))
                    irregular_count = max(1, int(total_members * 0.20))
                    follow_up_count = max(1, int(total_members * 0.10))
                    red_count = max(1, int(total_members * 0.05))
                    graduated_count = total_members - new_count - regular_count - irregular_count - follow_up_count - red_count

                total_members = new_count + regular_count + irregular_count + follow_up_count + red_count + graduated_count

                regular_pct = (regular_count / total_members * 100) if total_members > 0 else 0
                irregular_pct = (irregular_count / total_members * 100) if total_members > 0 else 0
                new_pct = (new_count / total_members * 100) if total_members > 0 else 0
                follow_up_pct = (follow_up_count / total_members * 100) if total_members > 0 else 0
                red_pct = (red_count / total_members * 100) if total_members > 0 else 0
                graduated_pct = (graduated_count / total_members * 100) if total_members > 0 else 0

                # Member status row - make clickable to expand details
                col1, col2, col3 = st.columns(3)

                # Initialize session state for expanded states
                if 'expand_new' not in st.session_state:
                    st.session_state.expand_new = False
                if 'expand_regular' not in st.session_state:
                    st.session_state.expand_regular = False
                if 'expand_irregular' not in st.session_state:
                    st.session_state.expand_irregular = False
                if 'expand_follow_up' not in st.session_state:
                    st.session_state.expand_follow_up = False
                if 'expand_red' not in st.session_state:
                    st.session_state.expand_red = False
                if 'expand_graduated' not in st.session_state:
                    st.session_state.expand_graduated = False

                with col1:
                    if st.button(f"🔵 New", key="btn_new", use_container_width=True):
                        st.session_state.expand_new = not st.session_state.expand_new
                    st.markdown(f"""
                    <div class="kpi-card" style="cursor: pointer;">
                        <div class="kpi-label">New Members</div>
                        <div class="kpi-number" style="color: #3498db;">{new_pct:.0f}%</div>
                        <div class="kpi-subtitle">{new_count} members</div>
                    </div>
                    """, unsafe_allow_html=True)
                    if st.session_state.expand_new:
                        st.markdown(f"<p style='color: #3498db; font-weight: 600;'>New Members</p>", unsafe_allow_html=True)
                        if status_col:
                            new_data = display_df[display_df['status_type'] == "New"].copy()
                        else:
                            new_data = display_df.head(new_count).copy()
                        if 'name' in new_data.columns or 'Name' in new_data.columns:
                            name_col = 'name' if 'name' in new_data.columns else 'Name'
                            # Find cell/group column
                            cell_col = None
                            for col in new_data.columns:
                                if col.lower().strip() in ['cell', 'group']:
                                    cell_col = col
                                    break
                            # Find attendance columns
                            attendance_cols = [col for col in new_data.columns if any(x in col.lower() for x in ['attend', 'present', 'participation'])]

                            names = sorted(new_data[name_col].unique().tolist())
                            tiles_html = ""
                            for name in names:
                                # Get cell for this person
                                person_cell = ""
                                if cell_col:
                                    person_row = new_data[new_data[name_col] == name]
                                    if not person_row.empty:
                                        person_cell = person_row[cell_col].iloc[0]
                                # Get attendance text from attendance_stats
                                tooltip_text = get_attendance_text(name, person_cell, attendance_stats)

                                tiles_html += f"<span class='member-tile' style='border-color: #3498db;' data-tooltip='{tooltip_text}'>{name}</span> "

                            st.markdown(tiles_html, unsafe_allow_html=True)
                        else:
                            st.dataframe(new_data, use_container_width=True)

                with col2:
                    if st.button(f"🟢 Regular", key="btn_regular", use_container_width=True):
                        st.session_state.expand_regular = not st.session_state.expand_regular
                    st.markdown(f"""
                    <div class="kpi-card" style="cursor: pointer;">
                        <div class="kpi-label">Regular Members</div>
                        <div class="kpi-number" style="color: #2ecc71;">{regular_pct:.0f}%</div>
                        <div class="kpi-subtitle">{regular_count} members</div>
                    </div>
                    """, unsafe_allow_html=True)
                    if st.session_state.expand_regular:
                        st.markdown(f"<p style='color: #2ecc71; font-weight: 600;'>Regular Members (75% and above attendance)</p>", unsafe_allow_html=True)
                        if status_col:
                            regular_data = display_df[display_df['status_type'] == "Regular"].copy()
                        else:
                            regular_data = display_df.iloc[new_count:new_count+regular_count].copy()
                        if 'name' in regular_data.columns or 'Name' in regular_data.columns:
                            name_col = 'name' if 'name' in regular_data.columns else 'Name'
                            # Find cell/group column
                            cell_col = None
                            for col in regular_data.columns:
                                if col.lower().strip() in ['cell', 'group']:
                                    cell_col = col
                                    break
                            # Find attendance columns
                            attendance_cols = [col for col in regular_data.columns if any(x in col.lower() for x in ['attend', 'present', 'participation'])]

                            names = sorted(regular_data[name_col].unique().tolist())
                            tiles_html = ""
                            for name in names:
                                # Get cell for this person
                                person_cell = ""
                                if cell_col:
                                    person_row = regular_data[regular_data[name_col] == name]
                                    if not person_row.empty:
                                        person_cell = person_row[cell_col].iloc[0]
                                # Get attendance text from attendance_stats
                                tooltip_text = get_attendance_text(name, person_cell, attendance_stats)

                                tiles_html += f"<span class='member-tile' style='border-color: #2ecc71;' data-tooltip='{tooltip_text}'>{name}</span> "

                            st.markdown(tiles_html, unsafe_allow_html=True)
                        else:
                            st.dataframe(regular_data, use_container_width=True)

                with col3:
                    if st.button(f"🟠 Irregular", key="btn_irregular", use_container_width=True):
                        st.session_state.expand_irregular = not st.session_state.expand_irregular
                    st.markdown(f"""
                    <div class="kpi-card" style="cursor: pointer;">
                        <div class="kpi-label">Irregular Members</div>
                        <div class="kpi-number" style="color: #e67e22;">{irregular_pct:.0f}%</div>
                        <div class="kpi-subtitle">{irregular_count} members</div>
                    </div>
                    """, unsafe_allow_html=True)
                    if st.session_state.expand_irregular:
                        st.markdown(f"<p style='color: #e67e22; font-weight: 600;'>Irregular Members (Below 75% attendance)</p>", unsafe_allow_html=True)
                        if status_col:
                            irregular_data = display_df[display_df['status_type'] == "Irregular"].copy()
                        else:
                            irregular_data = display_df.iloc[new_count+regular_count:new_count+regular_count+irregular_count].copy()
                        if 'name' in irregular_data.columns or 'Name' in irregular_data.columns:
                            name_col = 'name' if 'name' in irregular_data.columns else 'Name'
                            # Find cell/group column
                            cell_col = None
                            for col in irregular_data.columns:
                                if col.lower().strip() in ['cell', 'group']:
                                    cell_col = col
                                    break
                            # Find attendance columns
                            attendance_cols = [col for col in irregular_data.columns if any(x in col.lower() for x in ['attend', 'present', 'participation'])]

                            names = sorted(irregular_data[name_col].unique().tolist())
                            tiles_html = ""
                            for name in names:
                                # Get cell for this person
                                person_cell = ""
                                if cell_col:
                                    person_row = irregular_data[irregular_data[name_col] == name]
                                    if not person_row.empty:
                                        person_cell = person_row[cell_col].iloc[0]
                                # Get attendance text from attendance_stats
                                tooltip_text = get_attendance_text(name, person_cell, attendance_stats)

                                tiles_html += f"<span class='member-tile' style='border-color: #e67e22;' data-tooltip='{tooltip_text}'>{name}</span> "

                            st.markdown(tiles_html, unsafe_allow_html=True)
                        else:
                            st.dataframe(irregular_data, use_container_width=True)

                # Second row - Status breakdown with expandable Follow Up and Red
                st.markdown("")
                col1, col2 = st.columns(2)

                with col1:
                    if st.button(f"🟡 Follow Up", key="btn_follow_up", use_container_width=True):
                        st.session_state.expand_follow_up = not st.session_state.expand_follow_up
                    st.markdown(f"""
                    <div class="kpi-card" style="cursor: pointer;">
                        <div class="kpi-label">Follow Up</div>
                        <div class="kpi-number" style="color: #f39c12;">{follow_up_pct:.0f}%</div>
                        <div class="kpi-subtitle">{follow_up_count} members</div>
                    </div>
                    """, unsafe_allow_html=True)
                    if st.session_state.expand_follow_up:
                        st.markdown(f"<p style='color: #f39c12; font-weight: 600;'>Follow Up (0% attendance - past 2 months)</p>", unsafe_allow_html=True)
                        if status_col:
                            follow_up_data = display_df[display_df['status_type'] == "Follow Up"].copy()
                        else:
                            follow_up_data = display_df.iloc[new_count+regular_count+irregular_count:new_count+regular_count+irregular_count+follow_up_count].copy()
                        if 'name' in follow_up_data.columns or 'Name' in follow_up_data.columns:
                            name_col = 'name' if 'name' in follow_up_data.columns else 'Name'
                            # Find cell/group column
                            cell_col = None
                            for col in follow_up_data.columns:
                                if col.lower().strip() in ['cell', 'group']:
                                    cell_col = col
                                    break
                            # Find attendance columns
                            attendance_cols = [col for col in follow_up_data.columns if any(x in col.lower() for x in ['attend', 'present', 'participation'])]

                            names = sorted(follow_up_data[name_col].unique().tolist())
                            tiles_html = ""
                            for name in names:
                                # Get cell for this person
                                person_cell = ""
                                if cell_col:
                                    person_row = follow_up_data[follow_up_data[name_col] == name]
                                    if not person_row.empty:
                                        person_cell = person_row[cell_col].iloc[0]
                                # Get attendance text from attendance_stats
                                tooltip_text = get_attendance_text(name, person_cell, attendance_stats)

                                tiles_html += f"<span class='member-tile' style='border-color: #f39c12;' data-tooltip='{tooltip_text}'>{name}</span> "

                            st.markdown(tiles_html, unsafe_allow_html=True)
                        else:
                            st.dataframe(follow_up_data, use_container_width=True)

                with col2:
                    if st.button(f"🔴 Red", key="btn_red", use_container_width=True):
                        st.session_state.expand_red = not st.session_state.expand_red
                    st.markdown(f"""
                    <div class="kpi-card" style="cursor: pointer;">
                        <div class="kpi-label">Red</div>
                        <div class="kpi-number" style="color: #e74c3c;">{red_pct:.0f}%</div>
                        <div class="kpi-subtitle">{red_count} members</div>
                    </div>
                    """, unsafe_allow_html=True)
                    if st.session_state.expand_red:
                        st.markdown(f"<p style='color: #e74c3c; font-weight: 600;'>Red (Won't come to church anymore)</p>", unsafe_allow_html=True)
                        if status_col:
                            red_data = display_df[display_df['status_type'] == "Red"].copy()
                        else:
                            red_data = display_df.iloc[new_count+regular_count+irregular_count+follow_up_count:new_count+regular_count+irregular_count+follow_up_count+red_count].copy()
                        if 'name' in red_data.columns or 'Name' in red_data.columns:
                            name_col = 'name' if 'name' in red_data.columns else 'Name'
                            # Find cell/group column
                            cell_col = None
                            for col in red_data.columns:
                                if col.lower().strip() in ['cell', 'group']:
                                    cell_col = col
                                    break
                            # Find attendance columns
                            attendance_cols = [col for col in red_data.columns if any(x in col.lower() for x in ['attend', 'present', 'participation'])]

                            names = sorted(red_data[name_col].unique().tolist())
                            tiles_html = ""
                            for name in names:
                                # Get cell for this person
                                person_cell = ""
                                if cell_col:
                                    person_row = red_data[red_data[name_col] == name]
                                    if not person_row.empty:
                                        person_cell = person_row[cell_col].iloc[0]
                                # Get attendance text from attendance_stats
                                tooltip_text = get_attendance_text(name, person_cell, attendance_stats)

                                tiles_html += f"<span class='member-tile' style='border-color: #e74c3c;' data-tooltip='{tooltip_text}'>{name}</span> "

                            st.markdown(tiles_html, unsafe_allow_html=True)
                        else:
                            st.dataframe(red_data, use_container_width=True)

                # Third row - Graduated
                st.markdown("")
                col1, col2 = st.columns(2)

                with col1:
                    if st.button(f"⭐ Graduated", key="btn_graduated", use_container_width=True):
                        st.session_state.expand_graduated = not st.session_state.expand_graduated
                    st.markdown(f"""
                    <div class="kpi-card" style="cursor: pointer;">
                        <div class="kpi-label">Graduated</div>
                        <div class="kpi-number" style="color: #9b59b6;">{graduated_pct:.0f}%</div>
                        <div class="kpi-subtitle">{graduated_count} members</div>
                    </div>
                    """, unsafe_allow_html=True)
                    if st.session_state.expand_graduated:
                        st.markdown(f"<p style='color: #9b59b6; font-weight: 600;'>Graduated (Moved to leadership roles)</p>", unsafe_allow_html=True)
                        if status_col:
                            graduated_data = display_df[display_df['status_type'] == "Graduated"].copy()
                        else:
                            graduated_data = display_df.iloc[new_count+regular_count+irregular_count+follow_up_count+red_count:].copy()
                        if 'name' in graduated_data.columns or 'Name' in graduated_data.columns:
                            name_col = 'name' if 'name' in graduated_data.columns else 'Name'
                            # Find cell/group column
                            cell_col = None
                            for col in graduated_data.columns:
                                if col.lower().strip() in ['cell', 'group']:
                                    cell_col = col
                                    break
                            # Find attendance columns
                            attendance_cols = [col for col in graduated_data.columns if any(x in col.lower() for x in ['attend', 'present', 'participation'])]

                            names = sorted(graduated_data[name_col].unique().tolist())
                            tiles_html = ""
                            for name in names:
                                # Get cell for this person
                                person_cell = ""
                                if cell_col:
                                    person_row = graduated_data[graduated_data[name_col] == name]
                                    if not person_row.empty:
                                        person_cell = person_row[cell_col].iloc[0]
                                # Get attendance text from attendance_stats
                                tooltip_text = get_attendance_text(name, person_cell, attendance_stats)

                                tiles_html += f"<span class='member-tile' style='border-color: #9b59b6;' data-tooltip='{tooltip_text}'>{name}</span> "

                            st.markdown(tiles_html, unsafe_allow_html=True)
                        else:
                            st.dataframe(graduated_data, use_container_width=True)

                st.markdown("")
                st.markdown(
                    f"<h3 style='color: {daily_colors['primary']}; font-weight: 800; font-size: 1.15rem;'>Monthly Health</h3>",
                    unsafe_allow_html=True,
                )
                att_df_m, cg_df_m = load_attendance_and_cg_dataframes()
                if att_df_m is not None and cg_df_m is not None:
                    monthly_status_df = build_monthly_member_status_table(display_df, att_df_m, cg_df_m)
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

                        _cell_series = monthly_status_df["Cell"].fillna("").astype(str)
                        _cell_groups_mh = sorted(_cell_series.unique().tolist(), key=str.lower)
                        _all_names_mh = sorted(
                            monthly_status_df["Member"].dropna().astype(str).str.strip().unique().tolist(),
                            key=str.lower,
                        )
                        _all_names_mh = [n for n in _all_names_mh if n]

                        _mh_fcol1, _mh_fcol2 = st.columns([3, 1])
                        with _mh_fcol1:
                            _sel_cells_mh = st.multiselect(
                                "Filter by Cell Group...",
                                options=_cell_groups_mh,
                                default=[],
                                key=f"monthly_health_cell_multiselect_{_mh_fc}",
                                placeholder="Select cell groups...",
                                label_visibility="collapsed",
                            )
                        with _mh_fcol2:
                            if st.button(
                                "Clear All",
                                type="secondary",
                                use_container_width=True,
                                key="monthly_health_clear_filters",
                            ):
                                st.session_state.monthly_health_clear_filter_counter += 1
                                st.rerun()

                        st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)
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
                            st.markdown(
                                "<div style='height: 38px;'></div>",
                                unsafe_allow_html=True,
                            )

                        _filtered_monthly = monthly_status_df.copy()
                        if _sel_cells_mh:
                            _cs = monthly_status_df["Cell"].fillna("").astype(str)
                            _filtered_monthly = _filtered_monthly[_cs.isin(_sel_cells_mh)]
                        if _sel_names_mh:
                            _filtered_monthly = _filtered_monthly[
                                _filtered_monthly["Member"].isin(_sel_names_mh)
                            ]

                        _mh_filter_parts = []
                        if _sel_cells_mh:
                            _mh_filter_parts.append(f"{len(_sel_cells_mh)} cell group(s)")
                        if _sel_names_mh:
                            _mh_filter_parts.append(f"{len(_sel_names_mh)} name(s)")
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
                            st.markdown(
                                render_monthly_status_html_table(_filtered_monthly),
                                unsafe_allow_html=True,
                            )
                    else:
                        st.info(
                            "No monthly breakdown yet. Check that Attendance row 1 from column D has parseable dates "
                            "(e.g. DD/MM/YYYY or MM/DD/YYYY)."
                        )
                else:
                    st.info("Could not load the Attendance sheet for the monthly table.")

                st.markdown("")
                render_nwst_service_attendance_rate_charts(display_df, daily_colors)
            else:
                st.info("No cell health data available.")

            # LEADERSHIP SECTION
            st.markdown("")
            st.markdown(f"<h2 style='color: {daily_colors['primary']}; font-weight: 900;'>👔 LEADERSHIP</h2>", unsafe_allow_html=True)
            st.markdown(f"<div style='height: 3px; background: {daily_colors['primary']}; margin-bottom: 1.5rem;'></div>", unsafe_allow_html=True)

            if not display_df.empty:
                # Get leadership members from data
                leadership_data = get_leadership_by_role(display_df)

                if leadership_data:
                    # Calculate total leaders and percentage
                    total_leaders = sum(len(members) for members in leadership_data.values())

                    if cell_filter != "All" and cell_columns:
                        total_in_cell = len(display_df[display_df[cell_columns[0]] == cell_filter])
                    else:
                        total_in_cell = len(display_df)

                    leader_pct = (total_leaders / total_in_cell * 100) if total_in_cell > 0 else 0

                    # Display two cards in columns
                    leader_kpi_col1, leader_kpi_col2 = st.columns(2)

                    with leader_kpi_col1:
                        st.markdown(f"""
                        <div class="kpi-card">
                            <div class="kpi-label">Total Leaders</div>
                            <div class="kpi-number">{total_leaders}</div>
                        </div>
                        """, unsafe_allow_html=True)

                    with leader_kpi_col2:
                        st.markdown(f"""
                        <div class="kpi-card">
                            <div class="kpi-label">Leaders %</div>
                            <div class="kpi-number" style="color: {daily_colors['primary']};">{leader_pct:.0f}%</div>
                            <div class="kpi-subtitle">{total_leaders} of {total_in_cell}</div>
                        </div>
                        """, unsafe_allow_html=True)

                    st.markdown("")

                    # Display leadership organized by role
                    for role_name, members in leadership_data.items():
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
                st.info("No leadership data available.")

            # DETAILED MEMBERS SECTION
            st.markdown("")
            st.markdown(f"<h2 style='color: {daily_colors['primary']}; font-weight: 900;'>📋 DETAILED MEMBERS</h2>", unsafe_allow_html=True)
            st.markdown(f"<div style='height: 3px; background: {daily_colors['primary']}; margin-bottom: 1.5rem;'></div>", unsafe_allow_html=True)

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
            st.markdown("#### Global Filters")
            filter_col1, filter_col2, filter_col3 = st.columns(3)

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

            with filter_col3:
                status_filter_m = st.selectbox(
                    "Status",
                    options=["All", "Active", "Inactive"],
                    key="status_filter_m"
                )

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

            # Apply status filter if available
            if status_filter_m != "All":
                status_columns_m = [col for col in display_ministry_df.columns if 'status' in col.lower()]
                if status_columns_m:
                    display_ministry_df = display_ministry_df[display_ministry_df[status_columns_m[0]] == status_filter_m]

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
