import streamlit as st
from datetime import datetime, timedelta, timezone
import colorsys
import random
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import json
from upstash_redis import Redis

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
        spreadsheet = client.open_by_key("1uexbQinWl1r6NgmSrmOXPtWs-q4OJV3o1OwLywMWzzY")
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
        spreadsheet = client.open_by_key("1uexbQinWl1r6NgmSrmOXPtWs-q4OJV3o1OwLywMWzzY")
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

    client = get_google_sheet_client()
    if not client:
        return {}

    try:
        spreadsheet = client.open_by_key("1uexbQinWl1r6NgmSrmOXPtWs-q4OJV3o1OwLywMWzzY")

        # Load both sheets
        att_worksheet = spreadsheet.worksheet("Attendance")
        att_data = att_worksheet.get_all_values()

        cg_worksheet = spreadsheet.worksheet("CG Combined")
        cg_data = cg_worksheet.get_all_values()

        if not att_data or len(att_data) < 2:
            return {}

        if not cg_data or len(cg_data) < 2:
            return {}

        # Parse Attendance sheet - only use column A (index 0)
        att_headers = att_data[0]
        att_df = pd.DataFrame(att_data[1:], columns=att_headers)

        # Calculate attendance stats using only column A for names and columns from D onwards
        attendance_stats = {}

        # Find name column in attendance (usually column A)
        att_name_col = att_df.columns[0] if len(att_df.columns) > 0 else None

        # Parse CG Combined to get Name and Cell mapping
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

        # Cache in Redis
        redis = get_redis_client()
        if redis:
            try:
                redis.set("nwst_attendance_stats", json.dumps(attendance_stats), ex=300)
            except Exception:
                pass

        return attendance_stats
    except Exception:
        return {}

def categorize_member_status(attendance_count, total_possible):
    """Categorize member as Regular, Irregular, or Follow Up based on attendance."""
    if attendance_count > (total_possible * 0.50):  # Above 50% attendance = Regular
        return "Regular"
    elif attendance_count > 0:  # Between 0% and 50% = Irregular
        return "Irregular"
    else:  # 0% attendance = Follow Up
        return "Follow Up"

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
    # Use date as seed for consistent colors throughout the day
    random.seed(hash(date_str) % (10 ** 8))

    # Generate vibrant colors using the seed
    hue = random.random()
    saturation = 0.85
    lightness = 0.50

    # Generate a primary accent color (bright, vibrant)
    rgb = colorsys.hls_to_rgb(hue, lightness, saturation)
    primary_color = '#{:02x}{:02x}{:02x}'.format(
        int(rgb[0] * 255),
        int(rgb[1] * 255),
        int(rgb[2] * 255)
    )

    # Generate a lighter variant for hover states
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
    """Generate random colors based on today's date (MYT)."""
    today_myt = get_today_myt_date()
    return generate_colors_for_date(today_myt)

# Page configuration
st.set_page_config(
    page_title="NWST Health",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Generate daily colors for youthy vibe
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

</style>
""", unsafe_allow_html=True)

# Main app content
st.title("🏥 NWST Health")

# Generate daily colors for youthy vibe
daily_colors = generate_daily_colors()

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
                        st.error("No data found in Google Sheet.")
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
                    def extract_status_type(status_val):
                        """Extract the status type from the descriptive status value"""
                        if isinstance(status_val, str):
                            if status_val.startswith("Regular:"):
                                return "Regular"
                            elif status_val.startswith("Irregular:"):
                                return "Irregular"
                            elif status_val.startswith("New"):
                                return "New"
                            elif status_val.startswith("Follow Up:"):
                                return "Follow Up"
                            elif status_val.startswith("Red:"):
                                return "Red"
                            elif status_val.startswith("Graduated:"):
                                return "Graduated"
                        return None

                    # Create a mapped status column
                    display_df['status_type'] = display_df[status_col].apply(extract_status_type)

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
                        st.markdown(f"<p style='color: #2ecc71; font-weight: 600;'>Regular Members (Above 50% attendance)</p>", unsafe_allow_html=True)
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
                        st.markdown(f"<p style='color: #e67e22; font-weight: 600;'>Irregular Members (50% and below attendance)</p>", unsafe_allow_html=True)
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

                # Get available columns
                available_cols = display_df.columns.tolist()

                # Find default columns to display
                default_cols = []
                for col in available_cols:
                    col_lower = col.lower()
                    # Name column - exclude columns with 'last' in them
                    if (any(x in col_lower for x in ['name', 'member']) and 'last' not in col_lower):
                        default_cols.append(col)
                    # Cell/Group column
                    elif col.lower().strip() in ['cell', 'group']:
                        default_cols.append(col)
                    # Status column
                    elif any(x in col_lower for x in ['status']):
                        default_cols.append(col)
                    # Notes column
                    elif any(x in col_lower for x in ['notes', 'note']):
                        default_cols.append(col)

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
