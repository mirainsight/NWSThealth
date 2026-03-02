import streamlit as st
from datetime import datetime, timedelta, timezone
import colorsys
import random
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

@st.cache_resource
def get_google_sheet_client():
    """Initialize Google Sheets client using Streamlit secrets."""
    creds_dict = st.secrets["google"]
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    return gspread.authorize(creds)

@st.cache_data(ttl=300)
def load_sheet_data():
    """Load data from Google Sheet 'CG Combined' tab."""
    client = get_google_sheet_client()
    spreadsheet = client.open_by_key("1uexbQinWl1r6NgmSrmOXPtWs-q4OJV3o1OwLywMWzzY")
    worksheet = spreadsheet.worksheet("CG Combined")
    data = worksheet.get_all_values()

    if not data:
        return pd.DataFrame()

    # First row is headers
    df = pd.DataFrame(data[1:], columns=data[0])
    return df

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
    initial_sidebar_state="expanded"
)

# Generate daily colors for youthy vibe
daily_colors = generate_daily_colors()

# Add CSS to reduce Streamlit default spacing and style with youthy theme
st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700&display=swap');

    /* Base theme colors */
    .stApp {{
        background-color: {daily_colors['background']} !important;
        font-family: 'Outfit', sans-serif !important;
    }}

    /* Reduce default spacing */
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

    /* Text colors for dark theme */
    .stMarkdown, .stMarkdown p, .stMarkdown span, .stMarkdown div {{
        color: #ffffff !important;
    }}
    h1, h2, h3, h4, h5, h6 {{
        color: #ffffff !important;
        font-family: 'Outfit', sans-serif !important;
        font-weight: 700 !important;
    }}

    /* Sidebar styling */
    [data-testid="stSidebar"] {{
        background-color: #1a1a1a !important;
    }}
    [data-testid="stSidebar"] .stMarkdown, [data-testid="stSidebar"] p, [data-testid="stSidebar"] span {{
        color: #ffffff !important;
    }}

    /* Button styling with daily color */
    .stButton > button {{
        background-color: transparent !important;
        color: {daily_colors['primary']} !important;
        border: 2px solid {daily_colors['primary']} !important;
        border-radius: 0px !important;
        font-family: 'Outfit', sans-serif !important;
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
        font-family: 'Outfit', sans-serif !important;
        font-weight: 700 !important;
        letter-spacing: 1px !important;
    }}
    .stFormSubmitButton > button:hover {{
        background-color: {daily_colors['light']} !important;
        border-color: {daily_colors['light']} !important;
        transform: scale(1.02) !important;
    }}

    /* Input fields */
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input,
    .stSelectbox > div > div > select {{
        background-color: #1a1a1a !important;
        color: #ffffff !important;
        border-color: {daily_colors['primary']} !important;
    }}

    /* Multiselect styling */
    .stMultiSelect [data-baseweb="tag"] {{
        background-color: {daily_colors['primary']} !important;
        color: {daily_colors['background']} !important;
    }}
    .stMultiSelect [data-baseweb="select"] > div {{
        border-color: {daily_colors['primary']} !important;
    }}

    /* Labels */
    .stMultiSelect label, .stSelectbox label, .stTextInput label, .stNumberInput label {{
        color: #ffffff !important;
        font-family: 'Outfit', sans-serif !important;
    }}

</style>
""", unsafe_allow_html=True)

# Main app content
st.title("🏥 NWST Health")

# Generate daily colors for youthy vibe
daily_colors = generate_daily_colors()

# Markdown for header
st.markdown(f"### Welcome to NWST Health Dashboard")
st.markdown(f"Created with youthy vibes • Color of the day: **{daily_colors['primary']}**")

st.markdown("---")

# Load and display sheet data
try:
    df = load_sheet_data()
    if not df.empty:
        st.markdown("### 📊 CG Combined Data")
        st.dataframe(df, use_container_width=True)
    else:
        st.warning("No data found in the sheet.")
except Exception as e:
    st.error(f"Error loading data from Google Sheet: {e}")
    st.info("Make sure your Streamlit secrets are configured with Google service account credentials.")
