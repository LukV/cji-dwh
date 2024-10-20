import streamlit as st
import pandas as pd
import requests

API_BASE_URL = "http://localhost:8000/api/infras" 

# Set the wide layout as default
st.set_page_config(layout="wide")

# Function to fetch data from the API
def fetch_data(limit=10, offset=0, filters=None, sort_by="id", sort_order="asc"):
    params = {
        "limit": 15000,
        "offset": offset,
        "sort_by": "location_name",
        "sort_order": "asc"
    }
    if filters:
        params["filters"] = filters
    response = requests.get(API_BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Failed to fetch data: {response.status_code}")
        return None

# Streamlit UI
st.title("Infrastructure Dashboard")

# User inputs for pagination, sorting, and filtering
page_length = st.sidebar.number_input("Records per page", min_value=1, max_value=100, value=10)
current_page = st.sidebar.number_input("Page", min_value=1, value=1) - 1

sort_by = st.sidebar.selectbox("Sort by", ["id", "location_name", "location_type_label", "city", "source_system"], index=0)
sort_order = st.sidebar.radio("Sort order", ["asc", "desc"], index=0)

# Fetch data
offset = current_page * page_length
data = fetch_data(limit=page_length, offset=offset, sort_by=sort_by, sort_order=sort_order)

# Filter data
if data:
    df = pd.DataFrame(data["items"])
    
    # Display filter widgets for each column
    filters = {}
    for col in df.columns:
        if df[col].dtype == 'object':  # Only filter on text columns for now
            unique_vals = df[col].dropna().unique()
            selected_vals = st.sidebar.multiselect(f"Filter {col}", unique_vals)
            if selected_vals:
                df = df[df[col].isin(selected_vals)]
    
    st.write(df)

# Display pagination controls
total_records = data["total"] if data else 0
total_pages = (total_records // page_length) + (1 if total_records % page_length > 0 else 0)
st.sidebar.write(f"Total pages: {total_pages}")

# Next Step
st.write("Next Steps: Add a map view, bar charts for source systems, and location types.")
