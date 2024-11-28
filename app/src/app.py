import re
import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import requests
from streamlit_folium import st_folium
from pyproj import Transformer
import folium
from folium.plugins import MarkerCluster

# Attempt to load environment variables from .env if it exists
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'), override=True)

# Constants
API_BASE_URL = os.getenv("API_BASE_URL")
API_KEY = os.getenv("API_KEY")
PAGE_LENGTH = 20000
MAX_DISPLAY_RECORDS = 1000

# Set the wide layout as default
st.set_page_config(layout="wide")

@st.cache_data
def fetch_data(limit=10, offset=0, filters=None, sort_by="location_name", sort_order="asc"):
    """Fetch data from the API"""
    params = {
        "limit": limit,
        "offset": offset,
        "sort_by": sort_by,
        "sort_order": sort_order
    }
    if filters:
        params["filters"] = filters
    
    headers = {
        "accept": "application/json",
        "api-key": API_KEY
    }
    
    response = requests.get(API_BASE_URL, params=params, headers=headers, timeout=None)

    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Failed to fetch data: {response.status_code}")
        return None


def apply_filters(df):
    """Filter the DataFrame based on sidebar filters"""
    location_name_filter = st.sidebar.text_input("Zoek op naam")
    source_system_filter = st.sidebar.multiselect("Filter op bron",
                                        options=df["Bronsysteem"].dropna().unique().tolist())
    location_type_filter = st.sidebar.multiselect("Filter op type",
                                        options=df["Locatietype"].dropna().unique().tolist())
    city_filter = st.sidebar.multiselect("Filter op gemeente",
                                        options=df["Gemeente"].dropna().unique().tolist())

    if location_name_filter:
        df = df[df["Locatienaam"].str.contains(location_name_filter, case=False, na=False)]
    if source_system_filter:
        df = df[df["Bronsysteem"].isin(source_system_filter)]
    if location_type_filter:
        df = df[df["Locatietype"].isin(location_type_filter)]
    if city_filter:
        df = df[df["Gemeente"].isin(city_filter)]

    return df

def display_table_view(df):
    """Create the table view."""
    df = df.fillna('').astype(str)
    st.dataframe(df, use_container_width=True, height=600)

def display_chart_view(df):
    """Create the chart view."""
    if not df.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.write("Aantal records per bron")
            source_system_counts = df["Bronsysteem"].value_counts().sort_values(ascending=True)
            st.bar_chart(source_system_counts)

        with col2:
            st.write("Aantal records per type")
            location_type_counts = df["Locatietype"].value_counts().sort_values(ascending=True)
            st.bar_chart(location_type_counts)
    else:
        st.write("Geen gegevens beschikbaar om de grafieken weer te geven.")

def display_map_view(df):
    """Create the map view."""
    # Limit the number of records to a maximum of 500 for map visualization
    df_map = df.head(MAX_DISPLAY_RECORDS)

    # Initialize Folium Map centered around Belgium
    map_center = [50.85, 4.35]
    m = folium.Map(location=map_center, zoom_start=8)

    transformer = Transformer.from_crs("EPSG:31370", "EPSG:4326")
    marker_cluster = MarkerCluster().add_to(m)

    # Iterate over filtered data to add points and polygons to the map
    for _, item in df_map.iterrows():
        location_name = item["Locatienaam"] if pd.notna(item["Locatienaam"]) else "Onbekend"
        location_type = item["Locatietype"] if pd.notna(item["Locatietype"]) else "Onbekend"
        street = item["Straat"] if pd.notna(item["Straat"]) else "--"
        house_number = item["Huisnummer"] if pd.notna(item["Huisnummer"]) else "--"
        postal_code = item["Postcode"] if pd.notna(item["Postcode"]) else "--"
        city = item["Gemeente"] if pd.notna(item["Gemeente"]) else "--"
        source_system = item["Bronsysteem"] if pd.notna(item["Bronsysteem"]) else "Onbekend"


        # Create popup content
        popup_content = f"""
        <b>{location_name}</b><br>
        Type: {location_type}<br>
        Adres: {street} {house_number}, {postal_code} {city}<br>
        Bron: {source_system}
        """

        # Handle GML polygons
        if pd.notna(item["gml"]):
            match = re.search(r"<gml:posList>([-\d. ]+)</gml:posList>", item["gml"])
            if match:
                coord_list = list(map(float, match.group(1).split()))
                coordinates = [(coord_list[i], coord_list[i + 1]) \
                               for i in range(0, len(coord_list), 2)]
                converted_coords = [transformer.transform(x, y) for x, y in coordinates]

                folium.Polygon(
                    locations=converted_coords,
                    color='blue',
                    weight=1,
                    fill=True,
                    fill_opacity=0.5,
                    popup=folium.Popup(popup_content, max_width=300)
                ).add_to(m)

        # Handle point coordinates
        elif pd.notna(item["point"]):
            match = re.search(r"<gml:pos>([-\d.]+) ([-\d.]+)</gml:pos>", item["point"])
            if match:
                x_coord, y_coord = float(match.group(1)), float(match.group(2))
                lat, lon = transformer.transform(x_coord, y_coord)

                folium.Marker(
                    location=[lat, lon],
                    popup=folium.Popup(popup_content, max_width=300),
                    icon=folium.Icon(icon="info-sign", icon_size=(20, 20))
                ).add_to(marker_cluster)

    # Display the map in Streamlit
    st_folium(m, width=1600, height=600)

    # Display maximum number of results on map.
    st.write(f"Maximum aantal resultaten op kaart: {MAX_DISPLAY_RECORDS}")

def main():
    """Main application logic."""
    # Fetch data
    data = fetch_data(limit=PAGE_LENGTH, offset=0, sort_by="location_name", sort_order="asc")

    if data:
        # Data filtering and cleaning for UI display
        df = pd.DataFrame(data["items"])
        df = df[["location_name", "location_type_label", "street", "house_number", "postal_code",
                 "city", "source_system", "adresregister_uri", "perceel_uri", "identifier", "point", "gml"]]

        df.rename(columns={
            "location_name": "Locatienaam",
            "location_type_label": "Locatietype",
            "street": "Straat",
            "house_number": "Huisnummer",
            "postal_code": "Postcode",
            "city": "Gemeente",
            "source_system": "Bronsysteem",
            "adresregister_uri": "Adresregister URL",
            "perceel_uri": "Perceel URL",
            "identifier": "Bronsysteem URL"
        }, inplace=True)

        # Apply sidebar filters
        df_filtered = apply_filters(df)

        # Sidebar view selection
        view = st.sidebar.radio("Kies weergave",
                                ["Tabelweergave", "Kaartweergave", "Grafiekenweergave"])

        # Streamlit UI Title and Result Summary
        st.title("Cultuur en Jeugdinfrastructuur Dashboard")
        total_records = data.get("total", 0)
        filtered_records = len(df_filtered)
        st.markdown(f"### Resultaten: {filtered_records} van {total_records}")

        # Display the selected view
        if view == "Tabelweergave":
            display_table_view(df_filtered)

        elif view == "Kaartweergave":
            display_map_view(df_filtered)

        elif view == "Grafiekenweergave":
            display_chart_view(df_filtered)

    else:
        st.write("Geen gegevens gevonden.")

if __name__ == "__main__":
    main()
