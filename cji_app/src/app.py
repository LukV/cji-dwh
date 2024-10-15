import streamlit as st
import requests

st.title("API Data Viewer")

response = requests.get("http://localhost:8000/data")
if response.status_code == 200:
    data = response.json()
    st.write(data)
else:
    st.write("Error fetching data.")
