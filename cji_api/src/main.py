from fastapi import FastAPI
import json

app = FastAPI()

@app.get("/data")
def read_data():
    # Open and read the JSON file containing the array
    with open("../../data/topstory_ids.json", "r") as file:
        data = json.load(file)  # Read the JSON array directly into a Python list
    
    # Return the list as JSON response
    return data

