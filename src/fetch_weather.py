# src/fetch_weather.py
import requests
from datetime import datetime
from src.db_utils import insert_weather_data

API_KEY = "your_openweather_api_key"
LAT, LON = 34.05, -118.25  # Example: Los Angeles

def fetch_and_store_weather():
    url = (
        f"https://api.openweathermap.org/data/2.5/onecall?"
        f"lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"
    )
    response = requests.get(url)
    hourly_data = response.json().get("hourly", [])

    parsed_data = []
    for entry in hourly_data:
        timestamp = datetime.utcfromtimestamp(entry["dt"])
        parsed_data.append((
            timestamp, "Los Angeles",
            entry["temp"],
            entry["humidity"],
            entry["wind_speed"]
        ))

    insert_weather_data(parsed_data)

if __name__ == "__main__":
    fetch_and_store_weather()
