import os
import requests
from datetime import datetime
from src.db_utils import get_db_connection

CITIES = [
    "Los Angeles", "San Francisco", "San Diego",
    "Sacramento", "San Jose", "Fresno", "Oakland"
]

def fetch_weather():
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        print("OPENWEATHER_API_KEY not set.")
        return

    print(f"📥 Fetching weather data for {len(CITIES)} cities...")

    records = []
    for city in CITIES:
        print(f"City: {city}")
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city},CA,US&appid={api_key}&units=metric"
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Failed for {city}: HTTP {response.status_code}")
            continue

        weather = response.json()
        print(f"{city} → {weather['main']['temp']}°C, {weather['main']['humidity']}% humidity")

        record = {
            "timestamp": datetime.utcfromtimestamp(weather["dt"]),
            "location": city,
            "temperature": weather["main"]["temp"],
            "humidity": weather["main"]["humidity"],
            "wind_speed": weather["wind"]["speed"]
        }
        records.append(record)

    if records:
        insert_weather_data(records)
        print(f"Inserted {len(records)} weather records.")
    else:
        print("No data inserted.")

def insert_weather_data(records):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            query = """
                INSERT INTO weather_data (timestamp, location, temperature, humidity, wind_speed)
                VALUES (%(timestamp)s, %(location)s, %(temperature)s, %(humidity)s, %(wind_speed)s)
                ON CONFLICT (timestamp, location) DO NOTHING
            """
            cur.executemany(query, records)
        conn.commit()

if __name__ == "__main__":
    fetch_weather()
