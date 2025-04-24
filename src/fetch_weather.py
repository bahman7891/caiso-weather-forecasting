import os
import requests
import psycopg2
from datetime import datetime
from src.db_utils import get_db_connection

def fetch_weather():
    api_key = os.getenv("OPENWEATHER_API_KEY")
    city = "San Francisco"  # Change if needed
    print(f"📥 Fetching weather for {city}...")

    url = (
        f"http://api.openweathermap.org/data/2.5/weather?"
        f"q={city}&appid={api_key}&units=metric"
    )

    response = requests.get(url)
    if response.status_code != 200:
        print(f"❌ Failed to fetch weather: {response.status_code}")
        return

    weather = response.json()
    print(f"🌦 API response: {weather}")

    record = {
        "timestamp": datetime.utcfromtimestamp(weather["dt"]),
        "location": city,
        "temperature": weather["main"]["temp"],
        "humidity": weather["main"]["humidity"],
        "wind_speed": weather["wind"]["speed"]
    }

    print("🧾 Parsed weather record:", record)

    insert_weather_data([record])
    print("✅ Weather data inserted.")

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
