# src/db_utils.py
import psycopg2
from psycopg2.extras import execute_values
import os

def get_db_connection():
    return psycopg2.connect(
        dbname="energy_forecast",
        user="postgres",
        password=os.getenv("PGPASSWORD", "yourpassword"),
        host="localhost",
        port=5432
    )

def insert_weather_data(data):
    query = """
    INSERT INTO weather_data (timestamp, city, temperature, humidity, wind_speed)
    VALUES %s
    ON CONFLICT (timestamp, city) DO NOTHING;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, data)

def insert_caiso_data(data):
    query = """
    INSERT INTO caiso_load (timestamp, region, load_mw)
    VALUES %s
    ON CONFLICT (timestamp, region) DO NOTHING;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, data)
