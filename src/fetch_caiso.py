import os
import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta
from src.db_utils import insert_caiso_data

def get_oasis_forecast_url():
    # Use yesterday to avoid unavailable data for today
    date = (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y%m%d")
    start = f"{date}T00:00-0000"
    end = f"{date}T23:59-0000"
    return (
        "http://oasis.caiso.com/oasisapi/SingleZip?"
        f"queryname=SLD_FCST&resultformat=6&startdatetime={start}&enddatetime={end}&version=1"
    )

def fetch_and_store_caiso():
    url = get_oasis_forecast_url()
    print(f"Downloading: {url}")
    
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to download: HTTP {response.status_code}")
    
    # Extract zip
    z = zipfile.ZipFile(io.BytesIO(response.content))
    files = z.namelist()
    if not files:
        print("No files in ZIP archive.")
        return

    with z.open(files[0]) as f:
        df = pd.read_csv(f)

    print("🧾 Columns in CSV:", df.columns.tolist())
    df.columns = [col.strip() for col in df.columns]

    required = ['INTERVALSTARTTIME_GMT', 'TAC_AREA_NAME', 'MW']
    if not all(col in df.columns for col in required):
        print("Required columns missing.")
        return

    df = df[df['INTERVALSTARTTIME_GMT'].notnull() & df['MW'].notnull()]

    records = []
    for _, row in df.iterrows():
        try:
            timestamp = datetime.strptime(row['INTERVALSTARTTIME_GMT'], '%Y-%m-%dT%H:%M:%S-00:00')
            region = row['TAC_AREA_NAME']
            load_mw = float(row['MW'])
            records.append((timestamp, region, load_mw))
        except Exception as e:
            print(f"Skipping row: {e}")
            continue

    if not records:
        print("No valid rows to insert.")
        return

    insert_caiso_data(records)
    print(f"Inserted {len(records)} records into TimescaleDB.")

if __name__ == "__main__":
    fetch_and_store_caiso()
