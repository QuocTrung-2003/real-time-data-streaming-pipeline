import time
import requests
from loguru import logger
from config import LAT, LON

call_count = 0

def fetch_data():
    global call_count

    url = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&current_weather=true"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        record = data.get("current_weather", {})

        record["timestamp"] = time.time()
        record["location"] = {"lat": LAT, "lon": LON}

        call_count += 1
        logger.info(f"API call #{call_count}")

        return record

    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return None