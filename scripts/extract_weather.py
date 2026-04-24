"""
Weather Data Extraction Script
================================
Extracts hourly weather data from Open-Meteo API (free, no API key required)
for Colombo, Sri Lanka and stores it in PostgreSQL.

API Source: https://open-meteo.com/
"""

import requests
import psycopg2
import logging
from datetime import datetime, timezone

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
API_URL = "https://api.open-meteo.com/v1/forecast"
LOCATION = {
    "name": "Colombo",
    "latitude": 6.9271,
    "longitude": 79.8612,
}

DB_CONFIG = {
    "host": "postgres",       # Docker service name
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}

# ── Database Setup ────────────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather_data (
    id                  SERIAL PRIMARY KEY,
    location            VARCHAR(100)    NOT NULL,
    recorded_at         TIMESTAMP       NOT NULL,      -- API forecast time
    temperature_c       NUMERIC(5, 2),                 -- °C
    humidity_pct        INTEGER,                        -- %
    wind_speed_kmh      NUMERIC(6, 2),                 -- km/h
    precipitation_mm    NUMERIC(6, 2),                 -- mm
    weather_code        INTEGER,                        -- WMO weather code
    extracted_at        TIMESTAMP       NOT NULL        -- pipeline run time
);
"""


def get_db_connection():
    """Open and return a PostgreSQL connection."""
    logger.info("Connecting to PostgreSQL …")
    conn = psycopg2.connect(**DB_CONFIG)
    logger.info("Connected successfully.")
    return conn


def create_table(conn):
    """Ensure the target table exists."""
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    logger.info("Table 'weather_data' is ready.")


def fetch_weather() -> dict:
    """
    Call the Open-Meteo API and return a cleaned list of hourly records
    for the current day.
    """
    params = {
        "latitude":  LOCATION["latitude"],
        "longitude": LOCATION["longitude"],
        "hourly":    "temperature_2m,relativehumidity_2m,windspeed_10m,precipitation,weathercode",
        "forecast_days": 1,
        "timezone": "Asia/Colombo",
    }

    logger.info("Fetching weather data from Open-Meteo …")
    response = requests.get(API_URL, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()
    logger.info("API response received.")

    hourly = data["hourly"]
    times         = hourly["time"]
    temperatures  = hourly["temperature_2m"]
    humidities    = hourly["relativehumidity_2m"]
    wind_speeds   = hourly["windspeed_10m"]
    precipitations = hourly["precipitation"]
    weather_codes = hourly["weathercode"]

    records = []
    for i in range(len(times)):
        records.append({
            "recorded_at":      times[i],
            "temperature_c":    temperatures[i],
            "humidity_pct":     humidities[i],
            "wind_speed_kmh":   wind_speeds[i],
            "precipitation_mm": precipitations[i],
            "weather_code":     weather_codes[i],
        })

    logger.info("Parsed %d hourly records.", len(records))
    return records


def save_to_db(conn, records: list):
    """Insert records into weather_data, skipping duplicates."""
    extracted_at = datetime.now(timezone.utc)
    location     = LOCATION["name"]

    INSERT_SQL = """
        INSERT INTO weather_data
            (location, recorded_at, temperature_c, humidity_pct,
             wind_speed_kmh, precipitation_mm, weather_code, extracted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """

    with conn.cursor() as cur:
        for rec in records:
            cur.execute(INSERT_SQL, (
                location,
                rec["recorded_at"],
                rec["temperature_c"],
                rec["humidity_pct"],
                rec["wind_speed_kmh"],
                rec["precipitation_mm"],
                rec["weather_code"],
                extracted_at,
            ))
    conn.commit()
    logger.info("Saved %d records to the database.", len(records))


# ── Entry point ───────────────────────────────────────────────────────────────
def run_pipeline():
    """Orchestrate extract → load."""
    conn = None
    try:
        conn = get_db_connection()
        create_table(conn)
        records = fetch_weather()
        save_to_db(conn, records)
        logger.info("Pipeline completed successfully.")
    except requests.RequestException as exc:
        logger.error("API request failed: %s", exc)
        raise
    except psycopg2.Error as exc:
        logger.error("Database error: %s", exc)
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    run_pipeline()
