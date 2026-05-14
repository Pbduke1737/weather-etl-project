"""
Weather ETL Pipeline
====================
Extracts current weather data from Open-Meteo (free, no API key required),
transforms it into a structured format, and loads it into PostgreSQL.

Usage:
    python etl.py                        # Run once for default cities
    python etl.py --cities "Paris,Tokyo" # Custom cities
    python etl.py --schedule 3600        # Run every 3600 seconds (1 hour)
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import psycopg2
import requests
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_CITIES: list[dict[str, Any]] = [
    {"name": "New York",     "latitude": 40.7128,  "longitude": -74.0060},
    {"name": "London",       "latitude": 51.5074,  "longitude": -0.1278},
    {"name": "Tokyo",        "latitude": 35.6762,  "longitude": 139.6503},
    {"name": "Sydney",       "latitude": -33.8688, "longitude": 151.2093},
    {"name": "Denver",       "latitude": 39.7392,  "longitude": -104.9903},
]

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

OPEN_METEO_PARAMS = {
    "current": [
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "weather_code",
        "wind_speed_10m",
        "wind_direction_10m",
        "surface_pressure",
        "precipitation",
        "cloud_cover",
        "visibility",
    ],
    "temperature_unit": "celsius",
    "wind_speed_unit": "kmh",
    "timezone": "UTC",
    "forecast_days": 1,
}

# WMO Weather Interpretation Codes → human-readable description
WMO_CODES: dict[int, str] = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Icy fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    77: "Snow grains",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    85: "Slight snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm w/ hail", 99: "Thunderstorm w/ heavy hail",
}

# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
def extract(city: dict[str, Any]) -> dict[str, Any]:
    """Fetch current weather JSON from Open-Meteo for a single city."""
    params = {
        **OPEN_METEO_PARAMS,
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current": ",".join(OPEN_METEO_PARAMS["current"]),
    }
    log.info(f"Extracting weather for {city['name']} ...")
    response = requests.get(OPEN_METEO_URL, params=params, timeout=10)
    response.raise_for_status()
    raw = response.json()
    log.debug(f"Raw response for {city['name']}: {json.dumps(raw, indent=2)}")
    return raw

# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------
def transform(city: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
    """Parse and enrich the raw API payload into a flat record."""
    current = raw.get("current", {})
    wmo_code = current.get("weather_code")
    return {
        "city_name":          city["name"],
        "latitude":           city["latitude"],
        "longitude":          city["longitude"],
        "observed_at":        current.get("time"),          # ISO-8601 UTC string
        "fetched_at":         datetime.now(timezone.utc).isoformat(),
        "temperature_c":      current.get("temperature_2m"),
        "feels_like_c":       current.get("apparent_temperature"),
        "humidity_pct":       current.get("relative_humidity_2m"),
        "wind_speed_kmh":     current.get("wind_speed_10m"),
        "wind_direction_deg": current.get("wind_direction_10m"),
        "pressure_hpa":       current.get("surface_pressure"),
        "precipitation_mm":   current.get("precipitation"),
        "cloud_cover_pct":    current.get("cloud_cover"),
        "visibility_m":       current.get("visibility"),
        "weather_code":       wmo_code,
        "weather_description": WMO_CODES.get(wmo_code, "Unknown"),
        "raw_json":           json.dumps(raw),
    }

# ---------------------------------------------------------------------------
# Load (PostgreSQL)
# ---------------------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS weather_observations (
    id                  SERIAL PRIMARY KEY,
    city_name           TEXT        NOT NULL,
    latitude            NUMERIC(9,4),
    longitude           NUMERIC(9,4),
    observed_at         TIMESTAMPTZ,
    fetched_at          TIMESTAMPTZ,
    temperature_c       NUMERIC(6,2),
    feels_like_c        NUMERIC(6,2),
    humidity_pct        SMALLINT,
    wind_speed_kmh      NUMERIC(7,2),
    wind_direction_deg  SMALLINT,
    pressure_hpa        NUMERIC(8,2),
    precipitation_mm    NUMERIC(7,2),
    cloud_cover_pct     SMALLINT,
    visibility_m        NUMERIC(10,2),
    weather_code        SMALLINT,
    weather_description TEXT,
    raw_json            JSONB,
    -- Uniqueness: one record per city per observation timestamp
    UNIQUE (city_name, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_weather_city        ON weather_observations (city_name);
CREATE INDEX IF NOT EXISTS idx_weather_observed_at ON weather_observations (observed_at DESC);
"""

UPSERT_SQL = """
INSERT INTO weather_observations (
    city_name, latitude, longitude, observed_at, fetched_at,
    temperature_c, feels_like_c, humidity_pct,
    wind_speed_kmh, wind_direction_deg, pressure_hpa,
    precipitation_mm, cloud_cover_pct, visibility_m,
    weather_code, weather_description, raw_json
) VALUES %s
ON CONFLICT (city_name, observed_at) DO UPDATE SET
    fetched_at          = EXCLUDED.fetched_at,
    temperature_c       = EXCLUDED.temperature_c,
    feels_like_c        = EXCLUDED.feels_like_c,
    humidity_pct        = EXCLUDED.humidity_pct,
    wind_speed_kmh      = EXCLUDED.wind_speed_kmh,
    wind_direction_deg  = EXCLUDED.wind_direction_deg,
    pressure_hpa        = EXCLUDED.pressure_hpa,
    precipitation_mm    = EXCLUDED.precipitation_mm,
    cloud_cover_pct     = EXCLUDED.cloud_cover_pct,
    visibility_m        = EXCLUDED.visibility_m,
    weather_code        = EXCLUDED.weather_code,
    weather_description = EXCLUDED.weather_description,
    raw_json            = EXCLUDED.raw_json;
"""

def get_connection(dsn: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn

def ensure_schema(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    log.info("Schema ready.")

def load(conn: psycopg2.extensions.connection, records: list[dict[str, Any]]) -> int:
    """Upsert a batch of transformed records; returns the number of rows affected."""
    if not records:
        return 0

    rows = [
        (
            r["city_name"], r["latitude"], r["longitude"],
            r["observed_at"], r["fetched_at"],
            r["temperature_c"], r["feels_like_c"], r["humidity_pct"],
            r["wind_speed_kmh"], r["wind_direction_deg"], r["pressure_hpa"],
            r["precipitation_mm"], r["cloud_cover_pct"], r["visibility_m"],
            r["weather_code"], r["weather_description"], r["raw_json"],
        )
        for r in records
    ]

    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows)
        affected = cur.rowcount
    conn.commit()
    log.info(f"Upserted {affected} row(s) into weather_observations.")
    return affected

# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------
def run_pipeline(dsn: str, cities: list[dict[str, Any]]) -> None:
    log.info(f"Pipeline run started — {len(cities)} city/cities.")
    conn = get_connection(dsn)
    ensure_schema(conn)

    records: list[dict[str, Any]] = []
    for city in cities:
        try:
            raw = extract(city)
            record = transform(city, raw)
            records.append(record)
            log.info(
                f"  {city['name']}: {record['temperature_c']}°C, "
                f"{record['weather_description']}"
            )
        except requests.RequestException as exc:
            log.error(f"  HTTP error for {city['name']}: {exc}")
        except (KeyError, TypeError) as exc:
            log.error(f"  Transform error for {city['name']}: {exc}")

    load(conn, records)
    conn.close()
    log.info("Pipeline run complete.")

# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
def parse_city_arg(raw: str) -> list[dict[str, Any]]:
    """
    Accept a comma-separated list of 'City Name' strings and geocode them
    via Open-Meteo's geocoding API.
    """
    names = [n.strip() for n in raw.split(",") if n.strip()]
    cities = []
    for name in names:
        url = "https://geocoding-api.open-meteo.com/v1/search"
        resp = requests.get(url, params={"name": name, "count": 1}, timeout=10)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            log.warning(f"Could not geocode '{name}' — skipping.")
            continue
        r = results[0]
        cities.append({"name": r["name"], "latitude": r["latitude"], "longitude": r["longitude"]})
        log.info(f"Geocoded '{name}' → {r['name']} ({r['latitude']}, {r['longitude']})")
    return cities


def main() -> None:
    parser = argparse.ArgumentParser(description="Weather ETL Pipeline")
    parser.add_argument(
        "--dsn",
        default="postgresql://postgres:password@localhost:5432/weather_db",
        help="PostgreSQL DSN, e.g. postgresql://user:pass@host:port/db",
    )
    parser.add_argument(
        "--cities",
        default=None,
        help="Comma-separated city names (uses geocoding). Omit to use built-in defaults.",
    )
    parser.add_argument(
        "--schedule",
        type=int,
        default=0,
        metavar="SECONDS",
        help="If > 0, run repeatedly on this interval (seconds). Default: run once.",
    )
    args = parser.parse_args()

    cities = parse_city_arg(args.cities) if args.cities else DEFAULT_CITIES

    if args.schedule > 0:
        log.info(f"Scheduled mode: running every {args.schedule}s. Ctrl-C to stop.")
        while True:
            run_pipeline(args.dsn, cities)
            log.info(f"Sleeping {args.schedule}s …")
            time.sleep(args.schedule)
    else:
        run_pipeline(args.dsn, cities)


if __name__ == "__main__":
    main()
