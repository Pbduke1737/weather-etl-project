# Weather ETL Pipeline

Extracts current weather data for multiple cities from **Open-Meteo** (free, no API key required), transforms it, and loads it into **PostgreSQL** using upsert logic.

---

## Project structure

```
weather_etl/
├── etl.py              # Main pipeline (extract → transform → load)
├── requirements.txt
├── docker-compose.yml  # Quick local Postgres setup
└── README.md
```

---

## Quick start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start PostgreSQL (Docker, optional)

```bash
docker compose up -d
```

Or point the pipeline at any existing Postgres instance via `--dsn`.

### 3. Run the pipeline

```bash
# One-shot run with default cities (NY, London, Tokyo, Sydney, Denver)
python etl.py

# Custom cities (geocoded automatically)
python etl.py --cities "Paris, Berlin, Cairo"

# Custom DSN
python etl.py --dsn "postgresql://user:pass@host:5432/mydb"

# Scheduled — re-run every 30 minutes
python etl.py --schedule 1800
```

---

## Environment variables (alternative to --dsn)

You can set `DATABASE_URL` in your environment and pass it via shell substitution:

```bash
export DATABASE_URL="postgresql://user:pass@host:5432/mydb"
python etl.py --dsn "$DATABASE_URL"
```

---

## Database schema

The pipeline auto-creates the table on first run:

```sql
CREATE TABLE weather_observations (
    id                  SERIAL PRIMARY KEY,
    city_name           TEXT        NOT NULL,
    latitude            NUMERIC(9,4),
    longitude           NUMERIC(9,4),
    observed_at         TIMESTAMPTZ,          -- API observation timestamp (UTC)
    fetched_at          TIMESTAMPTZ,          -- When the pipeline ran
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
    raw_json            JSONB,                -- Full raw API response
    UNIQUE (city_name, observed_at)           -- Upsert key
);
```

### Useful queries

```sql
-- Latest reading per city
SELECT DISTINCT ON (city_name)
    city_name, observed_at, temperature_c, weather_description
FROM weather_observations
ORDER BY city_name, observed_at DESC;

-- Temperature history for London
SELECT observed_at, temperature_c, feels_like_c
FROM weather_observations
WHERE city_name = 'London'
ORDER BY observed_at DESC
LIMIT 48;

-- Coldest reading ever recorded
SELECT city_name, observed_at, temperature_c
FROM weather_observations
ORDER BY temperature_c ASC
LIMIT 1;
```

---

## Pipeline stages

| Stage | What happens |
|---|---|
| **Extract** | `GET https://api.open-meteo.com/v1/forecast` with lat/lon |
| **Transform** | Flatten JSON, decode WMO weather codes, add `fetched_at` timestamp |
| **Load** | Batch upsert via `psycopg2.extras.execute_values` — no duplicates |

---

## Extending the pipeline

- **Add more weather fields**: edit `OPEN_METEO_PARAMS["current"]` and the `transform()` function.
- **Switch to hourly data**: change `current` to `hourly` in the API params and adjust the transform.
- **Swap the weather API**: replace `extract()` with any API that returns JSON — the transform/load stages remain the same.
- **Run on a cron**: use `--schedule` flag or add a system cron job calling `python etl.py`.
