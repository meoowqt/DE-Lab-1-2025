from prefect import flow
from .config import CITIES
from .tasks import fetch_weather, save_raw_to_minio, transform_hourly, transform_daily, load_to_clickhouse, notify_telegram

@flow(name="Hourly Weather ETL for Moscow and Samara")
def weather_etl_flow():
    for city, coords in CITIES.items():
        raw = fetch_weather(city, coords["lat"], coords["lon"])
        save_raw_to_minio(raw, city)
        hourly = transform_hourly(raw)
        daily = transform_daily(hourly)
        load_to_clickhouse(hourly, daily)
        notify_telegram(daily, hourly)