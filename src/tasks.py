import json
from datetime import datetime, timedelta
from io import BytesIO
from prefect import task
import requests
from minio import Minio
from clickhouse_driver import Client
from .config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET,
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DB,
    CLICKHOUSE_USER, CLICKHOUSE_PASSWORD,
    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
)

@task
def fetch_weather(city: str, lat: float, lon: float):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,wind_speed_10m,wind_direction_10m",
        "timezone": "Europe/Moscow",
        "forecast_days": 2
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    data["_meta"] = {"city": city}
    return data

@task
def save_raw_to_minio(data: dict, city: str):
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
    date_str = datetime.now().strftime("%Y-%m-%d")
    obj = f"{city}/{date_str}.json"
    raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
    client.put_object(MINIO_BUCKET, obj, BytesIO(raw), len(raw))

@task
def transform_hourly(raw_data: dict):
    hourly = raw_data["hourly"]
    city = raw_data["_meta"]["city"]
    tomorrow = (datetime.now() + timedelta(days=1)).date()
    records = []
    for i, ts in enumerate(hourly["time"]):
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.date() == tomorrow:
            records.append({
                "city": city,
                "date": dt.date(),
                "hour": dt.hour,
                "temperature": float(hourly["temperature_2m"][i]),
                "precipitation": float(hourly["precipitation"][i]),
                "windspeed": float(hourly["wind_speed_10m"][i]),
                "winddirection": int(hourly["wind_direction_10m"][i]),
                "_loaded_at": datetime.now()
            })
    return records

@task
def transform_daily(hourly_data: list):
    if not hourly_data:
        return None
    temps = [r["temperature"] for r in hourly_data]
    return {
        "city": hourly_data[0]["city"],
        "date": hourly_data[0]["date"],
        "min_temperature": min(temps),
        "max_temperature": max(temps),
        "avg_temperature": sum(temps) / len(temps),
        "total_precipitation": sum(r["precipitation"] for r in hourly_data),
        "_loaded_at": datetime.now()
    }

@task
def load_to_clickhouse(hourly_data: list, daily_data: dict):
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )
    if hourly_data:
        client.execute("INSERT INTO weather_hourly VALUES", hourly_data)
    if daily_data:
        client.execute("INSERT INTO weather_daily VALUES", [daily_data])

@task
def notify_telegram(daily_data: dict, hourly_data: list):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    city = daily_data["city"]
    date = daily_data["date"]
    msg = f"🌤 Прогноз на {date}\n📍 {city}\n🌡 {daily_data['min_temperature']:.1f}–{daily_data['max_temperature']:.1f}°C\n💧 Осадки: {daily_data['total_precipitation']:.1f} мм"
    if daily_data["total_precipitation"] > 10:
        msg += "\n⚠️ Сильные осадки!"
    if any(r["windspeed"] > 15 for r in hourly_data):
        msg += "\n⚠️ Сильный ветер!"
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=10)
    except:
        pass