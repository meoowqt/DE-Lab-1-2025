from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from .main import weather_etl_flow

deployment = Deployment.build_from_flow(
    flow=weather_etl_flow,
    name="weather-etl-daily",
    schedule=CronSchedule(
        cron="0 8 * * *",
        timezone="Europe/Moscow"
    ),
    tags=["weather", "production"],
    work_queue_name="default"
)
deployment.apply()