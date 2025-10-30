from pathlib import Path
from flows.weather_etl_s3 import weather_etl_s3


if __name__ == "__main__":
    weather_etl_s3.from_source(
        source=str(Path(__file__).parent),  # code stored in local directory
        entrypoint="deploy.py:weather_etl_s3",
    ).deploy(  # type: ignore
        name="ETL Weather Data to S3",
        work_pool_name="basic-pipe",
        version="1.0.0",
        tags=["weather", "api_etl"],
        push=False,
        cron="0 8 * * *",
    )
