import json
from typing import TYPE_CHECKING, Union
import logging
import httpx
import pandas as pd
import pendulum
from prefect_aws.s3 import S3Bucket
from io import BytesIO
from prefect import flow, get_run_logger, task
from prefect import cache_policies as cshp
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy.engine import Engine

from src.s3_client import s3_controller
from src.config import settings
from src.utils import dict_to_raw_dataframe, get_storage_options, decode_object


if TYPE_CHECKING:
    LoggingAdapter = logging.LoggerAdapter[logging.Logger]


@task(
    name="get_weather_history",
    cache_policy=cshp.TASK_SOURCE + cshp.INPUTS,
    cache_expiration=timedelta(
        seconds=30,
    ),
)
def get_weather_history(
    date: str,
    city: str,
    logger: Union[logging.Logger, "LoggingAdapter"],
) -> dict:
    params = {"dt": date, "q": city, "key": settings.public_api.key}
    logger.info(f"Getting weather history for {city} on {date}")
    with httpx.Client(timeout=10) as client:
        try:
            response = client.get(settings.public_api.api_url, params=params)
            response.raise_for_status()
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response text: {response.text[:500]}")
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Error getting weather history for {city} on {date}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error getting weather history for {city} on {date}: {e}")
            raise


@task(name="pipeline_start")
def start(logger: Union[logging.Logger, "LoggingAdapter"]):
    s3_controller.init_storage()
    logger.info("Pipeline started")


@task(name="pipeline_end")
def end(logger: Union[logging.Logger, "LoggingAdapter"]):
    s3_controller.close()
    logger.info("Pipeline ended")


@task
def extract_weather_data_s3(
    date: str,
    city: str,
    logger: Union[logging.Logger, "LoggingAdapter"],
) -> str:
    # get weather history
    weather_data: dict = get_weather_history(date, city, logger)

    # load s3 bucket client
    bucket_s3: S3Bucket = s3_controller.s3_client  # type: ignore

    object_name = f"weather/raw/weather_{date}.json"

    # convert json to string
    json_data = json.dumps(weather_data, ensure_ascii=False, indent=2)
    json_bytes = json_data.encode("utf-8")

    # Native Boto3 SDK
    # s3_client = bucket_s3.credentials.get_s3_client()
    # s3_client.put_object(
    #     Bucket=settings.s3.bucket,
    #     Key=object_name,
    #     Body=BytesIO(json_bytes),
    #     ContentType="application/json",
    # )

    bucket_s3.upload_from_file_object(
        from_file_object=BytesIO(json_bytes),
        to_path=object_name,
    )

    logger.info("Weather data saved to %s", object_name)
    return object_name


@task
def transform_data_s3(
    execution_date: str,
    object_name: str,
    logger: Union[logging.Logger, "LoggingAdapter"],
):

    csv_object_name = f"weather/processed/weather_{execution_date}.csv"
    temp_object: BytesIO = BytesIO()

    bucket_s3 = s3_controller.s3_client

    # Native Boto3 SDK
    # s3_client = bucket_s3.credentials.get_s3_client()
    # temp_object = s3_client.get_object(Bucket=settings.s3.bucket, Key=object_name)

    bucket_s3.download_object_to_file_object(object_name, temp_object)
    weather_data: dict = decode_object(temp_object)

    df: pd.DataFrame = dict_to_raw_dataframe(weather_data)

    storage_options = get_storage_options()
    s3_path = f"s3://{settings.s3.bucket}/{csv_object_name}"
    df.to_csv(
        path_or_buf=s3_path,
        index=False,
        escapechar="\\",
        compression=None,
        storage_options=storage_options,
    )

    logger.info("Data transformed and saved to %s", s3_path)

    return csv_object_name


@task
def load_to_postgres_s3(
    execution_date: str,
    logger: Union[logging.Logger, "LoggingAdapter"],
) -> None:
    csv_object_name = f"weather/processed/weather_{execution_date}.csv"
    storage_options = get_storage_options()
    s3_path = f"s3://{settings.s3.bucket}/{csv_object_name}"

    df = pd.read_csv(
        filepath_or_buffer=s3_path,
        storage_options=storage_options,
        compression=None,  # No compression
    )

    block_name = "local-db-connector"
    with SqlAlchemyConnector.load(block_name) as connector:  # type: ignore
        engine: Engine = connector.get_engine()  # type: ignore
        df.to_sql(name="weather_raw", con=engine, if_exists="append", index=False)

        logger.info("Data loaded to postgres from %s", s3_path)


@task
def cleanup_s3(
    execution_date: str,
    logger: Union[logging.Logger, "LoggingAdapter"],
):
    bucket_s3 = s3_controller.s3_client
    objects_to_delete = [
        f"weather/raw/weather_{execution_date}.json",
        f"weather/processed/weather_{execution_date}.csv",
    ]
    for object_name in objects_to_delete:
        try:
            s3_client = bucket_s3.credentials.get_s3_client()
            s3_client.delete_object(Bucket=settings.s3.bucket, Key=object_name)
            logger.info("Object %s deleted", object_name)
        except Exception as e:
            logger.error("Error deleting object %s: %s", object_name, e)


################
### Pipeline ###
################


@flow
def weather_etl_s3(
    date: str = pendulum.today().format("YYYY-MM-DD"),
    city: str = "Minsk",
) -> None:
    """Prefect flow:
        Extracting weather data from openweathermap.org and loading it to postgres

    pipe:
        start → extract → transform → load → cleanup → end

    Args:
        date (str, optional): _description_. Defaults to pendulum.today().format("YYYY-MM-DD").
        city (str, optional): _description_. Defaults to "Minsk  ".
    """

    logger = get_run_logger()
    start(logger)

    object_name = extract_weather_data_s3(date, city, logger)  # Extracting the weather

    transform_data_s3(date, object_name, logger)  # Transforming the data

    load_to_postgres_s3(date, logger)  # Loading the data to postgres

    cleanup_s3(date, logger)  # Cleanup the data S3

    end(logger)
