from functools import lru_cache
from io import BytesIO
import json
import pandas as pd
from src.config import settings


def dict_to_raw_dataframe(
    data: dict,
) -> pd.DataFrame:
    row = {}

    # 1. Extract flattened data drom location
    location = data.get("location", {})
    row["location_name"] = location.get("name")
    row["location_region"] = location.get("region")
    row["location_country"] = location.get("country")
    row["location_lat"] = location.get("lat")
    row["location_lon"] = location.get("lon")
    row["location_tz_id"] = location.get("tz_id")
    row["location_localtime_epoch"] = location.get("localtime_epoch")
    row["location_localtime"] = location.get("localtime")

    # 2. Exteract forecast date (forecastday -> date)
    forecastday = None
    if (
        data.get("forecast")
        and isinstance(data["forecast"].get("forecastday"), list)
        and len(data["forecast"]["forecastday"]) > 0
    ):
        forecastday = data["forecast"]["forecastday"][0]

    row["forecastday_date"] = forecastday.get("date") if forecastday else None

    # 3. All contents of forecastday are saved as JSON in the params column
    if forecastday:
        params = {k: v for k, v in forecastday.items() if k != "date"}
        row["params"] = json.dumps(params, ensure_ascii=False, default=str)
    else:
        row["params"] = None

    return pd.DataFrame([row])


@lru_cache
def get_storage_options() -> dict:
    """
    Get storage options for pandas.

    :return: dict with storage options for pandas.
    """

    return {
        "key": settings.s3.access_key,
        "secret": settings.s3.secret_key,
        "client_kwargs": {"endpoint_url": settings.s3.endpoint_url},
    }


def decode_object(temp_object: BytesIO) -> dict:
    temp_object.seek(0)
    json_str = temp_object.read().decode("utf-8")
    return json.loads(json_str)
