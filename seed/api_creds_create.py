from src.config import settings
from prefect.variables import Variable


def api_variable_create() -> None:
    creds = {
        "key": settings.public_api.key,
        "api_url": settings.public_api.api_url,
    }
    if Variable.get(settings.public_api.var_name) is not None:
        print("API credentials already exists")
        return
    Variable.set(
        settings.public_api.var_name,
        creds,
        overwrite=True,
        tags=["public_api", "weather_etl_s3"],
    )
    print("API credentials created")
