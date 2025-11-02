from functools import lru_cache
from typing import Literal
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


BASE_DIR = Path(__file__).parent.parent


class PrefectConfig(BaseModel):
    """
    Configuration for Prefect
    """

    api_url: str
    # basic auth for varible Example: PREFECT_API_AUTH_STRING="admin:pass"
    basic_auth_username: str
    basic_auth_password: str


class DatabaseConfig(BaseModel):
    """
    Configuration for Database
    """

    username: str
    password: str
    host: str
    port: int
    database: str


class PublicApiConfig(BaseModel):
    """
    Configuration for Public API
    """

    api_url: str
    key: str
    var_name: str


class S3(BaseModel):
    """
    Configuration for S3.
    """

    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str
    secure: bool = False


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(BASE_DIR / ".env.template", BASE_DIR / ".env"),
        case_sensitive=False,
        env_nested_delimiter="__",
        env_prefix="APP_CONFIG__",
        extra="allow",
    )
    base_dir: Path = BASE_DIR
    mode: Literal["DEV", "PROD", "TEST"] = "DEV"
    db: DatabaseConfig
    prefect: PrefectConfig
    public_api: PublicApiConfig
    s3: S3


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore


settings = get_settings()
