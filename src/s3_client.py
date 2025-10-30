import logging
from typing import TYPE_CHECKING, Union
from prefect_aws.s3 import S3Bucket
from seed import create_s3_block
from src.config import settings
from prefect.logging import get_run_logger

if TYPE_CHECKING:
    LoggingAdapter = logging.LoggerAdapter[logging.Logger]


class S3Controller:
    """
    A singleton controller to manage the storage client.
    """

    _instance = None
    _storage = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(S3Controller, cls).__new__(cls)
            cls._storage = None
        return cls._instance

    def init_storage(self):
        if self._storage is None:
            self._storage = self._load_bucket()

    @property
    def s3_client(self) -> S3Bucket:
        if self._storage is None:

            self._storage = self._load_bucket()
        return self._storage  # type: ignore

    def close(self):
        if self._storage is not None:

            self._storage = None

    def _load_bucket(self):

        try:
            bucket_s3 = S3Bucket.load(settings.s3.bucket)
        except ValueError:

            create_s3_block()
            bucket_s3 = S3Bucket.load(settings.s3.bucket)

        return bucket_s3


s3_controller = S3Controller()
