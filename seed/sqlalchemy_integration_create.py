from prefect_sqlalchemy import SqlAlchemyConnector, ConnectionComponents, SyncDriver
from src.config import settings
from pydantic import SecretStr
from prefect.exceptions import ObjectNotFound


def create_db_connection():
    connector = SqlAlchemyConnector(
        connection_info=ConnectionComponents(
            driver=SyncDriver.POSTGRESQL_PSYCOPG2,
            username=settings.db.username,
            password=SecretStr(settings.db.password),
            host=settings.db.host,
            port=settings.db.port,
            database=settings.db.database,
        )
    )
    try:
        db = SqlAlchemyConnector.load("local-db-connector")
        if db is not None:
            print("Database connection already exists")
            return
    except ValueError as e:
        connector.save("local-db-connector")
        print("Database connection created")
        return


