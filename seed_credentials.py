from seed import (
    create_s3_block,
    api_variable_create,
    create_db_connection,
    ensure_bucket_exists,
)


def main():
    create_s3_block()
    api_variable_create()
    create_db_connection()
    ensure_bucket_exists()
    print("Credentials created")


if __name__ == "__main__":
    main()
