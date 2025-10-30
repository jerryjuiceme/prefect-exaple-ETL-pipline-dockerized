from seed import create_s3_block, api_variable_create, create_db_connection


def main():
    create_s3_block()
    api_variable_create()
    create_db_connection()


if __name__ == "__main__":
    main()
