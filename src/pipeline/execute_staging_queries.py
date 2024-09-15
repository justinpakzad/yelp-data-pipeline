import boto3
from botocore.exceptions import ClientError
import json
import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path
from psycopg2.extensions import connection as Connection
from src.utils.connect_redshift import get_secret, connect_redshift


def execute_queries(conn: Connection, files: list) -> None:
    with conn.cursor() as cursor:
        for file_path in files:
            try:
                with open(file_path, "r") as f:
                    sql = f.read()
                    cursor.execute(sql)
            except Exception as e:
                print(f"Error processing queries: {e}")


def main():
    load_dotenv()
    secrets = get_secret()
    database = secrets.get("dbName")
    username = secrets.get("username")
    password = secrets.get("password")
    host = os.getenv("HOST")
    conn = connect_redshift(
        host=host, database=database, user=username, password=password
    )
    sql_folder_path = Path(__file__).parent.parent / "sql" / "stg"
    sql_stg_files = list(sql_folder_path.glob("*.sql"))
    execute_queries(conn, sql_stg_files)


if __name__ == "__main__":
    main()
