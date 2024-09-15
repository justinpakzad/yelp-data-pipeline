import boto3
from botocore.exceptions import ClientError
import json
import psycopg2
import os


def get_secret():
    secret_name = os.getenv("SECRET_NAME")
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = get_secret_value_response["SecretString"]
    return json.loads(secret)


def connect_redshift(host, database, user, password, port=5439):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
        )
        return conn
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Error connecting to {database}: {e}")
        return False
