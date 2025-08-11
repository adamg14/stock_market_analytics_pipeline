import json
import boto3
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

# constant environment variables
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE")
REDSHIFT_ENDPOINT = os.getenv("REDSHIFT_ENDPOINT")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_PORT= os.getenv("REDSHIFT_PORT")
print(REDSHIFT_USER)


def redshift_database_connection():
    return psycopg2.connect(
        host=REDSHIFT_ENDPOINT,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        connect_timeout=10
    )


redshift_database_connection()