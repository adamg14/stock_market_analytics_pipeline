import json
import boto3
from datetime import datetime
from dotenv import load_dotenv
import os


# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))


redshift_data = boto3.client("redshift-data")

CLUSTER_ID = os.getenv("CLUSTER_ID")
DATABASE = os.getenv("REDSHIFT_DATABASE")
AWS_IAM_ROLE = os.get("AWS_IAM_ROLE")


def type_cast_value(value, cast_type):
    """This function cast the types as the messages are not always stored in the target type"""
    try:
        if cast_type == "float":
            return float(value)
        # ...
    except (ValueError, TypeError) as e:
        print(f"An error occurred casting the value {e}")
def process_kinesis_message(message):
    payload = json.loads(message["kinesis"]["data"])

    return {
        "ticker": payload["ticker"],
        "open": type_cast_value(payload["open"], "float")
        # ...
    }


def redshift_load(event, context):
    success_count = 0
    errors = []

    for record in event['Records']:
        try:
            data = process_kinesis_message(record)

            # batch database load
            sql_insert = f"""
                INSERT INTO {DATABASE}
                # ....
            '{}
            """