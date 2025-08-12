import os
import json
import base64
import time
import logging
from datetime import datetime
import boto3

# loging config
log = logging.getLogger()
log.setLevel(logging.INFO)

try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))
except Exception:
    pass

CLUSTER_ID = os.getenv("REDSHIFT_CLUSTER_ID")
DATABASE = os.getenv("REDSHIFT_DATABASE")
USER = os.getenv("REDSHIFT_USER")
SCHEMA = os.getenv("REDSHIFT_SCHEMA", "public")
TABLE = os.getenv("REDSHIFT_TABLE")

client = boto3.client("redshift-data", region_name="eu-north-1")

def execute_sql(sql, params):
    kwargs = {
        "Database": DATABASE,
        "Sql": sql,
        "ClusterIdentifier": CLUSTER_ID,
        "DbUser": USER,
        "Parameters": params
    }

    response = client.execute_statement(**kwargs)
    sid = response["Id"]
    while True:
        d = client.describe_statement(Id=sid)
        s = d["Status"]
        if s in ("FINISHED", "FAILED", "ABORTED"):
            if s != "FINISHED":
                raise RuntimeError(d.get("Error", "Statement failed"))
            return d

def lambda_handler(event, context):
    records = event.get("Records", [])
    if not records:
        return {"failures": []}
    else:
        sql_values = []
        for record in records:
            payload = json.loads(
                base64.b64decode(record["kinesis"]["data"])
            )
            try:
                sql_statement = """
                    INSERT INTO {schema}.{table}
                    (ticker,"interval",currency,exchange_timezone,exchange,date_timestamp,open_price,high,low,close,volume,processing_time)
                    VALUES (:ticker,:interval,:currency,:exchange_timezone,:exchange,TO_TIMESTAMP(:date_timestamp,'YYYY-MM-DD HH24:MI:SS'),:open_price,:high,:low,:close,:volume,GETDATE())""".format(schema=SCHEMA, table=TABLE)
                
                params = [
                    {
                        "name": "ticker",          "value": str(payload["ticker"])
                    },
                    {
                        "name": "interval",        "value": str(payload["interval"])
                    },
                    {
                        "name": "currency",        "value": str(payload["currency"])
                    },
                    {
                        "name": "exchange_timezone","value": str(payload["exchange_timezone"])
                    },
                    {
                        "name": "exchange",        "value": str(payload.get("exchange", ""))
                    },
                    {
                        "name": "date_timestamp",  "value": str(payload["date_timestamp"])
                    },
                    {
                        "name": "open_price",      "value": str(payload["open_price"])
                    },
                    {
                        "name": "high",            "value": str(payload["high"])
                    },
                    {
                        "name": "low",             "value": str(payload["low"])
                    },
                    {
                        "name": "close",           "value": str(payload["close"])
                    },
                    {
                        "name": "volume",          "value": str(payload["volume"])
                    }
                ]
                
                execute_sql(sql_statement, params=params)

            except Exception as e:
                log.exception(f"Failed record load")
