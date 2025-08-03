from dotenv import load_dotenv
import os

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

private_key = os.getenv("PRIVATE_KEY")
private_key_bytes = private_key.encode()

snowflake_connection_params = {
    "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
    "sfDatabase": "STOCK_DB",
    "sfSchema": "MARKET_DATA_SCHEMA",
    "sfWarehouse": "STOCK_DATA_WAREHOUSE",
    "sfRole": "STOCK_DATABASE_ADMIN",
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "authentication": "snowflake",
    "private_key": private_key_bytes
}