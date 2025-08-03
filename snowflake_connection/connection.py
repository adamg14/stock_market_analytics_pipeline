from dotenv import load_dotenv
import os
import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

private_key_path = os.path.join(os.path.dirname(__file__), "snowflake_key.pem")
                                
with open("snowflake_key.pem", "rb") as f:
    private_key = serialization.load_pem_private_key(
        f.read(),
        password=None,
        backend=default_backend()
    )

# convert from PEM to DER as required by snowflake
private_key_der = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

private_key_encoded = base64.b64encode(private_key_der).decode("utf-8")

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
pem_path = os.path.join(parent_dir, "snowflake_key.pem")

snowflake_connection_params = {
    "sfURL":        f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
    "sfUser":       os.getenv("SNOWFLAKE_USER"),
    "sfPassword":   os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase":   "STOCK_DB",
    "sfSchema":     "MARKET_DATA_SCHEMA",
    "sfWarehouse":  "STOCK_DATA_WAREHOUSE",
    "sfRole":       "STOCK_DATABASE_ADMIN",
    "authentication": "snowflake"
}

with open("snowflake_key.pem", "rb") as f:
    key = serialization.load_pem_private_key(f.read(), password=None)
    print("✅ Key loaded successfully")

print(pem_path)
print(os.path.exists(pem_path))