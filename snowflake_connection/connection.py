import os
from dotenv import load_dotenv

# Load environment variables from project root
load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

# Read the unencrypted PKCS8 PEM private key as a string
pem_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../snowflake_key.pem'))
with open(pem_path, 'r') as key_file:
    pem_private_key = key_file.read()

# Snowflake connection parameters
snowflake_connection_params = {
    'sfURL':        f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
    'sfUser':       os.getenv('SNOWFLAKE_USER'),
    'sfRole':       'STOCK_DATABASE_ADMIN',
    'sfDatabase':   'STOCK_DB',
    'sfSchema':     'MARKET_DATA_SCHEMA',
    'sfWarehouse':  'STOCK_DATA_WAREHOUSE',
    'authenticator': 'snowflake_jwt',
    'pem_private_key': pem_private_key
}

# Optional sanity print
print('Loaded Snowflake connection params; private key length:', len(pem_private_key))
