import os
from dotenv import load_dotenv


load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))


redshift_connection_params = {
    "url": os.getenv("REDSHIFT_JDBC_URL"),
    "user": os.getenv("REDSHIFT_USER"),
    "password": os.getenv("REDSHIFT_PASSWORD"),
    "driver": "com.amazon.redshift.jdbc42.Driver"
}
