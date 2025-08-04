import os
from dotenv import load_dotenv


load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))


redshift_connection_params = {
    "url": os.getenv("REDSHIFT_JDBC_URL"),
    "user": os.getenv("REDSHIFT_USER"),
    "password": os.getenv("REDSHIFT_PASSWORD"),
    "driver": "com.amazon.redshift.jdbc42.Driver",
    "temp_s3_directory": os.getenv("TEMP_S3_DIRECTORY"),
    "aws_iam_role": os.getenv("AWS_IAM_ROLE"),
    "redshift_table": os.getenv("REDSHIFT_TABLE")
}
