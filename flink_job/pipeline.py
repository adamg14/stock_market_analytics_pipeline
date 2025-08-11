from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kinesis import FlinkKinesisConsumer
from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.jdbc import (
    JdbcSink,
    JdbcExecutionOptions,
    JdbcConnectionOptions
)
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

REDSHIFT_JDBC_URL = os.getenv("REDSHIFT_JDBC_URL")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")


def create_kinesis_source():
    """Configure the kinesis source"""
    return FlinkKinesisConsumer(
        "raw-data-streaming",
        JsonRowDeserializationSchema.builfer().type_info(
            Types.ROW_NAMED(
                [
                    "ticker",
                    "interval",
                    "currency",
                    "exchange_timezone",
                    "exchange",
                    "datetime",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume"
                 ],
                 [
                     Types.STRING(),
                     Types.STRING(),
                     Types.STRING(),
                     Types.STRING(),
                     Types.STRING(),
                     Types.STRING(),
                     Types.STRING(),
                     Types.STRING(),
                     Types.STRING(),
                     Types.STRING(),
                     Types.STRING(),
                 ]
            )
        ).build(),
        {'aws.region': 'us-east-1', 'flink.stream.initpos': 'LATEST'}
    )


def load_redshift():
    """Redshift database load with JDBC sink"""
    return JdbcSink.sink(
        """INSERT INTO stock_prices
        (ticker, interval,  currency, exchange_timezone, exchange, datetime, open, high, low, close, volume, ingestion_time)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        type_info=Types.ROW(
            [
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.SQL_TIMESTAMP(),
                Types.FLOAT(),
                Types.FLOAT(),
                Types.FLOAT(),
                Types.FLOAT(),
                Types.FLOAT(),
                Types.SQL_TIMESTAMP()
            ]
        ),
        jdbc_connection_options= {
            "url": REDSHIFT_JDBC_URL,
            "driver_name": "org.postgresql.Driver",
            "username": REDSHIFT_USER,
            "password": REDSHIFT_PASSWORD
        },
        jdbc_execution_options=JdbcExecutionOptions.builder()
            .with_batch_interval_ms(1000)
            .with_batch_size(100)
            .build()
    )


if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        "file:///opt/flink/lib/flink-connector-kinesis-2.0.1.jar",
        "file:///opt/flink/lib/flink-connector-jdbc-3.0.0-1.17.jar",
        "file:///opt/flink/lib/postgresql-42.6.0.jar"
    )

    (env.add_source(create_kinesis_source())
        .map(transform_record)
        .filter(lambda x: x is not None)
        .map(lambda x: (logger.info(f"Processed: {x}"), x)[1])  # Log and pass through
        .add_sink(create_redshift_sink())
    )

    env.execute("EnhancedStockLoader")
    
# "ticker",
#                     "interval",
#                     "currency",
#                     "exchange_timezone",
#                     "exchange",
#                     "datetime",
#                     "open",
#                     "high",
#                     "low",
#                     "close",
#                     "volume"