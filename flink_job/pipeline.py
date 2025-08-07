from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kinesis import FlinkKinesisConsumer
from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.jdbc import (
    JdbcSink,
    JdbcConnectionOptions
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


