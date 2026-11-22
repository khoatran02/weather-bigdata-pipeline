import datetime as dt
import json

import pytest
from pyspark.sql import SparkSession

from weather_pipeline.streaming import transforms


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def bronze(spark):
    def build(*values):
        now = dt.datetime(2026, 11, 22, 13, 0)
        rows = [
            (
                v.decode() if isinstance(v, bytes) else v if isinstance(v, str) else json.dumps(v),
                "key",
                "weather.raw.live",
                0,
                offset,
                now,
                now.date(),
            )
            for offset, v in enumerate(values)
        ]
        return spark.createDataFrame(rows, transforms.BRONZE_SCHEMA)

    return build


@pytest.fixture
def silver(spark, bronze):
    def build(*values):
        return transforms.accepted(transforms.standardize(bronze(*values)))

    return build
