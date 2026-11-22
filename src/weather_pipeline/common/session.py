import os

from pyspark.sql import SparkSession


def spark_session(name: str) -> SparkSession:
    """Static options live in conf/spark-defaults.conf; only secrets are injected here."""
    builder = SparkSession.builder.appName(name)
    if key := os.environ.get("AWS_ACCESS_KEY_ID"):
        builder = builder.config("spark.hadoop.fs.s3a.access.key", key).config(
            "spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"]
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
