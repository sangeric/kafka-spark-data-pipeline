from pyspark.sql import SparkSession


def init_spark_session(name : str) -> SparkSession:

    return (
        SparkSession.builder
        .appName(name)
        .getOrCreate()
    )