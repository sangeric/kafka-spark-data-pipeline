from pyspark.sql import SparkSession


def init_spark_session(name : str) -> SparkSession:

    return (
        SparkSession.builder
        .appName(name)
        .master("spark://spark-master:7077")
        .getOrCreate()
    )