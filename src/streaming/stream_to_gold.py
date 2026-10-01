"""Consommateur : lit le topic Kafka en continu, nettoie (Silver) et agrège par minute (Gold)."""
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

from src.session.spark_session import init_spark_session
from src.batch.bronze_to_silver import (
    standardize_api_agg_trades,
    formate_timestamps_api,
    drop_null_agg_trades,
    drop_outlier_agg_trades,
)
from src.batch.silver_to_gold import gold1

KAFKA_SERVERS = "kafka:9092"
TOPIC = "binance.aggtrades"
OUTPUT_PATH = "data/gold/btcusdt_stream"
CHECKPOINT_PATH = "data/checkpoints/btcusdt_stream"

# Le format d'un message Binance : on ne garde que les 8 clés utiles
TRADE_SCHEMA = StructType([
    StructField("a", LongType()),
    StructField("p", StringType()),
    StructField("q", StringType()),
    StructField("f", LongType()),
    StructField("l", LongType()),
    StructField("T", LongType()),
    StructField("m", BooleanType()),
    StructField("M", BooleanType()),
])


def main():
    spark = init_spark_session("stream_transform")
    spark.conf.set("spark.sql.caseSensitive", "true")    # "m" et "M" sont 2 colonnes différentes
    spark.conf.set("spark.sql.shuffle.partitions", "4")  # petit volume : 4 au lieu de 200
    spark.sparkContext.setLogLevel("WARN")

    # 1. BRONZE : brancher Spark sur le topic Kafka (lecture continue)
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")           # 1er lancement : depuis le début du topic
        .load()
    )

    # 2. Décoder le message : la colonne "value" contient le JSON sous forme d'octets
    trades = (
        raw.select(from_json(col("value").cast("string"), TRADE_SCHEMA).alias("trade"))
        .select("trade.*")
    )

    # 3. SILVER : exactement les mêmes fonctions que pour l'API REST
    df = standardize_api_agg_trades(trades)
    df = formate_timestamps_api(df)                      # T est en millisecondes
    df = drop_null_agg_trades(df)
    df = drop_outlier_agg_trades(df)

    # 4. Watermark (attendre 2 min les retardataires) + suppression des doublons
    df = (
        df.withWatermark("timestamp_formated", "2 minutes")
        .dropDuplicatesWithinWatermark(["agg_trade_id"])
    )

    # 5. GOLD : la même agrégation par minute que le batch et l'API
    gold_df = gold1(df)

    # 6. Écriture en continu, toutes les 30 secondes
    query = (
        gold_df.writeStream
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)   # le marque-page de Spark
        .outputMode("append")                            # n'écrit une minute qu'une fois terminée
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()                             # tourne jusqu'à Ctrl+C


if __name__ == "__main__":
    main()