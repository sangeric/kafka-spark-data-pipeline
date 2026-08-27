from pyspark.sql.functions import col, when, trim, timestamp_micros, timestamp_millis

missing_values = [
    "",
    "NA",
    "N/A",
    "NULL",
    "null",
    "None",
    "none",
    "NaN",
    "nan",
]

columns = [
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "timestamp",
    "is_buyer_maker",
    "is_best_match",
]


def standardize_batch_agg_trades(df):
    return df.select(
            col("_c0").cast("long").alias("agg_trade_id"),
            col("_c1").cast("decimal(20,8)").alias("price"),
            col("_c2").cast("decimal(20,8)").alias("quantity"),
            col("_c3").cast("long").alias("first_trade_id"),
            col("_c4").cast("long").alias("last_trade_id"),
            col("_c5").cast("long").alias("timestamp"),
            col("_c6").cast("boolean").alias("is_buyer_maker"),
            col("_c7").cast("boolean").alias("is_best_match"),
        )


def standardize_api_agg_trades(df):
    return df.select(
        col("a").cast("long").alias("agg_trade_id"),
        col("p").cast("decimal(20,8)").alias("price"),
        col("q").cast("decimal(20,8)").alias("quantity"),
        col("f").cast("long").alias("first_trade_id"),
        col("l").cast("long").alias("last_trade_id"),
        col("T").cast("long").alias("timestamp"),
        col("m").cast("boolean").alias("is_buyer_maker"),
        col("M").cast("boolean").alias("is_best_match"),
    )

def drop_null_agg_trades(df):
    return df.na.drop()

def drop_outlier_agg_trades(df):
    return df.filter(
        (col("quantity") > 0)
        & (col("price") > 0)
        & (col("first_trade_id") <= col("last_trade_id"))
    )

def drop_duplicate(df):
    return df.dropDuplicates(["agg_trade_id"])


def formate_timestamps_batch(df):
    return df.withColumn(
        "timestamp_formated",
        timestamp_micros(col("timestamp"))
    )

def formate_timestamps_api(df):
    return df.withColumn(
        "timestamp_formated",
        timestamp_millis(col("timestamp"))
    )