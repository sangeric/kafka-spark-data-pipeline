from pyspark.sql.functions import (
    window,
    sum,
    avg,
    min,
    max,
    count,
    col,
)

def gold1(df):

    return (
    df.groupBy(
        window(col("timestamp_formated"), "1 minute")
    )
    .agg(
        count("*").alias("trade_count"),
        sum("quantity").alias("volume"),
        avg("price").alias("avg_price"),
        min("price").alias("low_price"),
        max("price").alias("high_price"),
    )
)