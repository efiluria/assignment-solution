import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame


def clean(df: DataFrame) -> DataFrame:
    # remove records with null order_id
    df_cleaned = df.filter(df.order_id.isNotNull())

    # Standardize status values
    df_cleaned = df_cleaned.withColumn(
        "status",
        F.when(
            F.col("status").isin("completed", "pending", "failed"), F.col("status")
        ).otherwise("pending"),
    )

    # Convert dates to one format
    df_cleaned = df_cleaned.withColumn(
        "order_time",
        F.coalesce(
            F.try_to_timestamp("order_time", F.lit("yyyy-MM-dd HH:mm:ss")),
            F.try_to_timestamp("order_time", F.lit("yyyy-MM-dd HH:mm")),
            F.try_to_timestamp("order_time", F.lit("yyyy-MM-dd")),
            F.try_to_timestamp("order_time", F.lit("MM/dd/yyyy HH:mm:ss")),
            F.try_to_timestamp("order_time", F.lit("MM/dd/yyyy")),
        ),
    )
    # Fill missing quantity with 1
    df_cleaned = df_cleaned.withColumn(
        "quantity", F.when(F.col("quantity").isNull(), 1).otherwise(F.col("quantity"))
    )

    # standardize email format
    email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

    df_cleaned = df_cleaned.withColumn("email", F.lower(F.trim(F.col("email"))))

    df_cleaned = df_cleaned.withColumn(
        "email",
        F.when(F.col("email").rlike(email_regex), F.col("email")).otherwise(
            F.lit(None)
        ),
    )

    return df_cleaned


def dedup(df: DataFrame) -> DataFrame:
    # order by time
    w = Window.partitionBy("order_id").orderBy(
        F.col("order_time").desc_nulls_last()  # latest time
    )

    df_deduped = (
        df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    )

    return df_deduped
