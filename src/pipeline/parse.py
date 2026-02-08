from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame


def parse_json_column(df: DataFrame, json_column: str) -> DataFrame:
    raw_payload_schema = StructType(
        [
            # StructField("record_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField(
                "customer",
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("email", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("product", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("city", StringType(), True),
            StructField("status", StringType(), True),
            StructField("order_time", StringType(), True),
        ]
    )

    df_parsed = df.withColumn(
        json_column, F.from_json(F.col(json_column), raw_payload_schema)
    )

    df_flat = df_parsed.select(
        # F.col("record_id"),
        F.col(f"{json_column}.order_id").alias("order_id"),
        F.col(f"{json_column}.customer.name").alias("customer_name"),
        F.col(f"{json_column}.customer.email").alias("email"),
        F.col(f"{json_column}.product").alias("product"),
        F.col(f"{json_column}.price").alias("price"),
        F.col(f"{json_column}.quantity").alias("quantity"),
        F.col(f"{json_column}.city").alias("city"),
        F.col(f"{json_column}.status").alias("status"),
        F.col(f"{json_column}.order_time").alias("order_time"),
    )

    return df_flat
