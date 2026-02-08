import os
from pyspark.sql import DataFrame

from src.pipeline.clean import dedup


def merge_into_snapshot(spark, new_df: DataFrame, output_path: str) -> DataFrame:
    if os.path.exists(output_path):
        existing_df = spark.read.parquet(output_path)
        merged = existing_df.unionByName(new_df, allowMissingColumns=True)
    else:
        merged = new_df

    merged = dedup(merged)
    return merged
