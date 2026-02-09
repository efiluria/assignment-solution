import sys
import yaml
from pyspark.sql import SparkSession, DataFrame

from src.pipeline.clean import clean
from src.pipeline.merge import merge_into_snapshot
from src.pipeline.parse import parse_json_column
import os


def load_config(path="config/config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    spark = (
        SparkSession.builder.master("local[*]").appName("LocalPySpark").getOrCreate()
    )

    try:
        config = load_config()
        input_file_path = os.getenv("INPUT_FILE_NAME", "data/raw/orders_raw_data.csv")
        print(f"Reading input file: {input_file_path}")

        df = (
            spark.read.option("header", "true")
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", True)
            .csv(input_file_path)
        )

        df_parsed = parse_json_column(df, "raw_payload")

        df_cleaned = clean(df_parsed)

        final_df = merge_into_snapshot(spark, df_cleaned, config["output_file"])

        final_df.write.mode("overwrite").parquet(config["output_file"])

        print("Pipeline finished successfully")

    except Exception as e:
        print(f"Pipeline failed: {str(e)}", file=sys.stderr)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
