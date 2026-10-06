import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


INPUT_SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("date", StringType(), True),
    StructField("temperature_max_c", DoubleType(), True),
    StructField("temperature_min_c", DoubleType(), True),
    StructField("precipitation_mm", DoubleType(), True),
])


def transform_weather(raw_df):
    normalized_df = (
        raw_df
        .withColumn("city", F.trim(F.col("city")))
        .withColumn(
            "date",
            F.try_to_timestamp(
                F.col("date"),
                F.lit("yyyy-MM-dd"),
            ).cast("date"),
        )
    )

    valid_df = normalized_df.filter(
        F.col("city").isNotNull()
        & (F.length(F.col("city")) > 0)
        & F.col("date").isNotNull()
        & F.col("temperature_max_c").isNotNull()
        & F.col("temperature_min_c").isNotNull()
        & F.col("precipitation_mm").isNotNull()
        & (
            F.col("temperature_max_c")
            >= F.col("temperature_min_c")
        )
        & (F.col("precipitation_mm") >= 0)
    )

    clean_df = (
        valid_df
        .dropDuplicates()
        .withColumn(
            "temperature_range_c",
            F.round(
                F.col("temperature_max_c")
                - F.col("temperature_min_c"),
                2,
            ),
        )
    )

    return valid_df, clean_df


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Clean weather NDJSON and write it to BigQuery."
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main():
    args = parse_arguments()

    spark = (
        SparkSession.builder
        .appName("weather-transformation")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        raw_df = (
            spark.read
            .schema(INPUT_SCHEMA)
            .option("mode", "FAILFAST")
            .json(args.input)
            .cache()
        )

        valid_df, clean_df = transform_weather(raw_df)
        clean_df = clean_df.cache()

        raw_count = raw_df.count()
        valid_count = valid_df.count()
        clean_count = clean_df.count()

        print(f"Raw records: {raw_count}")
        print(f"Rejected records: {raw_count - valid_count}")
        print(f"Duplicate records removed: {valid_count - clean_count}")
        print(f"Clean records: {clean_count}")

        if clean_count == 0:
            raise ValueError("No valid weather records remain.")

        conflicting_keys = (
            clean_df
            .groupBy("city", "date")
            .count()
            .filter(F.col("count") > 1)
        )

        if conflicting_keys.limit(1).count() > 0:
            raise ValueError(
                "Conflicting observations exist for the same city/date."
            )

        clean_df.printSchema()
        clean_df.orderBy("city", "date").show(30, truncate=False)

        (
            clean_df.write
            .format("bigquery")
            .option("writeMethod", "direct")
            .option("table", args.output)
            .mode("overwrite")
            .save()
        )

        print(f"Successfully wrote to {args.output}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
