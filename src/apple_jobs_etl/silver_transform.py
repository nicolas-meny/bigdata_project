import argparse
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from .s3_utils import download_file, upload_directory


def build_spark(app_name: str = "apple-jobs-silver") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Silver layer: clean & structure raw CSV with PySpark")
    parser.add_argument("--bucket", default=os.environ.get("LAB_BUCKET_NAME") or "user-n-meny-ece")
    parser.add_argument("--bronze-key", default="Projet/apple_jobs.csv")
    parser.add_argument("--silver-prefix", default="silver/apple_jobs_clean")
    parser.add_argument("--work-dir", default="/tmp/medallion_work")
    args = parser.parse_args()

    work = Path(args.work_dir)
    raw_local = work / "bronze" / "apple_jobs.csv"
    silver_local = work / "silver" / "apple_jobs_clean"

    download_file(args.bucket, args.bronze_key, str(raw_local))

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df_raw = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(str(raw_local))
    )
    raw_count = df_raw.count()

    df_dedup = df_raw.dropDuplicates()
    dedup_count = df_dedup.count()

    df_norm = (
        df_dedup.withColumn("title_clean", F.trim(F.regexp_replace(F.col("title"), r"\s+", " ")))
        .withColumn("location_clean", F.trim(F.regexp_replace(F.col("location"), r"\s+", " ")))
        .withColumn(
            "full_text_lower",
            F.lower(
                F.concat_ws(
                    " ",
                    F.coalesce(F.col("title"), F.lit("")),
                    F.coalesce(F.col("minimum_qual"), F.lit("")),
                    F.coalesce(F.col("preferred_qual"), F.lit("")),
                    F.coalesce(F.col("responsibilities"), F.lit("")),
                    F.coalesce(F.col("education&experience"), F.lit("")),
                )
            ),
        )
    )

    parts = F.split(F.col("location_clean"), ",")
    n_parts = F.size(parts)

    df_silver = (
        df_norm.withColumn("country", F.trim(F.element_at(parts, -1)))
        .withColumn(
            "region",
            F.when(n_parts >= 2, F.trim(F.element_at(parts, -2))).otherwise(F.lit("Unknown")),
        )
        .withColumn(
            "city",
            F.when(n_parts >= 3, F.trim(F.element_at(parts, -3))).otherwise(F.lit("Unknown")),
        )
        .withColumn("has_python", F.col("full_text_lower").rlike(r"\bpython\b").cast("int"))
        .withColumn("has_java", F.col("full_text_lower").rlike(r"\bjava\b").cast("int"))
        .withColumn("has_sql", F.col("full_text_lower").rlike(r"\bsql\b").cast("int"))
        .withColumn("has_linux", F.col("full_text_lower").rlike(r"\blinux\b").cast("int"))
        .withColumn("has_spark", F.col("full_text_lower").rlike(r"\bspark\b").cast("int"))
        .withColumn("has_ml", F.col("full_text_lower").rlike(r"\bmachine learning\b").cast("int"))
        .withColumn(
            "degree_level",
            F.when(F.col("full_text_lower").rlike(r"\b(phd|ph\.d\.|doctorate)\b"), "PhD")
            .when(F.col("full_text_lower").rlike(r"\b(master|ms|m\.s\.|mba)\b"), "Master")
            .when(F.col("full_text_lower").rlike(r"\b(bachelor|bs|b\.s\.|ba|b\.a\.)\b"), "Bachelor")
            .otherwise("Other/Unspecified"),
        )
        .select(
            "title_clean",
            "city",
            "region",
            "country",
            "degree_level",
            "has_python",
            "has_java",
            "has_sql",
            "has_linux",
            "has_spark",
            "has_ml",
        )
    )

    df_silver.write.mode("overwrite").parquet(str(silver_local))
    print(f"[SILVER] Raw rows: {raw_count} | Deduplicated rows: {dedup_count} | Duplicates removed: {raw_count - dedup_count}")
    spark.stop()

    upload_directory(str(silver_local), args.bucket, args.silver_prefix)


if __name__ == "__main__":
    main()