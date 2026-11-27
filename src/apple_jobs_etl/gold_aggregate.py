import argparse
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from .s3_utils import download_prefix, upload_file


def build_spark(app_name: str = "apple-jobs-gold") -> SparkSession:
    return SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold layer: business aggregations from Silver Parquet")
    parser.add_argument("--bucket", default=os.environ.get("LAB_BUCKET_NAME") or "user-n-meny-ece")
    parser.add_argument("--silver-prefix", default="silver/apple_jobs_clean")
    parser.add_argument("--gold-key", default="gold/country_skills_summary.csv")
    parser.add_argument("--work-dir", default="/tmp/medallion_work")
    args = parser.parse_args()

    work = Path(args.work_dir)
    silver_local = work / "silver" / "apple_jobs_clean"
    gold_tmp = work / "gold_tmp"
    gold_csv = work / "gold" / "country_skills_summary.csv"
    gold_csv.parent.mkdir(parents=True, exist_ok=True)

    download_prefix(args.bucket, args.silver_prefix, str(silver_local))

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df_silver = spark.read.parquet(str(silver_local))

    df_gold = (
        df_silver.groupBy("country")
        .agg(
            F.count("*").alias("total_jobs"),
            F.sum("has_python").alias("python_jobs"),
            F.sum("has_java").alias("java_jobs"),
            F.sum("has_sql").alias("sql_jobs"),
            F.sum("has_linux").alias("linux_jobs"),
            F.sum("has_spark").alias("spark_jobs"),
            F.sum("has_ml").alias("ml_jobs"),
            F.round(F.avg("has_python") * 100, 2).alias("python_pct"),
        )
        .orderBy(F.desc("total_jobs"), F.asc("country"))
    )

    df_gold.show(15, truncate=False)

    df_gold.coalesce(1).write.mode("overwrite").option("header", True).csv(str(gold_tmp))
    part_file = next(gold_tmp.glob("part-*.csv"))
    part_file.replace(gold_csv)
    print(f"[GOLD] Aggregated CSV written to {gold_csv}")

    spark.stop()

    upload_file(str(gold_csv), args.bucket, args.gold_key)


if __name__ == "__main__":
    main()