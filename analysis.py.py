"""
IST3134 Big Data Project — US Accidents Analysis
Run as an EMR Step (Spark application) with:
  --input  s3://your-bucket/raw/US_Accidents_March23.csv
  --output s3://your-bucket/output/

Example Step argument string:
  --input s3://ist3134-us-accidents-yourname/raw/US_Accidents_March23.csv --output s3://ist3134-us-accidents-yourname/output/
"""

import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="S3 path to the raw CSV")
    parser.add_argument("--output", required=True, help="S3 path (folder) to write results to")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("USAccidentsAnalysis").getOrCreate()

    timings = {}

    # ---------- Load ----------
    t0 = time.time()
    df = spark.read.csv(
        args.input,
        header=True,
        inferSchema=True,
        escape='"',
        multiLine=True,
    )
    row_count = df.count()
    timings["load_and_count"] = time.time() - t0
    print(f"[INFO] Loaded {row_count} rows in {timings['load_and_count']:.2f}s")
    df.printSchema()

    # ---------- Clean ----------
    t0 = time.time()

    # Drop columns that are fully/mostly null and not needed for this analysis
    drop_cols = ["End_Lat", "End_Lng"]
    df_clean = df.drop(*[c for c in drop_cols if c in df.columns])

    # Impute nulls: numeric weather fields -> 0 where missing (documented assumption)
    numeric_fill_cols = ["Wind_Chill(F)", "Precipitation(in)", "Wind_Speed(mph)"]
    fill_values = {c: 0.0 for c in numeric_fill_cols if c in df_clean.columns}
    df_clean = df_clean.fillna(fill_values)

    # Drop rows missing core fields we need for the analysis
    core_cols = ["Severity", "Start_Time", "State", "Weather_Condition"]
    df_clean = df_clean.dropna(subset=[c for c in core_cols if c in df_clean.columns])

    # Extract time features
    df_clean = df_clean.withColumn("hour", F.hour("Start_Time"))
    df_clean = df_clean.withColumn("month", F.month("Start_Time"))

    df_clean = df_clean.cache()
    clean_count = df_clean.count()  # materialize the cache
    timings["clean"] = time.time() - t0
    print(f"[INFO] Cleaned data: {clean_count} rows remain, in {timings['clean']:.2f}s")

    # ---------- Benchmark aggregations ----------
    # Each of these is timed individually — mirror the same operations in your
    # pandas comparison script so the timings are apples-to-apples.

    t0 = time.time()
    by_state = (
        df_clean.groupBy("State")
        .agg(
            F.count("*").alias("accident_count"),
            F.avg("Severity").alias("avg_severity"),
        )
        .orderBy(F.desc("accident_count"))
    )
    by_state.write.mode("overwrite").option("header", True).csv(args.output.rstrip("/") + "/by_state")
    timings["agg_by_state"] = time.time() - t0
    print(f"[INFO] Aggregation by State done in {timings['agg_by_state']:.2f}s")

    t0 = time.time()
    by_hour = (
        df_clean.groupBy("hour")
        .agg(
            F.count("*").alias("accident_count"),
            F.avg("Severity").alias("avg_severity"),
        )
        .orderBy("hour")
    )
    by_hour.write.mode("overwrite").option("header", True).csv(args.output.rstrip("/") + "/by_hour")
    timings["agg_by_hour"] = time.time() - t0
    print(f"[INFO] Aggregation by hour done in {timings['agg_by_hour']:.2f}s")

    t0 = time.time()
    by_weather = (
        df_clean.groupBy("Weather_Condition")
        .agg(
            F.count("*").alias("accident_count"),
            F.avg("Severity").alias("avg_severity"),
        )
        .orderBy(F.desc("accident_count"))
    )
    by_weather.write.mode("overwrite").option("header", True).csv(args.output.rstrip("/") + "/by_weather")
    timings["agg_by_weather"] = time.time() - t0
    print(f"[INFO] Aggregation by weather done in {timings['agg_by_weather']:.2f}s")

    # ---------- Save timing summary ----------
    timings_rows = [(k, float(v)) for k, v in timings.items()]
    timings_df = spark.createDataFrame(timings_rows, ["operation", "seconds"])
    timings_df.write.mode("overwrite").option("header", True).csv(args.output.rstrip("/") + "/timings")

    print("[INFO] All done. Timing summary:")
    for k, v in timings.items():
        print(f"  {k}: {v:.2f}s")

    spark.stop()


if __name__ == "__main__":
    main()
