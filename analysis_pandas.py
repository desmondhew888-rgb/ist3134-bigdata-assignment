"""
IST3134 Big Data Project — US Accidents Analysis (pandas comparison)

Mirrors analysis.py (the PySpark/EMR version) exactly: same cleaning steps,
same three aggregations, same timing methodology — so the results are
directly comparable.

Usage:
    python analysis_pandas.py --input US_Accidents_March23.csv --output ./pandas_output/
"""

import argparse
import os
import time

import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to the local CSV file")
    parser.add_argument("--output", required=True, help="Local folder to write results to")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    timings = {}

    # ---------- Load ----------
    t0 = time.time()
    df = pd.read_csv(args.input, low_memory=False)
    row_count = len(df)
    timings["load_and_count"] = time.time() - t0
    print(f"[INFO] Loaded {row_count} rows in {timings['load_and_count']:.2f}s")

    # ---------- Clean (mirrors analysis.py exactly) ----------
    t0 = time.time()

    drop_cols = ["End_Lat", "End_Lng"]
    df_clean = df.drop(columns=[c for c in drop_cols if c in df.columns])

    numeric_fill_cols = ["Wind_Chill(F)", "Precipitation(in)", "Wind_Speed(mph)"]
    fill_values = {c: 0.0 for c in numeric_fill_cols if c in df_clean.columns}
    df_clean = df_clean.fillna(fill_values)

    core_cols = ["Severity", "Start_Time", "State", "Weather_Condition"]
    df_clean = df_clean.dropna(subset=[c for c in core_cols if c in df_clean.columns])

    df_clean["Start_Time"] = pd.to_datetime(df_clean["Start_Time"], errors="coerce")
    df_clean = df_clean.dropna(subset=["Start_Time"])  # drop rows where datetime parsing failed
    df_clean["hour"] = df_clean["Start_Time"].dt.hour
    df_clean["month"] = df_clean["Start_Time"].dt.month

    clean_count = len(df_clean)
    timings["clean"] = time.time() - t0
    print(f"[INFO] Cleaned data: {clean_count} rows remain, in {timings['clean']:.2f}s")

    # ---------- Benchmark aggregations (identical logic to analysis.py) ----------

    t0 = time.time()
    by_state = (
        df_clean.groupby("State")
        .agg(accident_count=("Severity", "count"), avg_severity=("Severity", "mean"))
        .reset_index()
        .sort_values("accident_count", ascending=False)
    )
    by_state.to_csv(os.path.join(args.output, "by_state.csv"), index=False)
    timings["agg_by_state"] = time.time() - t0
    print(f"[INFO] Aggregation by State done in {timings['agg_by_state']:.2f}s")

    t0 = time.time()
    by_hour = (
        df_clean.groupby("hour")
        .agg(accident_count=("Severity", "count"), avg_severity=("Severity", "mean"))
        .reset_index()
        .sort_values("hour")
    )
    by_hour.to_csv(os.path.join(args.output, "by_hour.csv"), index=False)
    timings["agg_by_hour"] = time.time() - t0
    print(f"[INFO] Aggregation by hour done in {timings['agg_by_hour']:.2f}s")

    t0 = time.time()
    by_weather = (
        df_clean.groupby("Weather_Condition")
        .agg(accident_count=("Severity", "count"), avg_severity=("Severity", "mean"))
        .reset_index()
        .sort_values("accident_count", ascending=False)
    )
    by_weather.to_csv(os.path.join(args.output, "by_weather.csv"), index=False)
    timings["agg_by_weather"] = time.time() - t0
    print(f"[INFO] Aggregation by weather done in {timings['agg_by_weather']:.2f}s")

    # ---------- Save timing summary (same schema as Spark's timings output) ----------
    timings_df = pd.DataFrame(
        [(k, v) for k, v in timings.items()], columns=["operation", "seconds"]
    )
    timings_df.to_csv(os.path.join(args.output, "timings.csv"), index=False)

    print("[INFO] All done. Timing summary:")
    for k, v in timings.items():
        print(f"  {k}: {v:.2f}s")
    print(f"\nTotal: {sum(timings.values()):.2f}s")


if __name__ == "__main__":
    main()
