# Big Data vs. Traditional Analytics: A Performance Comparison Using US Traffic Accident Data

**Module:** IST3134 — Big Data Analytics in the Cloud
**Team members:** [Hew Zhen Wei], [Koo Yu Jian]

## Problem Statement

How much faster and more scalable is a Spark-based analysis of millions of traffic accident records compared to a traditional single-machine pandas approach?

## Dataset

**US Accidents (2016–2023)** — Sobhan Moosavi, Kaggle
Link: https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents

~7.7 million accident records across the United States, including severity, location, time, weather conditions, and road features.

> **Note on hosting:** The full dataset (several GB) exceeds GitHub's file size limits and is therefore not hosted directly in this repository. Please download it from the Kaggle link above to reproduce this analysis. A small sample (`sample_data/US_Accidents_sample.csv`, 200 rows) is included in this repo for quick inspection of the schema.

## Repository Structure

```
ist3134-bigdata-accidents/
├── README.md
├── code/
│   ├── analysis.py            # PySpark version — run as an AWS EMR Step
│   └── analysis_pandas.py     # pandas comparison version — run locally
├── results/
│   ├── spark_timings.csv
│   ├── pandas_timings.csv
│   ├── by_state.csv
│   ├── by_hour.csv
│   └── by_weather.csv
└── sample_data/
    └── US_Accidents_sample.csv
```

## Approach

Both scripts perform the identical pipeline, for a fair comparison:

1. **Load** the raw CSV.
2. **Clean:** drop `End_Lat`/`End_Lng` (fully null), impute missing `Wind_Chill(F)`, `Precipitation(in)`, `Wind_Speed(mph)` with 0, drop rows missing `Severity`, `Start_Time`, `State`, or `Weather_Condition`, and extract `hour`/`month` from `Start_Time`.
3. **Aggregate:** accident count and average severity, grouped by `State`, by `hour`, and by `Weather_Condition`.
4. **Time** each step individually.

- `analysis.py` runs the pipeline in **PySpark**, deployed as a Step on an **AWS EMR** cluster, reading from and writing to an S3 bucket.
- `analysis_pandas.py` runs the identical pipeline locally in **pandas**, for the performance comparison.

## How to Reproduce

### Spark version (AWS EMR)
1. Download the dataset from the Kaggle link above.
2. Upload it to an S3 bucket, e.g. `s3://your-bucket/raw/US_Accidents_March23.csv`.
3. Upload `code/analysis.py` to `s3://your-bucket/scripts/analysis.py`.
4. On a running EMR cluster (Spark application bundle), add a Step:
   - Application location: `s3://your-bucket/scripts/analysis.py`
   - Arguments:
     ```
     --input s3://your-bucket/raw/US_Accidents_March23.csv --output s3://your-bucket/output/
     ```
5. Results are written to `s3://your-bucket/output/` (`by_state/`, `by_hour/`, `by_weather/`, `timings/`).

### Pandas version (local)
```bash
pip install pandas
python code/analysis_pandas.py --input US_Accidents_March23.csv --output ./pandas_output/
```

## Results

### Timing comparison

| Operation | Spark (s) | Pandas (s) | Faster | Ratio |
|---|---|---|---|---|
| load_and_count | 97.52 | 154.93 | Spark | 1.59× |
| clean | 77.63 | 23.80 | Pandas | 3.26× |
| agg_by_state | 2.13 | 0.67 | Pandas | 3.18× |
| agg_by_hour | 1.49 | 0.22 | Pandas | 6.77× |
| agg_by_weather | 1.18 | 0.68 | Pandas | 1.74× |
| **Total** | **179.95** | **180.30** | ~Tie | ~1.00× |

### Key finding

While total pipeline runtime was comparable between Spark and pandas at this dataset scale (~7.7M rows), the two approaches diverged significantly by operation type. Spark's distributed architecture provided a clear advantage for I/O-bound operations (1.59× faster load time via parallel reads from S3), but introduced coordination overhead that made it 1.7×–6.8× slower than pandas for in-memory aggregations. This suggests Big Data tooling's benefits are workload-dependent, and become more pronounced as dataset size approaches or exceeds a single machine's memory capacity — beyond the scope of this dataset.

### Top findings from the data itself
- **California** had the highest accident count (1,701,655), followed by Florida and Texas.
- **"Fair" weather** was associated with the highest raw accident count (2,560,802) — likely reflecting exposure (most driving happens in fair weather) rather than risk per trip.
- Accident counts were highest in early morning hours (e.g., hour 0: 108,901 accidents).

## Individual Reflections

See `report/` for the full written report, including individual reflections from each team member.
