# 🕒 Hudi Timestamp Compatibility Test Automation

## Overview
This project automates verification of **timestamp-millis** and **timestamp-micros** handling between older Hudi releases (e.g., `0.14.0`) and the **fixed 1.1.0 branch**.  
It runs a consistent data-ingestion and read-comparison workflow across multiple configuration combinations to ensure timestamp consistency between the **table schema**, **Parquet schema**, and **actual stored values**.

---

## What It Tests

- Timestamp logical types: `timestamp-millis` and `timestamp-micros`
- Behavior differences between Hudi 0.14.0 (pre-fix) and Hudi 1.1.0 (fixed)
- All combinations of:
  - **Transformer**: enabled / disabled
  - **Row writer**: `BULK_INSERT` / `UPSERT`
  - **Schema provider**: `FileBased` / `Null`

Each combination is automatically tested end-to-end.

---

## Test Matrix (8 Configurations)

| Transformer | Row Writer  | Schema Provider | Example Table Name                  |
|------------|-------------|----------------|------------------------------------|
| ✅ Yes     | BULK_INSERT | file           | `hudi_table_T_BULK_INSERT_file`    |
| ✅ Yes     | BULK_INSERT | null           | `hudi_table_T_BULK_INSERT_null`    |
| ✅ Yes     | UPSERT      | file           | `hudi_table_T_UPSERT_file`         |
| ✅ Yes     | UPSERT      | null           | `hudi_table_T_UPSERT_null`         |
| ❌ No      | BULK_INSERT | file           | `hudi_table_F_BULK_INSERT_file`    |
| ❌ No      | BULK_INSERT | null           | `hudi_table_F_BULK_INSERT_null`    |
| ❌ No      | UPSERT      | file           | `hudi_table_F_UPSERT_file`         |
| ❌ No      | UPSERT      | null           | `hudi_table_F_UPSERT_null`         |

---


---

## How It Works

### 1. Generate Parquet Source
Creates test data with timestamps in both millis and micros precision.

```bash
spark-shell -i scripts/generate_parquet.scala \
--conf "spark.driver.extraJavaOptions=-DbatchId=1"
```

### 2. Automate All Tests

Run all configurations for both versions (0.14.0 and fixed 1.1.0):
```bash
python3 scripts/automate_tests.py
```

This script performs:
Runs DeltaStreamer (Hudi 0.14.0) → creates 8 Hudi tables
Runs DeltaStreamer (Hudi 1.1.0 fixed) → reuses same 8 tables
Reads results using the fixed jar
Compares stored vs intended timestamp values
Writes results to a summary CSV and diff files

### Outputs
| Artifact     | Count | Path                                                           |
| ------------ | ----- | -------------------------------------------------------------- |
| Hudi tables  | 8     | `/Users/onehouse/data/timestamp/hudi_tables/`                  |
| Diff reports | 8     | `/Users/onehouse/data/timestamp/results/`                      |
| Summary CSV  | 1     | `/Users/onehouse/data/timestamp/results/timestamp_summary.csv` |
| Raw Parquet  | 1     | `/Users/onehouse/data/timestamp/raw_parquet/`                  |


### Sample timestamp_summary.csv
| hudi_version | transformer | row_writer  | schema_provider | status  | diff_file                   |
| ------------ | ----------- | ----------- | --------------- | ------- | --------------------------- |
| 0.14.0       | True        | BULK_INSERT | file            | CREATED | N/A                         |
| 1.1.0        | True        | BULK_INSERT | file            | PASS    | diff_T_BULK_INSERT_file.txt |
| 1.1.0        | False       | UPSERT      | null            | FAIL    | diff_F_UPSERT_null.txt      |


### Key Scripts
| Script                     | Language | Purpose                                                |
| -------------------------- | -------- | ------------------------------------------------------ |
| `generate_parquet.scala`   | Scala    | Generates test parquet dataset with timestamps         |
| `run_hudi_streamer.sh`     | Bash     | Runs DeltaStreamer for each config                     |
| `read_hudi_fixed.scala`    | Scala    | Reads table data and schema using fixed jar            |
| `compare_timestamps.scala` | Scala    | Compares parquet schema, Hudi schema, and field values |
| `automate_tests.py`        | Python   | Orchestrates all runs, captures logs & summary         |


### Next Steps

Once verified for 0.14.0 → 1.1.0, you can extend this by adding older versions:
batch1_versions = ["0.14.0", "0.14.1", "0.15.0"]
in the automation script and re-run.
