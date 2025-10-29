import subprocess
import csv
from pathlib import Path

# -----------------------
# CONFIGURATION
# -----------------------
batch1_version = "0.14.0"
fixed_version = "1.1.0"  # fixed Hudi jar version
batch_id = 1  # same parquet dataset used for both

# Base paths
base_parquet_dir = Path("/opt/onehouse/data/timestamp/raw_parquet")
base_hudi_dir = Path("/opt/onehouse/data/timestamp/hudi_tables")
results_dir = Path("/opt/onehouse/data/timestamp/results")
results_dir.mkdir(exist_ok=True)

# 8 configuration combinations
transformer_options = [True, False]
row_writer_options = ["BULK_INSERT", "UPSERT"]
schema_provider_options = ["file", "null"]

# JAR sets
JARS_FILE_PROVIDER = "/opt/jars/null/hudi-spark3.4-bundle_2.12-1.2.0-SNAPSHOT.jar,/opt/jars/null/hudi-utilities-slim-bundle_2.12-1.2.0-SNAPSHOT.jar"
JARS_NULL_PROVIDER = "/opt/jars/null/hudi-spark3.4-bundle_2.12-1.2.0-SNAPSHOT.jar,/opt/jars/null/hudi-utilities-slim-bundle_2.12-1.2.0-SNAPSHOT.jar"

summary_csv = results_dir / "timestamp_summary.csv"

# -----------------------
# HELPERS
# -----------------------
def run(cmd):
    """Run a shell command with output."""
    print("▶", " ".join(cmd))
    subprocess.run(cmd, check=True)

def select_jars(schema_provider: str) -> str:
    """Return correct jar list based on schema provider."""
    if schema_provider == "file":
        return JARS_FILE_PROVIDER
    else:
        return JARS_NULL_PROVIDER

def run_generate_parquet():
    """Generate Parquet source data once."""
    print("\n🧱 Generating Parquet data (batchId=1) ...")
    jar_path = select_jars("file")
    run([
        "spark-shell", "-i", "scripts/generate_parquet.scala",
        "--jars", jar_path,
        "--conf", f"spark.driver.extraJavaOptions=-DbatchId={batch_id}"
    ])

def run_hudi_streamer(version, table_name, transformer, row_writer, schema_provider):
    """Run Hudi streamer for one configuration."""
    print(f"\n🚀 Running Hudi Streamer (v{version}) for table {table_name} with schema provider {schema_provider}")
    transformer_flag = "true" if transformer else "false"
    run([
        "bash", "scripts/run_hudi_streamer.sh",
        version, str(batch_id), table_name,
        transformer_flag, row_writer, schema_provider
    ])

def read_hudi_fixed(table_name, schema_provider):
    """Read table schema and data using the fixed jar."""
    jar_path = select_jars(schema_provider)
    print(f"\n📖 Reading Hudi table {table_name} with fixed jar for schema provider {schema_provider} ...")
    run([
        "spark-shell", "-i", "scripts/read_hudi_fixed.scala",
        "--jars", jar_path,
        "--conf", f"spark.driver.extraJavaOptions=-DtableName={table_name}"
    ])

def compare_timestamps(table_name, diff_file):
    """Compare parquet schema vs. table schema values."""
    print(f"\n🔍 Comparing timestamps for table {table_name}")
    run([
        "spark-shell", "-i", "scripts/compare_timestamps.scala",
        "--conf", f"spark.driver.extraJavaOptions=-DrawPath={base_parquet_dir} "
                  f"-DhudiPath={base_hudi_dir}/{table_name} -DdiffPath={diff_file}"
    ])

# -----------------------
# MAIN EXECUTION
# -----------------------
with open(summary_csv, mode="w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=[
        "hudi_version", "transformer", "row_writer", "schema_provider", "status", "diff_file"
    ])
    writer.writeheader()

    # -------------------------------------------------
    # STEP 1 : Generate Parquet Data
    # -------------------------------------------------
    run_generate_parquet()

    # -------------------------------------------------
    # STEP 2 : Batch 1 (Hudi 0.14.0) - Create Tables
    # -------------------------------------------------
    for transformer in transformer_options:
        for row_writer in row_writer_options:
            for schema_provider in schema_provider_options:
                table_name = f"hudi_table_{'T' if transformer else 'F'}_{row_writer}_{schema_provider}"
                diff_file = results_dir / f"diff_{table_name}.txt"
                try:
                    run_hudi_streamer(batch1_version, table_name, transformer, row_writer, schema_provider)
                    status = "CREATED"
                except subprocess.CalledProcessError:
                    status = "FAIL_CREATE"

                writer.writerow({
                    "hudi_version": batch1_version,
                    "transformer": transformer,
                    "row_writer": row_writer,
                    "schema_provider": schema_provider,
                    "status": status,
                    "diff_file": "N/A"
                })

    # -------------------------------------------------
    # STEP 3 : Batch 2 (Hudi 1.2.0 Fixed) - Reuse same tables
    # -------------------------------------------------
    print("\n====================")
    print("🔧 Running FIXED (1.2.0) streamer on same tables")
    print("====================\n")

    for transformer in transformer_options:
        for row_writer in row_writer_options:
            for schema_provider in schema_provider_options:
                table_name = f"hudi_table_{'T' if transformer else 'F'}_{row_writer}_{schema_provider}"
                diff_file = results_dir / f"diff_{table_name}.txt"
                try:
                    run_hudi_streamer(fixed_version, table_name, transformer, row_writer, schema_provider)
                    read_hudi_fixed(table_name, schema_provider)
                    compare_timestamps(table_name, diff_file)
                    status = "PASS"
                except subprocess.CalledProcessError:
                    status = "FAIL"

                writer.writerow({
                    "hudi_version": fixed_version,
                    "transformer": transformer,
                    "row_writer": row_writer,
                    "schema_provider": schema_provider,
                    "status": status,
                    "diff_file": diff_file if status == "PASS" else "N/A"
                })

print(f"\n✅ All runs complete. Summary written to: {summary_csv}")
