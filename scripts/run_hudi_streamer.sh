#!/bin/bash
set -e

# Parameters
HUDI_VERSION=$1       # e.g., 0.14.0 or 1.2.0
BATCH_ID=$2
TABLE_NAME=$3         # e.g., hudi_table_T_BULK_INSERT_null
USE_TRANSFORMER=$4    # true/false
ROW_WRITER=$5         # BULK_INSERT or UPSERT
SCHEMA_PROVIDER=$6    # e.g., null or file

# Source parquet directory (same for both batches)
SRC_DIR="file:///opt/onehouse/data/timestamp/raw_parquet"

# Target Hudi table directory (same for both batches)
TARGET_DIR="file:///opt/onehouse/data/timestamp/hudi_tables/${TABLE_NAME}"

# Determine jars based on Hudi version + schema provider
if [[ "$HUDI_VERSION" == "0.14.0" ]]; then
    HUDI_SPARK_JAR="file:///opt/jars/null/hudi-spark3.4-bundle_2.12-0.14.0.jar"
    HUDI_UTIL_JAR="file:///opt/jars/null/hudi-utilities-slim-bundle_2.12-0.14.0.jar"
else
    HUDI_SPARK_JAR="file:///opt/jars/null/hudi-spark3.4-bundle_2.12-1.2.0-SNAPSHOT.jar"
    HUDI_UTIL_JAR="file:///opt/jars/null/hudi-utilities-slim-bundle_2.12-1.2.0-SNAPSHOT.jar"
fi

# Transformer configuration
EXTRA_OPTS=()
if [[ "$USE_TRANSFORMER" == "true" ]]; then
  EXTRA_OPTS+=(
    --transformer-class org.apache.hudi.utilities.transform.SqlQueryBasedTransformer
    --hoodie-conf "hoodie.streamer.transformer.sql=SELECT id, event_name, ts_millis, ts_micros, local_ts FROM parquet.`/opt/onehouse/data/timestamp/raw_parquet`"
  )
fi

# Row writer configuration
if [[ "$ROW_WRITER" == "BULK_INSERT" ]]; then
  EXTRA_OPTS+=(--hoodie-conf hoodie.streamer.write.row.writer.enable=true)
else
  EXTRA_OPTS+=(--hoodie-conf hoodie.streamer.write.row.writer.enable=false)
fi

# Schema provider class
SCHEMA_PROVIDER_CLASS="org.apache.hudi.utilities.schema.FilebasedSchemaProvider"
if [[ "$SCHEMA_PROVIDER" == "null" ]]; then
  SCHEMA_PROVIDER_CLASS="org.apache.hudi.utilities.schema.NullTargetFilebasedSchemaProvider"
fi

# Spark-submit command
spark-submit \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  --jars "${HUDI_SPARK_JAR},${HUDI_UTIL_JAR}" \
  "${HUDI_UTIL_JAR}" \
  --table-type COPY_ON_WRITE \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
  --target-base-path "${TARGET_DIR}" \
  --target-table "${TABLE_NAME}" \
  --schemaprovider-class "${SCHEMA_PROVIDER_CLASS}" \
  --hoodie-conf hoodie.deltastreamer.source.dfs.root="${SRC_DIR}" \
  --hoodie-conf hoodie.deltastreamer.schemaprovider.source.schema.file=file:///tmp/schemas/schema.avsc \
  --hoodie-conf hoodie.deltastreamer.schemaprovider.target.schema.file=file:///tmp/schemas/schema.avsc \
  --hoodie-conf hoodie.datasource.write.recordkey.field=id \
  --hoodie-conf hoodie.datasource.write.precombine.field=event_name \
  --source-ordering-field event_name \
  "${EXTRA_OPTS[@]}" \
  --hoodie-conf hoodie.parquet.small.file.limit=-1
