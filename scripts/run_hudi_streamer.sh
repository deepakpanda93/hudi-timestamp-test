#!/bin/bash
set -e

HUDI_VERSION=$1       # 0.14.0, 0.14.1, 0.15.0
BATCH_ID=$2
TABLE_NAME=$3
USE_TRANSFORMER=$4
ROW_WRITER=$5
SCHEMA_PROVIDER=$6

SRC_DIR="file:///tmp/data/raw_parquet/batch_$BATCH_ID"
TARGET_DIR="file:///tmp/data/hudi_tables/${TABLE_NAME}_${HUDI_VERSION}_batch${BATCH_ID}"

# Determine jars based on Hudi version + schema provider
if [ "$HUDI_VERSION" == "0.14.0" ]; then
    if [ "$SCHEMA_PROVIDER" == "null" ]; then
        HUDI_SPARK_JAR="jars/hudi-0.14-spark-null-bundle.jar"
        HUDI_UTIL_JAR="jars/hudi-0.14-utilities-null-bundle.jar"
    else
        HUDI_SPARK_JAR="jars/hudi-0.14-spark-file-bundle.jar"
        HUDI_UTIL_JAR="jars/hudi-0.14-utilities-file-bundle.jar"
    fi
else  # fixed 1.1.0
    if [ "$SCHEMA_PROVIDER" == "null" ]; then
        HUDI_SPARK_JAR="jars/hudi-1.1-spark-null-bundle.jar"
        HUDI_UTIL_JAR="jars/hudi-1.1-utilities-null-bundle.jar"
    else
        HUDI_SPARK_JAR="jars/hudi-1.1-spark-file-bundle.jar"
        HUDI_UTIL_JAR="jars/hudi-1.1-utilities-file-bundle.jar"
    fi
fi

TRANSFORMER_CONF=""
if [ "$USE_TRANSFORMER" == "true" ]; then
  TRANSFORMER_CONF="--transformer-class org.apache.hudi.utilities.transform.SqlQueryBasedTransformer \
  --hoodie-conf hoodie.streamer.transformer.sql='SELECT id, event_name, ts_millis, ts_micros, local_ts FROM <SRC>'"
fi

ROW_WRITER_CONF=""
if [ "$ROW_WRITER" == "BULK_INSERT" ]; then
  ROW_WRITER_CONF="--hoodie-conf hoodie.streamer.write.row.writer.enable=true"
else
  ROW_WRITER_CONF="--hoodie-conf hoodie.streamer.write.row.writer.enable=false"
fi

SCHEMA_PROVIDER_CLASS="org.apache.hudi.utilities.schema.FilebasedSchemaProvider"
if [ "$SCHEMA_PROVIDER" == "null" ]; then
  SCHEMA_PROVIDER_CLASS="org.apache.hudi.utilities.schema.NullTargetFilebasedSchemaProvider"
fi

spark-submit \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  --packages ${HUDI_SPARK_JAR},${HUDI_UTIL_JAR} \
  ${HUDI_UTIL_JAR} \
  --table-type COPY_ON_WRITE \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
  --target-base-path ${TARGET_DIR} \
  --target-table ${TABLE_NAME} \
  --schemaprovider-class ${SCHEMA_PROVIDER_CLASS} \
  --hoodie-conf hoodie.deltastreamer.source.dfs.root=${SRC_DIR} \
  --hoodie-conf hoodie.deltastreamer.schemaprovider.source.schema.file=file:///tmp/schemas/schema.avsc \
  --hoodie-conf hoodie.deltastreamer.schemaprovider.target.schema.file=file:///tmp/schemas/schema.avsc \
  --hoodie-conf hoodie.datasource.write.recordkey.field=id \
  --hoodie-conf hoodie.datasource.write.precombine.field=event_name \
  --source-ordering-field event_name \
  ${TRANSFORMER_CONF} \
  ${ROW_WRITER_CONF} \
  --hoodie-conf hoodie.parquet.small.file.limit=-1
