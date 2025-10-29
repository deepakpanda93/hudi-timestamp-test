import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.sql.Timestamp

val batchId: Int = sys.props.get("batchId").map(_.toInt).getOrElse(1)

val spark = SparkSession.builder()
  .appName("GenerateTimestampParquet")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

// Schema compatible with Hudi
val schema = StructType(Seq(
  StructField("id", StringType, true),
  StructField("event_name", StringType, true),
  StructField("ts_millis", LongType, true),
  StructField("ts_micros", TimestampType, true),
  StructField("local_ts", TimestampType, true)
))

val baseTime = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS)

def toTimestamp(local: LocalDateTime): Timestamp = Timestamp.valueOf(local)
def toMillis(local: LocalDateTime): Long = local.atZone(java.time.ZoneId.systemDefault()).toInstant.toEpochMilli

val rows = Seq(
  Row(batchId.toString, "insert_event", toMillis(baseTime), toTimestamp(baseTime), toTimestamp(baseTime)),
  Row((batchId + 1).toString, "update_event", toMillis(baseTime.plusSeconds(5)), toTimestamp(baseTime.plusSeconds(5).plusNanos(123456000)), toTimestamp(baseTime.plusSeconds(5))),
  Row((batchId + 2).toString, "delete_event", toMillis(baseTime.minusMinutes(1)), toTimestamp(baseTime.minusMinutes(1).plusNanos(654321000)), toTimestamp(baseTime.minusMinutes(1)))
)

val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

// Output folder for batch
val outDir = s"file:///tmp/data/raw_parquet/batch_$batchId"
df.repartition(1).write.mode("overwrite").parquet(outDir)

println(s"Batch $batchId Parquet written to $outDir")
