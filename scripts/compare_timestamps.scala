import org.apache.spark.sql.SparkSession
import java.io.PrintWriter
import scala.util.Try

val drawPath = sys.props.get("rawPath").getOrElse("/tmp/data/raw_parquet")
val hudiPath = sys.props.get("hudiPath").getOrElse("/tmp/data/hudi_tables")
val diffPath = sys.props.get("diffPath").getOrElse("/tmp/results/diff.txt")

val spark = SparkSession.builder()
  .appName("CompareTimestamps")
  .master("local[*]")
  .getOrCreate()

// Load Parquet source
val parquetDF = spark.read.option("mergeSchema", "true").parquet(drawPath + "/*")

// Load Hudi table data saved by read_hudi_fixed.scala
val hudiDF = spark.read.json(hudiPath + "/*")

// Compare timestamp columns
val tsColumns = Seq("ts_millis", "ts_micros", "local_ts")
val diffs = tsColumns.flatMap { col =>
  val parquetVals = Try(parquetDF.select(col).distinct().collect().map(_.get(0)).toSet).getOrElse(Set.empty)
  val hudiVals = Try(hudiDF.select(col).distinct().collect().map(_.get(0)).toSet).getOrElse(Set.empty)
  val diff = (parquetVals -- hudiVals) ++ (hudiVals -- parquetVals)
  if (diff.nonEmpty) Some(s"$col mismatch: ${diff.mkString(", ")}") else None
}

val pw = new PrintWriter(diffPath)
if (diffs.isEmpty) {
  pw.write("✅ All timestamp values match!\n")
} else {
  pw.write(diffs.mkString("\n"))
}
pw.close()

println(s"Timestamp comparison complete. See diff report: $diffPath")
System.exit(0)
