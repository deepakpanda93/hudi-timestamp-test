import org.apache.spark.sql.SparkSession
import java.io.PrintWriter

val tableName = sys.props.get("tableName").getOrElse("hudi_table")
val batchId = sys.props.get("batchId").getOrElse("1")

val spark = SparkSession.builder()
  .appName("ReadFixedHudi")
  .master("local[*]")
  .getOrCreate()

val tablePath = s"file:///tmp/data/hudi_tables/${tableName}_1.1.0_batch${batchId}"

// Read Hudi table
val df = spark.read.format("hudi").load(tablePath + "/*")

// Save schema + data for comparison
val schemaOut = s"/tmp/results/comparison/${tableName}_batch${batchId}_schema.json"
val dataOut = s"/tmp/results/comparison/${tableName}_batch${batchId}_data.json"

df.schema.json // save schema
new PrintWriter(schemaOut) { write(df.schema.json); close() }
df.coalesce(1).write.mode("overwrite").json(dataOut)

println(s"Read fixed Hudi for $tableName batch $batchId -> schema: $schemaOut, data: $dataOut")
