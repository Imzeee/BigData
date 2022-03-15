// Databricks notebook source
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, StructField}
val schemat = StructType(Array(
  StructField("imdb_title_id",StringType, false),
  StructField("ordering", IntegerType, false),
  StructField("imdb_name_id", StringType,false),
  StructField("category", StringType, false),
  StructField("job", StringType, false),
  StructField("characters", StringType, false)))

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/actors.csv"
val file = spark.read.format("csv").option("header","true").schema(schemat).load(filePath)
display(file)

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/actors_json.json"

val schema_json=new StructType()
.add("imdb_title_id",StringType, true)
.add("ordering", IntegerType, true)
.add("imdb_name_id",StringType, true)
.add("job", StringType, true)
.add("characters", StringType, true)

val df_with_schema = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").schema(schema_json).json(filePath)
df_with_schema.printSchema()
df_with_schema.show(false  )
  
  
  

// COMMAND ----------



// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/actors.csv"
val file_1 = spark.read.format("csv").schema(schemat).option("badRecordsPath", "/tmp/source/badrecords").load(filePath)

val file_2 = spark.read.format("csv").option("mode","PERMISSIVE").schema(schemat).load(filePath)

val file_3 = spark.read.format("csv").option("mode","DROPMALFORMED").schema(schemat).load(filePath)

val file_4 = spark.read.format("csv").option("mode","FAILFAST").schema(schemat).load(filePath)

  display(file_1)


// COMMAND ----------

file_1.write.parquet("/tmp/output/actors.parquet")

// COMMAND ----------

// MAGIC %md
// MAGIC W docelowej lokalizacji widzimy, że DataFrame został zapisany w katalogu actors.parquet, który zawiera pliki SUCCESS, committed, started oraz dane podzielone i zapisane na odpowiednich partycjach.

// COMMAND ----------

val file_parquet= spark.read.parquet("/tmp/output/actors.parquet")
display(file_parquet)