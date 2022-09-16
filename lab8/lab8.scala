// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie?
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomięczy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy rzeby sprawdzić średni czas wykonania.
// MAGIC 
// MAGIC Powtórz z inną liczbą partycji
// MAGIC * 1 partycja
// MAGIC * 7 partycja
// MAGIC * 9 partycja
// MAGIC * 16 partycja
// MAGIC * 24 partycja
// MAGIC * 96 partycja
// MAGIC * 200 partycja
// MAGIC * 4000 partycja
// MAGIC 
// MAGIC Zastąp `repartition(n)` z `coalesce(n)` używając:
// MAGIC * 6 partycji
// MAGIC * 5 partycji
// MAGIC * 4 partycji
// MAGIC * 3 partycji
// MAGIC * 2 partycji
// MAGIC * 1 partycji
// MAGIC 
// MAGIC ** *Note:* ** *Dane muszą być wystarczająco duże żeby zaobserwować duże różnice z małymi partycjami.*<br/>

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).repartition(2000).groupBy("site").sum()

df.explain
df.count()

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).repartition(1)
printf("Partitions: %,d%n", df.rdd.getNumPartitions)

df.groupBy($"site").sum().explain()
df.count()

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).repartition(7)
printf("Partitions: %,d%n", df.rdd.getNumPartitions)

df.groupBy($"site").sum().explain()
df.count()

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).repartition(9)
printf("Partitions: %,d%n", df.rdd.getNumPartitions)

df.groupBy($"site").sum().explain()
df.count()

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).repartition(16)
printf("Partitions: %,d%n", df.rdd.getNumPartitions)

df.groupBy($"site").sum().explain()
df.count()

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).repartition(24)
printf("Partitions: %,d%n", df.rdd.getNumPartitions)

df.groupBy($"site").sum().explain()
df.count()

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).repartition(96)
printf("Partitions: %,d%n", df.rdd.getNumPartitions)

df.groupBy($"site").sum().explain()
df.count()

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).repartition(200)
printf("Partitions: %,d%n", df.rdd.getNumPartitions)

df.groupBy($"site").sum().explain()
df.count()

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).repartition(4000)
printf("Partitions: %,d%n", df.rdd.getNumPartitions)

df.groupBy($"site").sum().explain()
df.count()

// COMMAND ----------

// MAGIC %md
// MAGIC **Coalesce**

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).coalesce(6)
printf("Partitions: %,d%n", df.rdd.getNumPartitions)

df.groupBy($"site").sum().explain()
df.count()

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).coalesce(5)
printf("Partitions: %,d%n", df.rdd.getNumPartitions)

df.groupBy($"site").sum().explain()
df.count()

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).coalesce(4)
printf("Partitions: %,d%n", df.rdd.getNumPartitions)

df.groupBy($"site").sum().explain()
df.count()

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).coalesce(3)
printf("Partitions: %,d%n", df.rdd.getNumPartitions)

df.groupBy($"site").sum().explain()
df.count()

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).coalesce(2)
printf("Partitions: %,d%n", df.rdd.getNumPartitions)

df.groupBy($"site").sum().explain()
df.count()

// COMMAND ----------

spark.catalog.clearCache()
val parquetDir = "/FileStore/tables/pageviews_by_second.parquet"

val df = spark.read.parquet(parquetDir).coalesce(1)
printf("Partitions: %,d%n", df.rdd.getNumPartitions)

df.groupBy($"site").sum().explain()
df.count()

// COMMAND ----------

// MAGIC %md
// MAGIC **Poka Yoke**
// MAGIC <br>
// MAGIC Napisz 6 metod, które mogą być użyte w Pipeline tak aby były odporne na błędy użytkownika oraz jak najbardziej „produkcyjne”. 
// MAGIC Możesz użyć tego co już stworzyłeś i usprawnij rozwiązanie na bardziej odporne na błędy biorąc pod uwagę dobre praktyki. 

// COMMAND ----------

def division(a: Double, b: Double) = {
  if(a == 0 | b == 0)
  "Cannot divide by 0!"
  else
  a/b
}

// COMMAND ----------

printf("Result 1: %.2f\nResult 2: %s", division(10.0, 2.0), division(10.0, 0))

// COMMAND ----------

def sqrt_any_value(a: Double) = {
  import scala.math._
  if(a < 0) {
    sqrt(abs(a))
  }
  else
  sqrt(a)
}

// COMMAND ----------

printf("Result: %.2f", sqrt_any_value(-5.0))

// COMMAND ----------

import org.apache.spark.sql.DataFrame

def int_cols(df :DataFrame) : DataFrame = {
  val intCols = df.schema.fields.filter(_.dataType.typeName == "integer")
  if (intCols == spark.emptyDataFrame)
  {
    println( "There is no integer columns!")
    spark.emptyDataFrame    
  }
  else
   df.select(intCols.map(x=>col(x.name)):_*).show()
}

// COMMAND ----------

val table = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/salesorderdetail")

int_cols(table).show()

// COMMAND ----------

def minimum(name: String, df: DataFrame) = {
  if( df.columns.contains(name) )
    df.describe(name).filter("summary = 'min'").show()
  else
    printf("There is no column: %s!", name)
}

// COMMAND ----------

minimum("SalesOrderID", table)

// COMMAND ----------

minimum("_", table)

// COMMAND ----------

def maximum(name: String, df: DataFrame) = {
  if( df.columns.contains(name) )
    df.describe(name).filter("summary = 'max'").show()
  else
    printf("There is no column: %s!", name)
}

// COMMAND ----------

maximum("SalesOrderID", table)

// COMMAND ----------

def mean(name: String, df: DataFrame) = {
  if( df.columns.contains(name) )
    df.describe(name).filter("summary = 'mean'").show()
  else
    printf("There is no column: %s!", name)
}

// COMMAND ----------

mean("SalesOrderID", table)
