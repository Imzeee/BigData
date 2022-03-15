// Databricks notebook source
// MAGIC %md
// MAGIC Użyj każdą z tych funkcji 
// MAGIC * `unix_timestamp()` 
// MAGIC * `date_format()` - formatuje datę do stringa
// MAGIC * `to_unix_timestamp()`
// MAGIC * `from_unixtime()`
// MAGIC * `to_date()` 
// MAGIC * `to_timestamp()` 
// MAGIC * `from_utc_timestamp()` 
// MAGIC * `to_utc_timestamp()`

// COMMAND ----------

import org.apache.spark.sql.functions._

val kolumny = Seq("timestamp","unix", "Date")
val dane = Seq(("2015-03-22T14:13:34", 1646641525847L,"May, 2021"),
               ("2015-03-22T15:03:18", 1646641557555L,"Mar, 2021"),
               ("2015-03-22T14:38:39", 1646641578622L,"Jan, 2021"))

var dataFrame = spark.createDataFrame(dane).toDF(kolumny:_*)
  .withColumn("current_date",current_date().as("current_date"))
  .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
display(dataFrame)

// COMMAND ----------


dataFrame.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ## unix_timestamp(..) & cast(..)

// COMMAND ----------

// MAGIC %md
// MAGIC Konwersja **string** to a **timestamp**.
// MAGIC 
// MAGIC Lokalizacja funkcji 
// MAGIC * `pyspark.sql.functions` in the case of Python
// MAGIC * `org.apache.spark.sql.functions` in the case of Scala & Java

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Zmiana formatu wartości timestamp yyyy-MM-dd'T'HH:mm:ss 
// MAGIC `unix_timestamp(..)`
// MAGIC 
// MAGIC Dokumentacja API `unix_timestamp(..)`:
// MAGIC > Convert time string with given pattern (see <a href="http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html" target="_blank">SimpleDateFormat</a>) to Unix time stamp (in seconds), return null if fail.
// MAGIC 
// MAGIC `SimpleDataFormat` is part of the Java API and provides support for parsing and formatting date and time values.

// COMMAND ----------

val nowyunix = dataFrame.select($"timestamp",unix_timestamp($"timestamp","yyyy-MM-dd'T'HH:mm:ss").cast("timestamp")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC 2. Zmień format zgodnie z klasą `SimpleDateFormat`**yyyy-MM-dd HH:mm:ss**
// MAGIC   * a. Wyświetl schemat i dane żeby sprawdzicz czy wartości się zmieniły

// COMMAND ----------


val zmianaFormatu = dataFrame
  .withColumnRenamed("timestamp", "xxxx")
  .select( $"*", unix_timestamp($"xxxxx", "yyyy-MM-dd HH:mm:ss") )

zmianaFormatu.printSchema()

// COMMAND ----------


val tempE = dataFrame
  .withColumnRenamed("timestamp", "xxxx")
  .select( $"*", unix_timestamp($"xxxx", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
  .withColumnRenamed("CAST(unix_timestamp(capturedAt, yyyy-MM-dd'T'HH:mm:ss) AS TIMESTAMP)", "xxxcast")


// COMMAND ----------

// MAGIC %md
// MAGIC ## Stwórz nowe kolumny do DataFrame z wartościami year(..), month(..), dayofyear(..)

// COMMAND ----------

val dataFrame2=dataFrame.withColumn("year",year(current_timestamp()))
.withColumn("month", month(current_timestamp()))
.withColumn("day of year", dayofyear(current_timestamp()))
dataFrame2.show()

// COMMAND ----------

Seq(1).toDF("seq").select(
    current_date().as("current_date"),
    date_format(current_timestamp(),"yyyy MM dd").as("yyyy MM dd"),
    date_format(current_timestamp(),"MM/dd/yyyy hh:mm").as("MM/dd/yyyy"),
    date_format(current_timestamp(),"yyyy MMM dd").as("yyyy MMMM dd"),
    date_format(current_timestamp(),"yyyy MMMM dd E").as("yyyy MMMM dd E")
  ).show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss');

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT to_unix_timestamp('2000-09-23', 'yyyy-MM-dd');

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT to_date('2000-09-23', 'yyyy-MM-dd');

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT to_timestamp('2000-09-23 00:12:00');

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT from_utc_timestamp('2000-09-23 02:40:00.0', 'GMT+1');

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT to_utc_timestamp( '2000-09-23 02:40:00.0', 'GMT+1');