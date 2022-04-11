// Databricks notebook source
val transactions = Seq(( 1, "2011-01-01", 500),
( 1, "2011-01-15", 50),
( 1, "2011-01-22", 250),
( 1, "2011-01-24", 75),
( 1, "2011-01-26", 125),
( 1, "2011-01-28", 175),
( 2, "2011-01-01", 500),
( 2, "2011-01-15", 50),
( 2, "2011-01-22", 25),
( 2, "2011-01-23", 125),
( 2, "2011-01-26", 200),
( 2, "2011-01-29", 250),
( 3, "2011-01-01", 500),
( 3, "2011-01-15", 50 ),
( 3, "2011-01-22", 5000),
( 3, "2011-01-25", 550),
( 3, "2011-01-27", 95 ),
( 3, "2011-01-30", 2500)).toDF("AccountId", "TranDate", "TranAmt")


val logical = Seq((1,"George", 800),
(2,"Sam", 950),
(3,"Diane", 1100),
(4,"Nicholas", 1250),
(5,"Samuel", 1250),
(6,"Patricia", 1300),
(7,"Brian", 1500),
(8,"Thomas", 1600),
(9,"Fran", 2450),
(10,"Debbie", 2850),
(11,"Mark", 2975),
(12,"James", 3000),
(13,"Cynthia", 3000),
(14,"Christopher", 5000)).toDF("RowID" ,"FName" , "Salary" )


// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val window  = Window.partitionBy(col("AccountId")).orderBy("TranDate")
val run_total_transactions=sum("TranAmt").over(window)

transactions.select(col("AccountId"), col("TranDate"), col("TranAmt") ,run_total.alias("RunTotalAmt") ).show()


// COMMAND ----------

val window  = Window.partitionBy(col("AccountId")).orderBy("TranDate")

val t_avg=avg("TranAmt").over(window)
val t_count=count("*").over(window)
val t_min=min("TranAmt").over(window)
val t_max=max("TranAmt").over(window)
val t_sum=sum("TranAmt").over(window)

val df=transactions.select(col("AccountId"), col("TranDate"), col("TranAmt") ,t_avg.alias("RunAvg") ,t_count.alias("RunTranQty") ,t_min.alias("RunSmallAmt") , t_max.alias("RunLargeAmt") , t_sum.alias("RunTotalAmt") )
display(df)

// COMMAND ----------

val windowBase = Window.partitionBy(col("AccountId")).orderBy("TranDate")
val window = windowBase.rowsBetween(-2, Window.currentRow) 

val t_avg=avg("TranAmt").over(window)
val t_count=count("*").over(window)
val t_min=min("TranAmt").over(window)
val t_max=max("TranAmt").over(window)
val t_sum=sum("TranAmt").over(window)
val row_nr= row_number().over(windowBase)

val df=transactions.select(col("AccountId"), col("TranDate"), col("TranAmt") ,t_avg.alias("SlideAvg") ,t_count.alias("SlideQty") ,t_min.alias("SlideMin") , t_max.alias("SlideMax") , t_sum.alias("SlideTotal") , row_nr. alias("RN") ).orderBy("AccountId", "TranDate", "RN")
display(df)

// COMMAND ----------

val window  = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow) 
val window2  = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow) 

val sum_rows= sum(col("Salary")).over(window)
val sum_range= sum(col("Salary")).over(window2)

val df=logical.select(col("RowID"), col("FName"), col("Salary") ,sum_rows.alias("SumByRows"),sum_range.alias("SumByRange") ).orderBy("RowID")
display(df)

// COMMAND ----------

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val sales_header = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM SalesLT.SalesOrderHeader")
  .load()

display(sales_header)

// COMMAND ----------

val sales_detail = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM SalesLT.SalesOrderDetail")
  .load()

display(sales_detail)

// COMMAND ----------

val window  = Window.partitionBy("AccountNumber").orderBy("OrderDate")

val row_nb= row_number().over(window)
val df=sales_header.select(col("AccountNumber"), col("OrderDate"), col("TotalDue") ,row_nb.alias("RN") ).orderBy("AccountNumber").limit(10)
display(df)

// COMMAND ----------

val window =Window.partitionBy(col("AccountId")).orderBy("TranDate")
val windowRows=window.rowsBetween(Window.unboundedPreceding, -2) 

val t_lead=lead(col("TranAmt"), 2).over(window)
val t_first=first(col("TranAmt")).over(windowRows)
val t_last=last(col("TranAmt")).over(windowRows)
val row_nb=row_number().over(window)
val t_rank=dense_rank().over(window)

val df=transactions.select(col("*"), t_lead,t_first, t_last, row_nb, t_rank).orderBy("AccountId", "TranDate")
display(df)

// COMMAND ----------

val windowSpec =Window.partitionBy(col("AccountId")).orderBy("TranAmt")
val windowSpecRange=windowSpec.rangeBetween(-100 ,Window.currentRow) 

val tr_lead=lead(col("TranAmt"), 2).over(windowSpec)
val tr_first=first(col("TranAmt")).over(windowSpecRange)
val tr_last=last(col("TranAmt")).over(windowSpecRange)
val row_nb=row_number().over(windowSpec)
val tr_rank=dense_rank().over(windowSpec)

val df=transactions.select(col("*"),tr_lead, tr_first, tr_last, row_nb, tr_rank).orderBy("AccountId", "TranAmt")
display(df)

// COMMAND ----------

val LeftSemiJoin = df.join(df2, df("ProductCategoryID") === df2("ProductCategoryID"), "leftsemi")
LeftSemiJoin.explain()

// COMMAND ----------

val LeftAntiJoin = df.join(df2, df("ProductCategoryID") === df2("ProductCategoryID"), "leftanti")
LeftAntiJoin.explain()

// COMMAND ----------



// COMMAND ----------

val joinExpression= sales_header.col("SalesOrderID") === sales_detail.col("SalesOrderID") 
val result =sales_header.join(sales_detail, joinExpression).drop(sales_header.col("SalesOrderID")).select("SalesOrderID")
result.show()

// COMMAND ----------

val sales_detail2=sales_detail.withColumnRenamed("SalesOrderID", "order_id")
val joinExpression= sales_header.col("SalesOrderID") === sales_detail2.col("order_id")
val result =sales_header.join(sales_detail2, joinExpression).select("SalesOrderID", "order_id")
display(result)

// COMMAND ----------

import org.apache.spark.sql.functions.broadcast
val res = sales_header.join(broadcast(sales_detail), sales_header("ProductCategoryID") === sales_detail("ProductCategoryID"))

res.explain()
