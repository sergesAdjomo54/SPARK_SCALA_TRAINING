// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Overview
// MAGIC 
// MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
// MAGIC 
// MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

// COMMAND ----------

// MAGIC %scala
// MAGIC // File location and type
// MAGIC val file_location = "/FileStore/tables/sale_data.csv"
// MAGIC var file_type = "csv"
// MAGIC 
// MAGIC // CSV options
// MAGIC val infer_schema = "true"
// MAGIC var first_row_is_header = "true"
// MAGIC var delimiter = ","
// MAGIC 
// MAGIC // The applied options are for CSV files. For other file types, these will be ignored.
// MAGIC val df = spark.read.format(file_type) 
// MAGIC   .option("inferSchema", infer_schema) 
// MAGIC   .option("header", first_row_is_header) 
// MAGIC   .option("sep", delimiter) 
// MAGIC   .load(file_location)
// MAGIC 
// MAGIC display(df)

// COMMAND ----------

// MAGIC %scala
// MAGIC // Create a view or table
// MAGIC 
// MAGIC df.createOrReplaceTempView("table")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC /* Query the created temp table in a SQL cell */
// MAGIC 
// MAGIC select * from `table`

// COMMAND ----------

// MAGIC %scala
// MAGIC // With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a //table from the DataFrame.
// MAGIC // Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
// MAGIC // To do so, choose your table name and uncomment the bottom line.
// MAGIC 
// MAGIC val permanent_table_name = "sale_data_csv"
// MAGIC 
// MAGIC // df.write.format("parquet").saveAsTable(permanent_table_name)

// COMMAND ----------

import org.apache.spark.sql.functions.{window, column, desc, col}
val res1 = df.selectExpr("CustomerId",
              "(UnitPrice * Quantity) as total_cost",
              "InvoiceDate")
  .groupBy(
          col("CustomerID"), window(col("InvoiceDate"), "1 day"))
  .sum("total_cost")
  
display(res1)

// COMMAND ----------

df.printSchema

val DFschema = df.schema

// COMMAND ----------

val streamDF = spark.readStream
    .schema(DFschema)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .option("header", true)
    .load(file_location)

// COMMAND ----------

val achatClientParHeure = streamDF
    .selectExpr("customerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
    .groupBy($"CustomerId", window($"InvoiceDate", "1 day"))
    .sum("total_cost")

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partition", "5")

// COMMAND ----------

achatClientParHeure.writeStream
     .format("memory")
    .queryName("Achat_client")
.outputMode("complete")
.start()



// COMMAND ----------

res66.status

// COMMAND ----------


