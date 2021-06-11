// Databricks notebook source
// MAGIC %scala
// MAGIC //EXERCIE 1
// MAGIC 
// MAGIC val dept = Seq(
// MAGIC    ("50000.0#0#0#", "#"),
// MAGIC    ("0@1000.0@", "@"),
// MAGIC    ("1$", "$"),
// MAGIC    ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")
// MAGIC 
// MAGIC dept.createOrReplaceTempView("dept")
// MAGIC 
// MAGIC val dept1 = spark.sql("select VALUES,Delimiter, " +
// MAGIC   "CASE\n    WHEN delimiter =='$' THEN split(VALUES,'\\\\$')  " +
// MAGIC   "  WHEN delimiter == '^' THEN split(VALUES,'\\\\^')" +
// MAGIC   "  ELSE split(VALUES,delimiter)\n  END AS split_values from dept")

// COMMAND ----------

display(dept1)

// COMMAND ----------



// COMMAND ----------

//EXERCIE 2

val input = Seq(
  (1, "MV1"),
  (1, "MV2"),
  (2, "VPV"),
  (2, "Others")).toDF("id", "value")



// COMMAND ----------

input.createOrReplaceTempView("table")
spark.sql("select id, value from (select id,value, row_number() over (partition by id order by id) rn from table) t where rn=1")


// COMMAND ----------

display(res5)

// COMMAND ----------

res1 = input.dropDuplicates(Seq("id"))

// COMMAND ----------

display(res1)

// COMMAND ----------

//EXERCICE 3

val input = Seq(
  ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
  ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
  ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
  ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
  ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
  ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
  ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
  ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
  ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
  ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
  ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
  ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
  ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
  ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
  ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
  ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
  ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
  ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
  ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
  ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2)).toDF("column0", "column1", "column2", "label")

// COMMAND ----------

display(input)

// COMMAND ----------

val res1 = input.select("column0","column1", "column2","label").count()

// COMMAND ----------

val res24 = input.groupBy("column0","column1", "column2","label").

// COMMAND ----------

display(res24)

// COMMAND ----------

val res11 = input.groupBy("column0","column1", "column2","label").sum()

// COMMAND ----------

display(res11)

// COMMAND ----------

val res12 = input.groupBy("column0","column1", "column2","label").count()

// COMMAND ----------

display(res12)

// COMMAND ----------

//EXERCIE 4

val input = spark.range(50).withColumn("key", $"id" % 5)
val limitUDF = udf{(nums: Seq[Long], limit: Int) => nums.take(limit)}

val df1 = input.groupBy("key").agg(collect_set("id") as "all")
display(df1)

// COMMAND ----------

val df2 = df1.withColumn("only_first_three", limitUDF($"all", lit(3)))

display(df2)
