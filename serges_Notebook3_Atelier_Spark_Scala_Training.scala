// Databricks notebook source
// MAGIC %scala
// MAGIC //EX 1 structured query that splits a column by using delimiters from another column.
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

//EX 2 structured query that selects the most important rows per assigned priority

val input = Seq(
  (1, "MV1"),
  (1, "MV2"),
  (2, "VPV"),
  (2, "Others")).toDF("id", "value")

display(input)



// COMMAND ----------

input.createOrReplaceTempView("table")
val res1 = spark.sql("select id, value from (select id,value, row_number() over (partition by id order by id) rn from table) t where rn=1")
display(res1)


// COMMAND ----------

val res2 = input.dropDuplicates(Seq("id"))

display(res2)

// COMMAND ----------

display(res1)

// COMMAND ----------

//EXE 3 structured query that adds aggregations to the input dataset.

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

val res24 = input.groupBy("column0","column1", "column2","label")

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

//EX  structured query that limits collect_set standard function.


import org.apache.spark.sql.functions._
val input = spark.range(50).withColumn("key", $"id" % 5)
val limitUDF = udf{(nums: Seq[Long], limit: Int) => nums.take(limit)}
val df1 = input.groupBy("key").agg(collect_set("id") as "all")
display(df1)

// COMMAND ----------

val df2 = df1.withColumn("only_first_three", limitUDF($"all", lit(3)))
display(df2)

// COMMAND ----------

//EX 5  structured query that “merges” two rows of the same id (to replace nulls).

val input = Seq(
  ("100","John", Some(35),None),
  ("100","John", None,Some("Georgia")),
  ("101","Mike", Some(25),None),
  ("101","Mike", None,Some("New York")),
  ("103","Mary", Some(22),None),
  ("103","Mary", None,Some("Texas")),
  ("104","Smith", Some(25),None),
  ("105","Jake", None,Some("Florida"))).toDF("id", "name", "age", "city")

display(input)

// COMMAND ----------

val df = input.groupBy("id", "name").agg(first("age", ignoreNulls = true) as "age", first("city", ignoreNulls = true) as "city").orderBy("id").take(5)
display(df)

// COMMAND ----------

//EX 6 structured query that “transpose” a dataset so a new dataset uses column names and values from a struct column.

import org.apache.spark.sql.functions._

case class MovieRatings(movieName: String, rating: Double)
case class MovieCritics(name: String, movieRatings: Seq[MovieRatings])
val movies_critics = Seq(
  MovieCritics("Manuel", Seq(MovieRatings("Logan", 1.5), MovieRatings("Zoolander", 3), MovieRatings("John Wick", 2.5))),
  MovieCritics("John", Seq(MovieRatings("Logan", 2), MovieRatings("Zoolander", 3.5), MovieRatings("John Wick", 3))))
val ratings = movies_critics.toDF

display(ratings)

// COMMAND ----------

 val df1 = ratings.withColumn("tescol", explode($"movieRatings")).select("name", "tescol")

// COMMAND ----------

 val df1 = ratings.withColumn("tescol", explode($"movieRatings")).select("name", "tescol.movieName", "tescol.rating")

// COMMAND ----------

val df2 = df1.groupBy("name").pivot("movieName").agg(expr("last(rating)")).sort(desc("name"))

display(df2)
