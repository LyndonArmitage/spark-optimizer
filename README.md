# Spark Optimizer

This project contains some toy code and ideas for optimizing Apache Spark Jobs
as well as some useful extensions.

## RecordCountListener & ExternalCatalogHelper

This is a simple `SparkListener` that, when attached to a job with a single
output, will expose the metrics on the amount of rows written, bytes written,
and execution time.

This data can be passed to the `ExternalCatalogHelper` class to populate
whatever catalog you have connected to Spark with such metrics without
performing additional analysis steps.

## TableAnalyzer

`TableAnalyzer` is an object that essentially exposes the SQL `ANALYSE` command
as Scala methods that can be used more easily in ELT and ETL jobs written in
Scala.

## ExtraColumnFunctions

`ExtraColumnFunctions` are useful column functions currently only including
`randLong` functions for generating random long integers within a range.

## ExtraDataFrameFunctions

`ExtraDataFrameFunctions` are useful dataframe functions.

Currently, the only method implemented is one that densifies a table based on a
sparse date column, i.e. adds rows for missing dates in a table. Example usage:

```scala
val df = Seq(
  (Date.valueOf("2020-01-15"), 0),
  (Date.valueOf("2020-01-16"), 1),
  (Date.valueOf("2020-01-17"), 1),
  // densify will add 3 rows here
  (Date.valueOf("2020-01-21"), 2),
  // densify will 10 rows here
  (Date.valueOf("2020-02-01"), 5)
).toDF("date", "val")

val dense = df.densify_on_date("date")

val count = dense.count()
assert(count == 18, "Wrong count of entries")
```

This actually makes use of a custom Catalyst function defined in
`LyndonFunctions` briefly described in this
[blog post](https://lyndon.codes/2021/02/18/spark-native-functions/).

## PartitionCalculator

`PartitionCalculator` is a class dedicated to trying to calculate the correct
sizes for your Spark partitions.

It includes a simple, numerical, based recommender and a more complex
recommender that can sample an existing `DataFrame`, estimate its size, and
then recommend a partition size.