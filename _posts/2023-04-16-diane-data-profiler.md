---
title:  "Diane - All tables - Data Profiler"
date:   2023-04-16 00:00:00 -0400
categories: oss delta-lake hive
---

[Diane](https://github.com/brayanjuls/diane) is an open-source hive helper library for apache spark users. The library currently supports parquet and delta tables and provides a set of capabilities that are:

* Get the table type(external, managed).
* Register a table to hive.
* **Describe all the tables registered in hive**
* Create a view from a delta table.

Today we will discuss in which scenario we could use the feature to describe all tables. The all `allTable` function provides metadata about the tables registered in your hive database. The attributes provided are database, tableName, provider(only delta and parquet are supported for now), owner, partitionColumns, bucketColumns, type(refers to managed or unmanaged), schema, and details(here you will find all provider-specific attributes).

 Some of the use cases we can handle using these functions are:

* Data exploration 
* Debugging
* Data profiling 

## Data Profiling

In this case, we are going leverage `allTable` function to build a simple data profiling that helps us understand the health of the tables in our database. Let’s start with a bit of context and define what data profiling is. According to [atlan](https://atlan.com/data-profiling-101/), it is the systematic process of determining and recording the characteristics of data sets. We can also think of it as building a metadata catalog that summarizes the essential characteristics.

Because we want to keep it simple, we are going to choose only 5 characteristics to apply to our dataset, the number of rows, the number of null values, the mean value, max, and min values. Before moving to implement our data profiler let’s first see what data is stored in our hive database, for that we will execute the following command.

```scala
 HiveHelper.allTable(“default”).show(truncate = false)
```

Results:

```sql
+--------+------------+--------+------------+----------------+-------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|database|tableName   |provider|owner       |partitionColumns|bucketColumns|type    |schema                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |detail                                                                                                                                                                                                                                     |
+--------+------------+--------+------------+----------------+-------------+--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|default |category    |parquet |brayan_jules|[]              |[]           |EXTERNAL|{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"description","type":"string","nullable":true,"metadata":{}}]}                                                                                                                                                                                                                                                                            |{inputFormat -> org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat, outputFormat -> org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat, serdeLibrary -> org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe}|
|default |customer    |delta   |brayan_jules|[]              |[]           |MANAGED |{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"first_name","type":"string","nullable":true,"metadata":{}},{"name":"last_name","type":"string","nullable":true,"metadata":{}},{"name":"email","type":"string","nullable":true,"metadata":{}}]}                                                                                                                                                                                                         |{tableProperties -> [delta.minReaderVersion=1,delta.minWriterVersion=2]}                                                                                                                                                                   |
|default |inventory   |delta   |brayan_jules|[]              |[]           |EXTERNAL|{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"description","type":"string","nullable":true,"metadata":{}},{"name":"quantity","type":"integer","nullable":true,"metadata":{}},{"name":"price","type":"double","nullable":true,"metadata":{}},{"name":"vendor_id","type":"integer","nullable":true,"metadata":{}},{"name":"category_id","type":"integer","nullable":true,"metadata":{}}]}|{tableProperties -> [delta.minReaderVersion=1,delta.minWriterVersion=2]}                                                                                                                                                                   |
|default |transactions|parquet |brayan_jules|[customer_id]   |[]           |MANAGED |{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"transaction_date","type":"date","nullable":true,"metadata":{}},{"name":"amount","type":"double","nullable":true,"metadata":{}},{"name":"customer_id","type":"integer","nullable":true,"metadata":{}}]}                                                                                                                                                                                                 |{inputFormat -> org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat, outputFormat -> org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat, serdeLibrary -> org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe}|
+--------+------------+--------+------------+----------------+-------------+--------+--
```

We can see that we have four tables all with flat schemas and some are managed by hive and other not, this is good because it means that our profiler will work for both cases. Now let’s build our profiler using the `allTable` function result as a datasource. TLDR, here is the full implementation of the data profiler function:

```scala 
  def generateDataProfile(tableNames:Seq[String] = Seq.empty): DataFrame = {

// Use the diane function to describe all the tables of our
// hive default database
    val database = "default"
    val allTablesDf = HiveHelpers
      .allTables(s"$database",tableNames)

// Iterate over all the rows the tables to get the table
// name and schema which is what we are going to use as
// input to our data profiler
    val dataProfileResult = allTablesDf
      .collect()
      .map(row => {
      val tableName = row.getAs[String]("tableName")
      val schema = row.getAs[String]("schema")

// parse each table schema from json to a structType to
// easily manipulate it.
      val schemaStructType = DataType
        .fromJson(schema)
        .asInstanceOf[StructType]

//Select all columns and compute some basic statistics

// Get current table columns and Dataframe
      val columns = schemaStructType
        .fields
        .map(field => col(field.name))
      val tableDF = spark
        .table(s"$database.$tableName")

// Calculate the number of null records for each column
// in the table
      val countNulls = tableDF
        .select(columns
          .map(col => count(when(col.isNull,lit(1)))
            .cast(StringType)
            .alias(col.toString)).toSeq: _*)
        .withColumn("characteristic",lit("null_count"))

// Calculate the mean for each numeric column in the table
      val mean = tableDF.select(
        columns.map(col => {
          if (schemaStructType(col.toString())
            .dataType.isInstanceOf[NumericType])
            round(avg(col), 2)
              .cast(StringType)
              .alias(col.toString())
          else
            lit("N/A").alias(col.toString())
        }).toSeq: _*)
        .withColumn("characteristic", lit("mean"))

// Calculate the min value for each column in the table
      val minValues = tableDF
        .select(columns.map(col => min(col)
          .cast(StringType)
          .alias(col.toString())).toSeq:_*)
        .withColumn("characteristic",lit("min"))

// Calculate the max value for each column in the table
      val maxValues = tableDF.select(columns.map(col =>
        max(col)
        .cast(StringType)
        .alias(col.toString())).toSeq:_*)
        .withColumn("characteristic",lit("max"))

// Get the number of rows in the table
      val countRows = tableDF.count()

// Generate a single dataframe with all calculations results.
// schema: table_name/characteristic/table_columns...
      val calculationsResultDF = countNulls
        .union(mean)
        .union(minValues)
        .union(maxValues)
        .withColumn("table_name",lit(tableName))


// Prepare the dataframe to have a single schema for all
// the tables to be processed: UnPivoting
// schema: table_name/characteristic/col_name/value
      val columnsName = columns
        .map(c => s"'$c',$c")
        .mkString(",")
      val dataProfileDF = calculationsResultDF
        .selectExpr(
          "characteristic",
          "table_name",
        s"stack(${columns.length}, $columnsName) as (col_name,value)")

// Now we can pivot the dataframe by characteristic to
// achieve the desired table structure
// schema: table_name/col_name/count/nulls_count/mean/min/max
      dataProfileDF
        .groupBy("table_name","col_name")
        .pivot("characteristic")
        .agg(first("value"))
        .withColumn("count",lit(countRows))
  })
   .reduce((df1, df2) => df1.union(df2))

    dataProfileResult
  }
```
 
Now let’s break down that implementation to better understand it. To implement this function we iterated over the result of all described tables to calculate each characteristic, each metric is calculated independently for code clarity and to make it easy to extract each calculation into separate functions that can be unit tested.

```scala

allTablesDf
      .collect()
      .map(row => {
      val tableName = row.getAs[String]("tableName")
      val schema = row.getAs[String]("schema")

// parse each table schema from json to a structType to
// easily manipulate it.
      val schemaStructType = DataType
        .fromJson(schema)
        .asInstanceOf[StructType]

//Select all columns and compute some basic statistics

// Get current table columns and Dataframe
      val columns = schemaStructType
        .fields
        .map(field => col(field.name))
      val tableDF = spark
        .table(s"$database.$tableName")

// Calculate the number of null records for each column
// in the table
      val countNulls = tableDF
        .select(columns
          .map(col => count(when(col.isNull,lit(1)))
            .cast(StringType)
            .alias(col.toString)).toSeq: _*)
        .withColumn("characteristic",lit("null_count"))

// Calculate the mean for each numeric column in the table
      val mean = tableDF.select(
        columns.map(col => {
          if (schemaStructType(col.toString())
            .dataType.isInstanceOf[NumericType])
            round(avg(col), 2)
              .cast(StringType)
              .alias(col.toString())
          else
            lit("N/A").alias(col.toString())
        }).toSeq: _*)
        .withColumn("characteristic", lit("mean"))

// Calculate the min value for each column in the table
      val minValues = tableDF
        .select(columns.map(col => min(col)
          .cast(StringType)
          .alias(col.toString())).toSeq:_*)
        .withColumn("characteristic",lit("min"))

// Calculate the max value for each column in the table
      val maxValues = tableDF.select(columns.map(col =>
        max(col)
        .cast(StringType)
        .alias(col.toString())).toSeq:_*)
        .withColumn("characteristic",lit("max"))

// Get the number of rows in the table
      val countRows = tableDF.count()

```

After the calculation we union all the metrics into a single dataframe to make it easy to manipulate it. Note we did not include the rows count metric because it is the same for all the tables so we are going to add it at the end of our transformation chain.

```scala
// Generate a single dataframe with all calculations results.
// schema: table_name/characteristic/table_columns...
      val calculationsResultDF = countNulls
        .union(mean)
        .union(minValues)
        .union(maxValues)
        .withColumn("table_name",lit(tableName))
```

At this stage we only need to organize our dataframe to achieve the desired result schema, to accomplish that we first unpivot the table to convert each column of the current table into rows(numrows = number of characteristics in this case 4) and generate a single schema(table_name,characteristic,col_name,value) for all the tables. 

```scala
// Prepare the dataframe to have a single schema for all
// the tables to be processed using unpivot technique
// schema: table_name/characteristic/col_name/value
      val columnsName = columns
        .map(c => s"'$c',$c")
        .mkString(",")
      val dataProfileDF = calculationsResultDF
        .selectExpr(
          "characteristic",
          "table_name",
        s"stack(${columns.length}, $columnsName) as (col_name,value)")
```


Secondly, we pivot on the "characteristic" column to generate one column for each "characteristic" and add our last characteristic which is the total count of rows, that way we get the desired **schema(table_name,col_name,count,nulls_count,mean,min,max)**. 

```scala
// Now we can pivot the dataframe by characteristic to
// achieve the desired table structure
// schema: table_name/col_name/count/nulls_count/mean/min/max
      dataProfileDF
        .groupBy("table_name","col_name")
        .pivot("characteristic")
        .agg(first("value"))
        .withColumn("count",lit(countRows))
```


Finally, we reduce all the dataframe in the collection using the union function from spark sql.

```scala
    reduce((df1, df2) => df1.union(df2))
```

When we run this function using the following command we get this result:

```scala 
 generateDataProfile().show(numRows = 100,truncate = false)
```

```sql
+------------+----------------+---------------------+------+---------------------+----------+-----+
|table_name  |col_name        |max                  |mean  |min                  |null_count|count|
+------------+----------------+---------------------+------+---------------------+----------+-----+
|category    |description     |kitchen              |N/A   |all food             |0         |3    |
|category    |id              |4                    |3.0   |2                    |0         |3    |
|category    |name            |kitchen              |N/A   |food                 |0         |3    |
|customer    |email           |rebeca@testdomain.com|N/A   |altuve@testdomain.com|0         |5    |
|customer    |first_name      |rebeca               |N/A   |amy                  |0         |5    |
|customer    |id              |22                   |11.2  |1                    |0         |5    |
|customer    |last_name       |pascualoto           |N/A   |Farrah Fowler        |0         |5    |
|inventory   |category_id     |2                    |2.0   |2                    |0         |4    |
|inventory   |description     |vegetable            |N/A   |eggs                 |0         |4    |
|inventory   |id              |4                    |2.5   |1                    |0         |4    |
|inventory   |name            |tuna                 |N/A   |eggs                 |0         |4    |
|inventory   |price           |5.0                  |3.58  |2.5                  |0         |4    |
|inventory   |quantity        |3999                 |1495.0|101                  |0         |4    |
|inventory   |vendor_id       |367                  |270.0 |12                   |0         |4    |
|transactions|amount          |345.5                |190.24|50.3                 |0         |5    |
|transactions|customer_id     |22                   |17.6  |14                   |0         |5    |
|transactions|id              |5                    |3.0   |1                    |0         |5    |
|transactions|transaction_date|2023-02-25           |N/A   |2023-02-25           |2         |5    |
+------------+----------------+---------------------+------+---------------------+----------+-----+

```

Using `allTables` function is a convenient way to explore the tables in your hive database or leverage it to create useful functions. In case you don't want to explore the whole database you can always select the tables for which you want to get the result by using the `tables` attribute of the function.

Reach out to [GitHub](https://github.com/brayanjuls/diane) and open an issue if you have new ideas in mind for Diane, we will be glad to discuss them.