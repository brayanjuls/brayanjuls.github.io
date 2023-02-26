---
title:  "Jodie - Append Without Duplication"
date:   2023-02-26 00:00:00 -0400
categories: oss delta-lake
---

[Jodie](https://github.com/MrPowers/jodie) is an open-source library that offers a variety of helper functions that make it easy to perform common delta lake operations using apache spark and scala. One of those functions is `appendWithoutDuplicates`. 

We often use the de-duplication technique in our pipelines to clean the data, but there are cases where we could go a little bit further by not only cleaning but preventing duplication in the first place. Better than solving the problem is not having it in the first place, in those use cases is where this function shines. 

let's see one example where jodie could be useful.

# Ingestion Data Pipeline

Suppose you need to ingest a product **stock** table from on-premise to a cloud lake house, your client tells you that one potential use case of this data is to study the stock behavior history and take better decisions in the supply chain, but for some reason, the origin table doesn’t have a column to identify when a change in the stock size happens. The source table is stored in a relational database and looks like this:


![jodie-table](/assets/images/jodie-table-1.png)
*Source table stored in a relational database* 

Because at the moment you can't implement a CDC approach to solve this issue you decide to fully ingest the table every single day, store the result in a delta table format, and perform deduplication afterward to identify the changes. Let's see how we could implement this using apache spark:

## Naive Implementation

In our first approach, we will load the data from the source(jdbc) table, add a new ingestion date column to our dataframe to be able to identify changes later, and persist to our destination as a delta table. Take a look at the following code for the implementation:

```scala
spark
    .format("jdbc")
    .options(...)
    .load()
    .withColumn("ingestionDate",current_date())
    .write
    .partitionBy("ingestionDate")
    .format("delta")
    .options(...)
    .mode(SaveMode.Append)
    .save("schema://.../stock")
```

The result of this approach is that we are storing the whole source dataset in each version of our delta table, and because of this, we can't easily recognize what changes each day. To get what changed we need to deduplicate by comparing the current day with the previous day, that might be an expensive operation if our dataset is big because we need to **load two versions of the stock table in memory** to do the comparison.

 Let's see how that looks in the code:

```scala
val currentStock = spark
                    .format("delta")
                    .options(...)
                    .load("schema://.../stock")
                    .where("ingestionDate = current_date()")

val previousStock = spark
                    .format("delta")
                    .options(...)
                    .load("schema://.../stock")
                    .where("ingestionDate = current_date()-1")

val conditionExpression = 
    currentStock("productId") === previousStock("productId") 
    and currentStock("quantity") === previousStock("quantity")

val elemmentsToAppendWithoutDuplication = 
    currentStock.alias("last")
    .join(previousStock.("prev"),conditionExpression,"left")
    .where("prev.productId is null")                                    

```

We can do better, let's see how we can efficiently avoid duplication using Jodie in the next section.


## Using Jodie Library

To use the Jodie library, we first need to set up the library in our scala project like this:

```scala
libraryDependencies += "com.github.mrpowers" %% "jodie" % "0.0.3"
```
Now we should be ready to use Jodie. As in the previous section, we first load the source table, and after that, we call the `appendWithoutDuplicates` function passing the existing delta table, the data that should be appended, and a sequence of columns name that represent a unique key as input parameters. Here is the code implementation:

```scala
val sourceDF = spark
                .format("jdbc")
                .options(...)
                .load()
                .withColumn("ingestionDate",current_date())

val stockTable = DeltaTable.forpath("schema://.../stock")            

DeltaHelpers.appendWithoutDuplicates(
        deltaTable = stockTable,
        appendData = sourceDF,
        primaryKeysColumns = Seq("productId","quantity"))
```

Using `appendWithoutDuplicates` will first deduplicate the incoming source data in memory based on the parameter **primaryKeysColumns**, after that, it will deduplicate using the existing data in the destination table, for this it only needs to load the current version of the delta table in memory to perform the deduplication. Using this function will reduce the storage in your delta table as you will be saving only the records that changed, also this function provides a simple API to prevent duplication when appending new data, and avoid the need of having to implement it yourself. 

We currently are in the process of adding an [optional functionality to this function](https://github.com/MrPowers/jodie/issues/49) that will allow us to store the data that is being filtered out, we think this could be useful in some use cases. Reach out to [GitHub](https://github.com/MrPowers/jodie) and open an issue if you have new ideas in mind for Jodie, we will be glad to discuss them.