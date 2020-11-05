# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Upsert Into a Delta Table
# MAGIC Repair Records with an Upsert
# MAGIC In the previous lesson, we identified two issues with the health_tracker_processed table:
# MAGIC - There were 95 missing records
# MAGIC - There were 67 records with broken readings
# MAGIC In this lesson, we will repair the table by modifying the health_tracker_processed table.

# COMMAND ----------

# MAGIC %md ## Notebook Configuration
# MAGIC
# MAGIC Before you run this cell, make sure to add a unique user name to the file
# MAGIC `includes/configuration`, e.g.
# MAGIC
# MAGIC ```
# MAGIC username = "yourfirstname_yourlastname"
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC There are two patterns for modifying existing Delta tables.
# MAGIC appending files to an existing directory of Delta files
# MAGIC merging a set of updates and insertions
# MAGIC In this lesson, we explore the second.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The word "upsert" is a portmanteau of the words "update" and "insert," and this is what it does.
# MAGIC An upsert will update records where some criteria are met and otherwise will insert the record.
# MAGIC When upserting into an existing Delta table, use Spark SQL to perform the merge from another registered table or view.
# MAGIC The Transaction Log records the transaction, and the Metastore immediately reflects the changes.
# MAGIC The merge appends both the new/inserted files and the files containing the updates to the Delta file directory.
# MAGIC The transaction log tells the Delta reader which file to use for each record.


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Prepare Updates DataFrame
# MAGIC To repair the broken sensor readings (less than zero), we'll interpolate using the value recorded before and after for each device. The Spark SQL functions LAG and LEAD will make this a trivial calculation.
# MAGIC We'll write these values to a temporary view called updates. This view will be used later to upsert values into our health_tracker_processed Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Create a DataFrame Interpolating Broken Values
# MAGIC Recall that we want to partition on our Device ID column, which we named:
# MAGIC `"p_device_id"`

# COMMAND ----------

# TODO
# from pyspark.sql.window import Window
# from pyspark.sql.functions import col, lag, lead
#
# dteWindow = Window.partitionBy(FILL_THIS_IN).orderBy("dte")
#
# interpolatedDF = (
#    spark.read
#    .table("health_tracker_processed")
#    .select(col("dte"),
#            col("time"),
#            col("heartrate"),
#            lag(col("heartrate")).over(dteWindow).alias("prev_amt"),
#            lead(col("heartrate")).over(dteWindow).alias("next_amt"),
#            col("name"),
#            col("p_device_id"))
#  )

# COMMAND ----------

# ANSWER
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, lead

dteWindow = Window.partitionBy("p_device_id").orderBy("dte")

interpolatedDF = (
   spark.read
   .table("health_tracker_processed")
   .select(col("dte"),
           col("time"),
           col("heartrate"),
           lag(col("heartrate")).over(dteWindow).alias("prev_amt"),
           lead(col("heartrate")).over(dteWindow).alias("next_amt"),
           col("name"),
           col("p_device_id"))
 )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Create a DataFrame of Updates

# COMMAND ----------


updatesDF = (
  interpolatedDF
  .where(col("heartrate") < 0)
  .select(col("dte"),
          col("time"),
          ((col("prev_amt") + col("next_amt"))/2).alias("heartrate"),
          col("name"),
          col("p_device_id"))
)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: View the Schemas of the updatesDF and health_tracker_processed table
# MAGIC We use the .printSchema() function to view the schema of the health_tracker_processed table.
# MAGIC Which format would we read in?

# COMMAND ----------

# TODO
# (
#   spark.read
#   .format(FILL_THIS_IN)
#   .load(health_tracker + "processed")
#   .printSchema()
# )
# updatesDF.printSchema()

# COMMAND ----------

# ANSWER
(
  spark.read
  .format("delta")
  .load(health_tracker + "processed")
  .printSchema()
)
updatesDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4: Verify UpdatesDF
# MAGIC Perform a .count() on the updatesDF view. It should have the same number of records as the SUM performed on the broken_readings view.

# COMMAND ----------

updatesDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Prepare Inserts DataFrame
# MAGIC It turns out that our expectation of receiving the missing records late was correct. These records have subsequently been made available to us as the file health_tracker_data_2020_02_01.json.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1: Load the Late-Arriving Data

# COMMAND ----------

file_path = health_tracker + "raw/late/health_tracker_data_2020_2_late.json"

health_tracker_data_2020_2_late_df = (
  spark.read
  .format("json")
  .load(file_path)
)


# COMMAND ----------

# TODO
# How would we count the number of records?
# health_tracker_data_2020_2_late_df.FILL_THIS_IN()

# COMMAND ----------

# ANSWER
health_tracker_data_2020_2_late_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Transform the Data
# MAGIC In addition to updating the broken records, we wish to add this late-arriving data. We begin by preparing another temporary view with the appropriate transformations:
# MAGIC Use the from_unixtime Spark SQL function to transform the unixtime into a time string
# MAGIC Cast the time column to type timestamp to replace the column time
# MAGIC Cast the time column to type date to create the column dte

# COMMAND ----------

insertsDF = process_health_tracker_data(spark, health_tracker_data_2020_2_late_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: View the Schema of the Inserts DataFrame

# COMMAND ----------

insertsDF.printSchema()


# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Create the Union DataFrame
# MAGIC Finally, we prepare the upsertsDF that consists of all the records in both the updatesDF and the insertsDF. We use the DataFrame .union() command to create the view.

# COMMAND ----------

upsertsDF = updatesDF.union(insertsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: View the Schema

# COMMAND ----------

upsertsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Perform Upsert Into the health_tracker_processed Table
# MAGIC You can upsert data into a Delta table using the merge operation. This operation is similar to the SQL MERGE command but has added support for deletes and other conditions in updates, inserts, and deletes. In other words, using the DeltaTable command .merge() provides full support for an upsert operation.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Perform the Upsert

# COMMAND ----------

from delta.tables import DeltaTable
processedDeltaTable = DeltaTable.forPath(spark, health_tracker + "processed")

update_match = """health_tracker.time = upserts.time
                  AND
                  health_tracker.p_device_id = upserts.p_device_id"""
update = {"heartrate" : "upserts.heartrate"}

insert = {
    "p_device_id" : "upserts.p_device_id",
    "heartrate" : "upserts.heartrate",
    "name" : "upserts.name",
    "time" : "upserts.time",
    "dte" : "upserts.dte"
}

(processedDeltaTable.alias("health_tracker")
 .merge(upsertsDF.alias("upserts"), update_match)
 .whenMatchedUpdate(set=update)
 .whenNotMatchedInsert(values=insert)
 .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## View the Commit Using Time Travel

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: View the table as of Version 1
# MAGIC This is done by specifying the option "versionAsOf" as 1. When we time travel to Version 0, we see only the first month of data.
# MAGIC When we time travel to Version 1, we see the first two months of data, minus the 72 missing records.

# COMMAND ----------


(spark.read
 .option("versionAsOf", 1)
 .format("delta")
 .load(health_tracker + "processed")
 .count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Count the Most Recent Version
# MAGIC When we query the table without specifying a version, it shows the latest version of the table and includes the full two months of data.
# MAGIC Note that the range of data includes the month of February during a leap year. That is why there are 29 days in the month.

# COMMAND ----------

spark.read.table("health_tracker_processed").count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Describe the History of the health_tracker_processed Table
# MAGIC The .history() Delta Table command provides provenance information, including the operation, user, and so on, for each write to a table.
# MAGIC Note that each operation performed on the table is given a version number. These are the numbers we have been using when performing a time travel query on the table, e.g., SELECT COUNT(*) FROM health_tracker_processed VERSION AS OF 1.

# COMMAND ----------

display(processedDeltaTable.history())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Perform a Second Upsert
# MAGIC In the previous lesson, we performed an upsert to the health_tracker_processed table, simultaneously:
# MAGIC updated records containing broken readings
# MAGIC Inserting the late-arriving data
# MAGIC In doing so, we added more broken readings!

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Sum the Broken Readings
# MAGIC Let’s sum the records in the broken_readings view once more.
# MAGIC Note that there are still broken readings in the table.
# MAGIC This is because many of the records inserted as part of the upsert also contained broken readings.
# MAGIC We can perform a filter operation to isolate our `heartrate` values < 0, similar to before, with a `.where()` statement
# MAGIC as well as a `.groupby()` to group all of our records by the `"dte"` column.
# MAGIC 🧐 _NOTE_
# MAGIC It is not necessary to redefine the view broken_readings.
# MAGIC The view is simply a pointer to the query, as opposed to being the actual data,
# MAGIC and automatically pulls the latest correct number of broken readings from the data
# MAGIC in our health_tracker_processed Delta table.

# COMMAND ----------

# TODO
# from pyspark.sql.functions import col, count
#
# broken_readings = (
#   spark.read
#   .format("delta")
#   .load(health_tracker + "processed")
#   .select(col("heartrate"), col("dte"))
#   FILL_THIS_IN
#   FILL_THIS_IN
#   .agg(count("heartrate"))
#   .orderBy("dte")
# )
# broken_readings.createOrReplaceTempView("broken_readings")

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col, count

broken_readings = (
  spark.read
  .format("delta")
  .load(health_tracker + "processed")
  .select(col("heartrate"), col("dte"))
  .where(col("heartrate") < 0)
  .groupby("dte")
  .agg(count("heartrate"))
  .orderBy("dte")
)
broken_readings.createOrReplaceTempView("broken_readings")

# COMMAND ----------

%sql

SELECT SUM(`count(heartrate)`) FROM broken_readings

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Verify That These are New Broken Readings
# MAGIC Let’s query the broken_readings with a WHERE clause to verify that these are indeed new broken readings introduced by inserting the late-arriving data.
# MAGIC Note that there are no broken readings before ‘2020-02-25’.

# COMMAND ----------

%sql
SELECT SUM(`count(heartrate)`) FROM broken_readings WHERE dte < '2020-02-25'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Verify Updates
# MAGIC Perform a .count() on the updatesDF view.
# MAGIC Note that it is not necessary to redefine the DataFrame. Recall that a Spark DataFrame is lazily defined, pulling the correct number of updates when an action is triggered.
# MAGIC It should have the same number of records as the SUM performed on the broken_readings view.

# COMMAND ----------

# TODO
# updatesDF.FILL_THIS_IN()

# COMMAND ----------

# ANSWER
updatesDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4: Perform Upsert Into the health_tracker_processed Table
# MAGIC Once more, we upsert into the health_tracker_processed Table using the DeltaTable command .merge().

# COMMAND ----------

upsertsDF = updatesDF

(processedDeltaTable.alias("health_tracker")
 .merge(upsertsDF.alias("upserts"), update_match)
 .whenMatchedUpdate(set=update)
 .whenNotMatchedInsert(values=insert)
 .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 5: Sum the Broken Readings
# MAGIC Let’s sum the records in the broken_readings view one last time. Finally, there are no more broken readings in the table.

# COMMAND ----------

%sql
SELECT SUM(`count(heartrate)`) FROM broken_readings
