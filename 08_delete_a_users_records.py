# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Delete all records associated with a user
# MAGIC Under the European Union General Data Protection Regulation (GDPR) and the California Consumer Privacy Act (CCPA),
# MAGIC a user of the health tracker device has the right to request that their data be expunged from the system.
# MAGIC We might simply do this by deleting all records associated with that user's device id.

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

# MAGIC %md
# MAGIC
# MAGIC #### Step 1: Delete all records for the device 4
# MAGIC Here, we use the DELETE Spark SQL command to remove all records from the health_tracker_processed
# MAGIC table that match the given predicate.

# COMMAND ----------

from delta.tables import DeltaTable

processedDeltaTable = DeltaTable.forPath(spark, health_tracker + "processed")
processedDeltaTable.delete("p_device_id = 4")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Recover the Lost Data
# MAGIC In the previous lesson, we deleted all records from the health_tracker_processed table
# MAGIC for the health tracker device with id, 4. Suppose that the user did not wish to remove all of their data,
# MAGIC but merely to have their name scrubbed from the system. In this lesson,
# MAGIC we use the Time Travel capability of Delta Lake to recover everything but the user’s name.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Prepare New upserts View
# MAGIC We prepare a view for upserting using Time Travel to recover the missing records.
# MAGIC Note that we have replaced the entire name column with the value NULL.

# COMMAND ----------

from pyspark.sql.functions import lit

upsertsDF = (
  spark.read
  .option("versionAsOf", 3)
  .format("delta")
  .load(health_tracker + "processed")
  .where("p_device_id = 4")
  .select("dte", "time", "device_type",
          "heartrate", lit(None).alias("name"), "p_device_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Perform Upsert Into the health_tracker_processed Table
# MAGIC Once more, we upsert into the health_tracker_processed Table using the DeltaTable command .merge().
# MAGIC Note that it is necessary to define 1) the reference to the Delta table and 2) the insert logic because the schema has changed.
# MAGIC Our keys will be our original column names and our values will be
# MAGIC `"upserts+columnName"`

# COMMAND ----------

# TODO
# processedDeltaTable = DeltaTable.forPath(spark, health_tracker + "processed")
#
# update_match = """health_tracker.time = upserts.time
#                   AND
#                   health_tracker.p_device_id = upserts.p_device_id"""
# update = {"heartrate" : "upserts.heartrate"}
#
# insert = {
#       "p_device_id" : "upserts.p_device_id",
#       "heartrate" : "upserts.heartrate",
#       "FILL_THIS_IN" : "upserts.name",
#       "FILL_THIS_IN" : "upserts.time",
#       "FILL_THIS_IN" : "upserts.dte",
#       "device_type" : "upserts.device_type"
# }
#
# (processedDeltaTable.alias("health_tracker")
#  .merge(upsertsDF.alias("upserts"), update_match)
#  .whenMatchedUpdate(set=update)
#  .whenNotMatchedInsert(values=insert)
#  .execute())
#

# COMMAND ----------

# ANSWER
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
      "dte" : "upserts.dte",
      "device_type" : "upserts.device_type"
}

(processedDeltaTable.alias("health_tracker")
 .merge(upsertsDF.alias("upserts"), update_match)
 .whenMatchedUpdate(set=update)
 .whenNotMatchedInsert(values=insert)
 .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Count the Most Recent Version
# MAGIC When we look at the current version, we expect to see three months of data, five device measurements, 24 hours a day for (31 + 29 + 31) days, or 10920 records. Note that the range of data includes the month of February during a leap year. That is why there are 29 days in the month.
# MAGIC Note that the range of data includes the month of February during a leap year. That is why there are 29 days in the month.

# COMMAND ----------

(
  spark.read
  .format("delta")
  .load(health_tracker + "processed")
  .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4: Query Device 4 to Demonstrate Compliance
# MAGIC We query the health_tracker_processed table to demonstrate that the name associated with device 4 has indeed been removed.

# COMMAND ----------

display(
  spark.read
  .format("delta")
  .load(health_tracker + "processed")
  .where("p_device_id = 4")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Maintaining Compliance with a Vacuum Operation
# MAGIC Unfortunately, with the power of the Delta Lake Time Travel feature, we are still out of compliance as the table could simply be queried against an earlier version to identify the name of the user associated with device 4.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Query an Earlier Table Version
# MAGIC We query the health_tracker_processed table against an earlier version to demonstrate that it is still possible to retrieve the name associated with device 4.

# COMMAND ----------

display(
  spark.read
  .option("versionAsOf", 2)
  .format("delta")
  .load(health_tracker + "processed")
  .where("p_device_id = 4")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Vacuum Table to Remove Old Files
# MAGIC The VACUUM Spark SQL command can be used to solve this problem. The VACUUM command recursively vacuums directories associated with the Delta table and removes files that are no longer in the latest state of the transaction log for that table and that are older than a retention threshold. The default threshold is 7 days.

# COMMAND ----------

from pyspark.sql.utils import IllegalArgumentException

try:
  processedDeltaTable.vacuum(0)
except IllegalArgumentException as error:
  print(error)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table Retention Period
# MAGIC When we run this command, we receive the below error. The default threshold is in place
# MAGIC to prevent corruption of the Delta table.
# MAGIC IllegalArgumentException: requirement failed: Are you sure you would like
# MAGIC to vacuum files with such a low retention period?
# MAGIC If you have writers that are currently writing to this table, there is a risk
# MAGIC that you may corrupt the state of your Delta table.
# MAGIC
# MAGIC If you are certain that there are no operations being performed on this table, such as insert/upsert/delete/optimize, then you may turn off this check by setting: spark.databricks.delta.retentionDurationCheck.enabled = false
# MAGIC
# MAGIC If you are not sure, please use a value not less than "168 hours".

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Set Delta to Allow the Operation
# MAGIC To demonstrate the VACUUM command, we set our retention period to 0 hours
# MAGIC to be able to remove the questionable files now. This is typically not a best practice
# MAGIC and in fact, there are safeguards in place to prevent this operation from being performed.
# MAGIC For demonstration purposes, we will set Delta to allow this operation.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4: Vacuum Table to Remove Old Files

# COMMAND ----------

processedDeltaTable.vacuum(0)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 5: Attempt to Query an Earlier Version
# MAGIC Now when we attempt to query an earlier version, an error is thrown.
# MAGIC This error indicates that we are not able to query data from this earlier version because the files have been expunged from the system.

# COMMAND ----------

display(
  spark.read
  .option("versionAsOf", 4)
  .format("delta")
  .load(health_tracker + "processed")
  .where("p_device_id = 4")
)
