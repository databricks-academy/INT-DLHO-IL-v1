# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
)
from pyspark.sql.session import SparkSession
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.window import Window

# COMMAND ----------


def create_stream_writer(
    dataframe: DataFrame,
    checkpoint: str,
    name: str,
    partition_column: str = None,
    mode: str = "append",
) -> DataStreamWriter:

    stream_writer = (
        dataframe.writeStream.format("delta")
        .outputMode(mode)
        .option("checkpointLocation", checkpoint)
        .queryName(name)
    )
    if partition_column is not None:
        return stream_writer.partitionBy(partition_column)
    return stream_writer


# COMMAND ----------


def read_stream_delta(spark: SparkSession, deltaPath: str) -> DataFrame:
    return spark.readStream.format("delta").load(deltaPath)


# COMMAND ----------


def read_stream_raw(spark: SparkSession, rawPath: str) -> DataFrame:
    kafka_schema = "value STRING"
    return spark.readStream.format("text").schema(kafka_schema).load(rawPath)


# COMMAND ----------


def update_silver_table(spark: SparkSession, silverPath: str) -> bool:

    update_match = """
    health_tracker.eventtime = updates.eventtime
    AND
    health_tracker.device_id = updates.device_id
  """

    update = {"heartrate": "updates.heartrate"}

    dateWindow = Window.orderBy("p_eventdate")

    interpolatedDF = spark.read.table("health_tracker_plus_silver").select(
        "*",
        lag(col("heartrate")).over(dateWindow).alias("prev_amt"),
        lead(col("heartrate")).over(dateWindow).alias("next_amt"),
    )

    updatesDF = interpolatedDF.where(col("heartrate") < 0).select(
        "device_id",
        ((col("prev_amt") + col("next_amt")) / 2).alias("heartrate"),
        "eventtime",
        "name",
        "p_eventdate",
    )

    silverTable = DeltaTable.forPath(spark, silverPath)

    (
        silverTable.alias("health_tracker")
        .merge(updatesDF.alias("updates"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True


# COMMAND ----------


def transform_bronze(bronze: DataFrame) -> DataFrame:

    json_schema = "device_id INTEGER, heartrate DOUBLE, name STRING, time FLOAT"

    return (
        bronze.select(from_json(col("value"), json_schema).alias("nested_json"))
        .select("nested_json.*")
        .select(
            "device_id",
            "heartrate",
            from_unixtime("time").cast("timestamp").alias("eventtime"),
            "name",
            from_unixtime("time").cast("date").alias("p_eventdate"),
        )
    )


# COMMAND ----------


def transform_raw(df: DataFrame) -> DataFrame:
    return df.select(
        lit("files.training.databricks.com").alias("datasource"),
        current_timestamp().alias("ingesttime"),
        "value",
        current_timestamp().cast("date").alias("p_ingestdate"),
    )


# COMMAND ----------


def transform_silver_mean_agg(silver: DataFrame) -> DataFrame:
    return silver.groupBy("device_id").agg(
        mean(col("heartrate")).alias("mean_heartrate"),
        stddev(col("heartrate")).alias("std_heartrate"),
        max(col("heartrate")).alias("max_heartrate"),
    )


# COMMAND ----------


def transform_silver_mean_agg_last_thirty(silver: DataFrame) -> DataFrame:
    health_tracker_gold_aggregate_heartrate = spark.read.table(
        "health_tracker_gold_aggregate_heartrate"
    )
    return silver.join(
        spark.read.table("health_tracker_gold_aggregate_heartrate"), "device_id"
    ).where("p_eventdate > cast('2020-03-01' AS DATE) - 30")
