# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, from_unixtime

def process_health_tracker_data(spark: SparkSession, df: DataFrame) -> DataFrame:
  return (
    df
    .withColumn("time", from_unixtime("time"))
    .withColumnRenamed("device_id", "p_device_id")
    .withColumn("time", col("time").cast("timestamp"))
    .withColumn("dte", col("time").cast("date"))
    .withColumn("p_device_id", col("p_device_id").cast("integer"))
    .select("dte", "time", "heartrate", "name", "p_device_id")
    )
