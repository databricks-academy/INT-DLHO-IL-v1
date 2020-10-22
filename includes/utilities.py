# Databricks notebook source
from pyspark.sql.session import SparkSession
from urllib.request import urlretrieve
import time

BASE_URL = "https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/"


def retrieve_data(year: int, month: int, raw_path: str, is_late: bool = False) -> bool:
    file, dbfsPath, driverPath = _generate_file_handles(year, month, raw_path, is_late)
    uri = BASE_URL + file

    urlretrieve(uri, file)
    dbutils.fs.mv(driverPath, dbfsPath)
    return True


def _generate_file_handles(year: int, month: int, raw_path: str, is_late: bool):
    late = ""
    if is_late:
        late = "_late"
    file = f"health_tracker_data_{year}_{month}{late}.json"

    dbfsPath = raw_path
    if is_late:
        dbfsPath += "late/"
    dbfsPath += file

    driverPath = "file:/databricks/driver/" + file

    return file, dbfsPath, driverPath


def stop_all_streams() -> bool:
    stopped = False
    for stream in spark.streams.active:
        stopped = True
        stream.stop()
    return stopped


def stop_named_stream(spark: SparkSession, namedStream: str) -> bool:
    stopped = False
    for stream in spark.streams.active:
        if stream.name == namedStream:
            stopped = True
            stream.stop()
    return stopped


def untilStreamIsReady(namedStream: str, progressions: int = 3) -> bool:
    queries = list(filter(lambda query: query.name == namedStream, spark.streams.active))
    while len(queries) == 0 or len(queries[0].recentProgress) < progressions:
        time.sleep(5)
        queries = list(filter(lambda query: query.name == namedStream, spark.streams.active))
    print("The stream {} is active and ready.".format(namedStream))
    return True
