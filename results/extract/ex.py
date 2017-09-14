from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("extract")\
        .getOrCreate()

    folder_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/huy/results/extract/'

    extractSchema = StructType([
                         StructField('timestamp', LongType(), True),
                         StructField('startTime', LongType(), True),
                         StructField('endTime', LongType(), True),
                         StructField('jobID', LongType(), True)])


    extractConcat = spark\
      .read\
      .csv("/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/huy/results/extract/out.csv", schema = extractSchema)
    extractConcat.createOrReplaceTempView("extract")
    extract = spark.sql("""
      select timestamp, (endTime - startTime) as exTime 
      from extract
      where timestamp > 0
      order by timestamp
    """)
    extract.toPandas().to_csv('extime.csv', index=False)
    spark.stop()
