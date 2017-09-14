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
      .csv("%s*.csv"%(folder_path), schema = extractSchema)
    extractConcat.createOrReplaceTempView("extract")
    extract = spark.sql("""
      select min(timestamp) as ts, min(startTime), max(endTime), jobID
      from extract
      group by jobID
      order by ts
    """)
    extract.toPandas().to_csv('results/extract/out.csv', index=False, header=None)
    spark.stop()
