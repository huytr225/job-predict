
from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("jobevents")\
        .getOrCreate()

    jobschema = StructType([StructField('timestamp', LongType(), True),
                         StructField('missinginfo', IntegerType(), True),
                         StructField('jobID', LongType(), True),
                         StructField('eventtype', IntegerType(), True),
                         StructField('username', StringType(), True),
                         StructField('schedulingclass', IntegerType(), True),
                         StructField('jobname', StringType(), True),
                         StructField('logicaljobname', StringType(), True)])

    jobEvents = spark\
        .read\
        .csv("../../clusterdata-2011-2/job_events/out.csv", schema = jobschema)

    jobEvents.createOrReplaceTempView("job_events")
    sqlWay = spark.sql("""
    select round((timestamp/1000000)/60) as timestamp, count(jobID) as numberOfJob
    from job_events
    where eventtype = 0
    group by round((timestamp/1000000)/60)
    order by timestamp
    """)
    sqlWay.toPandas().to_csv('results/jobevents-minutes-submit.csv', index=False, header=None)
    spark.stop()
