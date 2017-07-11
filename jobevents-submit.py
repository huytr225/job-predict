import pandas as pd
import os
from pyspark.sql.session import SparkSession as spark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from matplotlib import pyplot

sc = SparkContext(appName="jobevents")
sql_context = SQLContext(sc)

schema = StructType([StructField('timestamp', LongType(), True),
                     StructField('missinginfo', IntegerType(), True),
                     StructField('jobID', LongType(), True),
                     StructField('eventtype', IntegerType(), True),
                     StructField('username', StringType(), True),
                     StructField('schedulingclass', IntegerType(), True),
                     StructField('jobname', StringType(), True),
                     StructField('logicaljobname', StringType(), True)])
jobEventsDF = (
    sql_context.read
    .format('com.databricks.spark.csv')
    .schema(schema)
    .load("../../clusterdata-2011-2/job_events/out.csv")
)
jobEventsDF.createOrReplaceTempView("dataFrame")
df = sql_context.sql("select round((timestamp/1000000)/3600) as timestamp, count(jobID) as numberOfJob from dataFrame where eventtype = 0  group by round((timestamp/1000000)/3600) order by timestamp")
df.toPandas().to_csv('results/jobevents-hours-submit.csv', index=False, header=None)
df = sql_context.sql("select round((timestamp/1000000)/86400) as timestamp, count(jobID) as numberOfJob from dataFrame where eventtype = 0  group by round((timestamp/1000000)/86400) order by timestamp")
df.toPandas().to_csv('results/jobevents-days-submit.csv', index=False, header=None)
sc.stop()
