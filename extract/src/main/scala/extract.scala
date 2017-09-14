// scalastyle:off println

import scala.math.random

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

object extract {
  class IdleTime extends UserDefinedAggregateFunction {
    override def inputSchema: StructType =
      StructType(
        StructField("start", LongType) ::
        StructField("end", LongType) :: Nil
      )
  
    override def bufferSchema: StructType = StructType(
      StructField("start", LongType) ::
      StructField("end", LongType) ::
      StructField("exe", LongType) :: Nil
    )

    override def dataType: DataType = LongType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
      buffer(2) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val startC = buffer.getAs[Long](0)
      val endC   = buffer.getAs[Long](1)
      val exe    = buffer.getAs[Long](2)
      val startI = input.getAs[Long](0)
      val endI   = input.getAs[Long](1)
      if(endC <= startI){
	buffer(2) = exe + (endC - startC) 
        buffer(0) = startI
        buffer(1) = endI
      } else {
	buffer(1) = endI 
      }
      
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val start1 = buffer1.getAs[Long](0)
      val end1   = buffer1.getAs[Long](1)
      val exe1   = buffer1.getAs[Long](2)
      val start2 = buffer2.getAs[Long](0)
      val end2   = buffer2.getAs[Long](1)
      val exe2   = buffer2.getAs[Long](2)
      buffer1(2) = exe1 + exe2
    }
  
    override def evaluate(buffer: Row): Any = {
      buffer.getAs[Long](2)
    }
  }
  
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("extract")
      .getOrCreate()

    val jobSchema = new StructType(Array(
                         new StructField("timestamp", LongType, true),
                         new StructField("missingInfo", IntegerType, true),
                         new StructField("jobID", LongType, true),
                         new StructField("eventType", IntegerType, true),
                         new StructField("username", StringType, true),
                         new StructField("schedulingClass", IntegerType, true),
                         new StructField("jobName", StringType, true),
                         new StructField("logicalJobName", StringType, true)))

    val taskSchema = new StructType(Array(
                         new StructField("startTime", LongType, true),
                         new StructField("endTime", LongType, true),
                         new StructField("jobID", LongType, true),
                         new StructField("taskIndex", LongType, true),
                         new StructField("machineID", LongType, true),
                         new StructField("meanCPUUsage", FloatType, true),
                         new StructField("CMU", FloatType, true),
                         new StructField("assignMem", FloatType, true),
                         new StructField("unmapped_cache_usage", FloatType, true),
                         new StructField("page_cache_usage", FloatType, true),
                         new StructField("max_mem_usage", FloatType, true),
                         new StructField("mean_diskIO_time", FloatType, true),
                         new StructField("mean_local_disk_space", FloatType, true),
                         new StructField("max_cpu_usage", FloatType, true),
                         new StructField("max_disk_io_time", FloatType, true),
                         new StructField("cpi", FloatType, true),
                         new StructField("mai", FloatType, true),
                         new StructField("sampling_portion", FloatType, true),
                         new StructField("agg_type", FloatType, true),
                         new StructField("sampled_cpu_usage", FloatType, true)))

    val jobEvents = spark.read.format("csv")
        .schema(jobSchema)
        .load("/mnt/volume/ggcluster/clusterdata-2011-2/job_events/out.csv")

    val taskUsage = spark.read.format("csv")
        .schema(taskSchema)
        .load("/mnt/volume/ggcluster/clusterdata-2011-2/task_usage/part-00000-of-00500.csv")

    jobEvents.createOrReplaceTempView("job_events")
    taskUsage.createOrReplaceTempView("task_usage")

    spark.udf.register("idle", new IdleTime)
    val sqlWay = spark.sql("""
        select min(timestamp)/1000000 as ts, floor((max(endTime)-min(startTime))/1000000), floor(idle(startTime,endTime)/1000000), t.jobID
        from job_events j, task_usage t
        where t.jobID = j.jobID and eventType = 0
        group by t.jobID
        order by ts
    """)
    val extract = spark.sql("""
        select j.jobID, min(startTime) - max(endTime) + idle(startTime, endTime) 
        from  task_usage t, job_events j
        where t.jobID = j.jobID and eventType = 0
	group by j.jobID
    """)
//    sqlWay.toPandas().to_csv("/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/huy/extract/results")
    extract.coalesce(1).write.format("com.databricks.spark.csv").save("/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/huy/extract/results/out.csv")
    spark.stop()
  }
}
// scalastyle:on println
