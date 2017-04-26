package org.apache.spark.func.tool

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

trait HbaseUtils {
  def createJob(table: String, conf: Configuration): Configuration = {
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    job.getConfiguration
  }
}