package org.apache.spark.hbase.writer

import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

class HbaseDataWriterFunc[T](rdd:RDD[T]) {
  
  def writeToHbase(
      table:String,
      conf:Configuration,
      f:T=>(ImmutableBytesWritable, Put)){
    val hconf=createJob(table, conf)
    rdd.map( f )
       .saveAsNewAPIHadoopDataset(hconf)
  }
}