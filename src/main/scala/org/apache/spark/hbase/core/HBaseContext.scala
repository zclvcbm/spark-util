package org.apache.spark.hbase.core

import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SerializableWritable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.hbase.mapreduce.IdentityTableMapper
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.Get
import scala.collection.JavaConversions._
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
class HBaseContext(
    @transient sc: SparkContext,
    @transient config: Configuration) extends Serializable {
  val conf = sc.broadcast(new SerializableWritable(config))
  val zookeeper=conf.value.value.get("hbase.zookeeper.quorum")
  /**
   * scan Hbase To RDD
   */
  def bulkScanRDD[T:ClassTag](
    tableName: String,
    scan: Scan,
    f: ((ImmutableBytesWritable, Result)) => T):RDD[T]= {
    var job: Job = new Job(conf.value.value)
    TableMapReduceUtil.initTableMapperJob(tableName, scan, classOf[IdentityTableMapper], null, null, job)
    sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map(f)
  }
  def bulkGetRDD[T, U](
    tableName: String,
    batchSize: Int,//get batch
    rdd: RDD[T],
    makeGet: (T) => Get,
    convertResult: (Result) => U): RDD[U] = {
    rdd.mapPartitions[U]{it =>
    val table=HbaseStorageCache.getTable(zookeeper, tableName)
    it.grouped(batchSize).flatMap { ts =>
      table.get(ts.map { makeGet(_) }.toList)
      .filter { x => x!=null && !x.isEmpty() }
    }.map { convertResult(_) }
    } (fakeClassTag[U])
  }
  def bulkGetRDD[T, U](
    tableName: String,
    rdd: RDD[T],
    makeGet: (T) => Get,
    convertResult: (Result) => U): RDD[U] = {
    bulkGetRDD(tableName, 1000, rdd, makeGet, convertResult)
  }
}