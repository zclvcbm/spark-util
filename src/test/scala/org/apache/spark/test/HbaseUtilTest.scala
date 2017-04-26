package org.apache.spark.test

import org.apache.spark.hbase.core.SparkHBaseContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client.Scan
import org.apache.spark.rdd.RDD

object HbaseUtilTest {
val tablename = ""
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val hc = new SparkHBaseContext(sc, sc.hadoopConfiguration)
    val rdd = sc.parallelize(Array("1"))
   
  }
  def testBulkGet(hc:SparkHBaseContext,rdd:RDD[String]){
    val r1 = hc.bulkGetRDD(tablename, rdd, makeGet, convertResult)
  }
  def testBulkScan(hc:SparkHBaseContext){
    val r2 = hc.bulkScanRDD(tablename, new Scan(), f)
  }
  

}