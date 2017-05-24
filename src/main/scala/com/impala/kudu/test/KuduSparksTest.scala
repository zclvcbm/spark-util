package com.impala.kudu.test

import org.kududb.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.conf.Configuration
import org.apache.kudu.client.RowResult
import org.apache.spark.sql.SQLContext
import java.io.Serializable
import org.apache.kudu.mapreduce.SparkKuduTableInputFormat

object KuduSparksTest {
  System.setProperty("hadoop.home.dir", "F:\\eclipse\\hdplocal2.6.0")
  val kuduMaster = "kylin-master2"

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Test"))
  val sparksql = new SQLContext(sc)
  import sparksql.implicits._
  val a = new KuduContext(kuduMaster)
  def main(args: Array[String]): Unit = {
    newKuduRDD

  }
  def writetoKudu() {
    val tableName = "impala::default.student"
    val rdd = sc.parallelize(Array("k", "b", "a")).map { n => STU(n.hashCode, n) }
    val data = rdd.toDF()
    a.writeRows(data, tableName, false)

  }
  def getKuduRDD() {
    val tableName = "impala::default.kudu_pc_log"
    val columnProjection = Seq("siteid", "uid")
    val df = a.kuduRDD(sc, tableName, columnProjection)
    df.foreach { x => println(x.mkString(",")) }

  }
  def newKuduRDD() {
    val tableName = "impala::default.kudu_pc_log"
    val conf = new Configuration
    conf.set("kudu.mapreduce.master.address", kuduMaster)
    conf.set("kudu.mapreduce.input.table", tableName)
    conf.set("kudu.mapreduce.column.projection","siteid,uid")
    val rdd = sc.newAPIHadoopRDD(conf, classOf[SparkKuduTableInputFormat], classOf[NullWritable], classOf[RowResult])
    rdd.foreach(println)
  }
  case class STU(id: Int, name: String)
  case class PCLOG(deliverytime: String, siteid: String, plan: String, activity: String, uid: String) extends Serializable
}