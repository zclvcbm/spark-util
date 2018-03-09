package com.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.core.StreamingKafkaContext
import org.apache.spark.func.tool._
import kafka.serializer.StringDecoder
object KafkaWriterTest{
   PropertyConfigurator.configure("conf/log4j.properties")
  def main(args: Array[String]): Unit = {
    runJob
  }
  def runJob() {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("Test"))
    val ssc = new StreamingKafkaContext(sc, Seconds(5))
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "test",
      "kafka.last.consum" -> "consum")
    val topics = Set("test")
    val ds = ssc.createDirectStream[
      String,String,StringDecoder,StringDecoder,(String, String)](kp, topics, msgHandle)
    ds.foreachRDD { rdd => 
      rdd.foreach(println)
      rdd.map(_._2)
         .writeToKafka(producerConfig, transformFunc(outTopic,_))
      }

    ssc.start()
    ssc.awaitTermination()
  }
}