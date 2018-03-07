package org.apache.spark.core

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration
import scala.reflect.ClassTag
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka.StreamingKafkaManager
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.common.util.Configuration
import org.apache.spark.streaming.kafka.SparkContextKafkaManager
import org.apache.spark.rdd.RDD
import org.apache.spark.common.util.KafkaConfig

class StreamingKafkaContext {
  var streamingContext: StreamingContext = null
  var sc: SparkContext = null

  def this(streamingContext: StreamingContext) {
    this()
    this.streamingContext = streamingContext
    this.sc = streamingContext.sparkContext
  }
  def this(sc: SparkContext, batchDuration: Duration) {
    this()
    this.sc = sc
    streamingContext = new StreamingContext(sc, batchDuration)
  }
  def start() {
    streamingContext.start()
  }
  def awaitTermination() {
    streamingContext.awaitTermination
  }
  //将当前的topic的groupid更新至最新的offsets
  def updataOffsetToLastest(topics: Set[String], kp: Map[String, String]) = {
    val lastestOffsets = SparkContextKafkaManager.getLatestOffsets(topics, kp)
    SparkContextKafkaManager.updateConsumerOffsets(kp, lastestOffsets)
    lastestOffsets
  }
  def getLastOffset(topics: Set[String], kp: Map[String, String])={
    SparkContextKafkaManager.getLatestOffsets(topics, kp)
  }
  /**
   * 更新rdd的offset
   */
  def updateRDDOffsets[T](
    kp: Map[String, String],
    groupId: String,
    rdd: RDD[T]) {
    SparkContextKafkaManager.updateRDDOffset(kp, groupId, rdd)
  }
  
  def getRDDOffsets[T](rdd: RDD[T]) = {
    StreamingKafkaManager.getRDDConsumerOffsets(rdd)
  }

  def createDirectStream[R: ClassTag](
    kp: Map[String, String],
    topics: Set[String],
    fromOffset: Map[TopicAndPartition, Long],
    msgHandle: (MessageAndMetadata[String, String]) => R): InputDStream[R] = {
    StreamingKafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder, R](streamingContext, kp, topics, fromOffset, msgHandle)
  }
  def createDirectStream[R: ClassTag](
    kp: Map[String, String],
    topics: Set[String],
    msgHandle: (MessageAndMetadata[String, String]) => R): InputDStream[R] = {
    StreamingKafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder, R](streamingContext, kp, topics, null, msgHandle)
  }
  def createDirectStream[R: ClassTag](
    conf: KafkaConfig,
    fromOffset: Map[TopicAndPartition, Long],
    msgHandle: (MessageAndMetadata[String, String]) => R): InputDStream[R] = {
    StreamingKafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder, R](streamingContext, conf, fromOffset, msgHandle)
  }
  def createDirectStream[R: ClassTag](
    conf: KafkaConfig,
    msgHandle: (MessageAndMetadata[String, String]) => R): InputDStream[R] = {
    StreamingKafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder, R](streamingContext, conf, null, msgHandle)
  }
}
object StreamingKafkaContext extends SparkKafkaConfsKey {
/**
   * 更新rdd的offset
   */
  def updateRDDOffsets[T](
    kp: Map[String, String],
    rdd: RDD[T]) {
    if (kp.contains("group.id")) {
      val groupid = kp.get("group.id").get
      SparkContextKafkaManager.updateRDDOffset(kp, groupid, rdd)
    }else println("No Group Id To UpdateRDDOffsets")
  }
}