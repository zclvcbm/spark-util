package org.apache.spark.streaming.kafka

import kafka.message.MessageAndMetadata
import org.apache.spark.rdd.RDD
import kafka.common.TopicAndPartition

trait SparkKafkaManagerBase extends KafkaSparkTool{
   /**
   * @author LMQ
   * @description  默认的一个handle (key,value)=>(topic,msg)
   */
  def msgHandle = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message)
    /**
   * @author LMQ
   * @description 获取RDD的offset。但这个rdd必须继承HasOffsetRanges 的
   */
  def getRDDConsumerOffsets[T](rdd: RDD[T]) = {
    var consumoffsets = Map[TopicAndPartition, Long]()
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      consumoffsets += ((topicAndPartition, offsets.untilOffset))
    }
    consumoffsets
  }
  /**
   * @author LMQ
   * @description 将rdd的offset更新至zookeeper
   */
  def updateRDDOffset[T](kp: Map[String, String], groupId: String, rdd: RDD[T]) {
    val offsets = getRDDConsumerOffsets(rdd)
    updateConsumerOffsets(kp, groupId, offsets)
  }

}