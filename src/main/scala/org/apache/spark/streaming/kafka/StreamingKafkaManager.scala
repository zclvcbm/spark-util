
package org.apache.spark.streaming.kafka
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkException
import kafka.message.MessageAndMetadata
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.rdd.RDD
import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import scala.collection.mutable.HashMap
import org.apache.spark.common.util.Configuration
import kafka.serializer.Decoder
import scala.reflect.ClassTag
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.common.util.KafkaConfig
/**
 * @author LMQ
 * @time 2018.03.07
 * @description 用于spark streaming 读取kafka数据
 */
private[spark] object StreamingKafkaManager
    extends SparkKafkaManagerBase{
  logname = "StreamingKafkaManager"
  /**
   * @author LMQ
   * @description 创建一个kafka的Dstream。
   * @param ssc ： 一个StreamingContext
   * @param kp : kafka的配置信息 (不知道怎么配，可以看示例)
   * @param topics ： kakfa的topic列表
   * @param fromOffset ： 如果想自己自定义从指定的offset开始读的话，传入这个值
   * @param msghandle： 读取kafka时提取的数据
   */
  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    ssc: StreamingContext,
    kp: Map[String, String],
    topics: Set[String],
    fromOffset: Map[TopicAndPartition, Long],
    msghandle: (MessageAndMetadata[K, V]) => R = msgHandle
    ): InputDStream[R] = {
    if (kp == null || !kp.contains(GROUP_ID))
      throw new SparkException(s"kafkaParam is Null or ${GROUP_ID} is not setted")
    instance(kp)
    val groupId = kp.get(GROUP_ID).get
    val consumerOffsets: Map[TopicAndPartition, Long] =
      if (fromOffset == null) {
        val last = if (kp.contains(KAFKA_CONSUMER_FROM)) kp.get(KAFKA_CONSUMER_FROM).get
        else defualtFrom
        last.toUpperCase match {
          case "LAST"     => getLatestOffsets(topics, kp)
          case "EARLIEST" => getEarliestOffsets(topics, kp)
          case "CONSUM"   => getConsumerOffset(kp, groupId, topics)
          case _          => log.error(s"""${KAFKA_CONSUMER_FROM} must LAST or CONSUM,defualt is LAST"""); getLatestOffsets(topics, kp)
        }
      } else fromOffset
    consumerOffsets.foreach(x => log.info(x.toString))
    KafkaUtils.createDirectStream[K, V, KD, VD, R](
      ssc,
      kp,
      consumerOffsets,
      msghandle)
  }
  /**
   * @author LMQ
   * @description 创建一个kafka的Dstream。使用配置文件的方式。kp和topic统一放入KafkaConfiguration
   * @param ssc ： 一个StreamingContext
   * @param conf : 配置信息 (不知道怎么配，可以看示例)
   * @param fromOffset ： 如果想自己自定义从指定的offset开始读的话，传入这个值
   * @param msghandle： 读取kafka时提取的数据
   */
  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    ssc: StreamingContext,
    conf: KafkaConfig,
    fromOffset: Map[TopicAndPartition, Long],
    msghandle: (MessageAndMetadata[K, V]) => R  = msgHandle
    ): InputDStream[R] = {
    if (conf.kpIsNull || conf.tpIsNull) {
      throw new SparkException(s"Configuration s kafkaParam is Null or Topics is not setted")
    }
    val kp = conf.getKafkaParams()
    if (!kp.contains(GROUP_ID) && !conf.containsKey(GROUP_ID))
      throw new SparkException(s"Configuration s kafkaParam is Null or ${GROUP_ID} is not setted")
    instance(kp)
    val groupId = if (kp.contains(GROUP_ID)) kp.get(GROUP_ID).get
    else conf.get(GROUP_ID)
    val topics = conf.topics
    val consumerOffsets: Map[TopicAndPartition, Long] =
      if (fromOffset == null) {
        val last = if (kp.contains(KAFKA_CONSUMER_FROM)) kp.get(KAFKA_CONSUMER_FROM).get
        else if (conf.containsKey(KAFKA_CONSUMER_FROM)) conf.get(KAFKA_CONSUMER_FROM)
        else defualtFrom
        last.toUpperCase match {
          case "LAST"     => getLatestOffsets(topics, kp)
          case "EARLIEST" => getEarliestOffsets(topics, kp)
          case "CONSUM"   => getConsumerOffset(kp, groupId, topics)
          case "LAST"     => getLatestOffsets(topics, kp)
        }
      } else fromOffset
    consumerOffsets.foreach(x => log.info(x.toString))
    KafkaUtils.createDirectStream[K, V, KD, VD, R](
      ssc,
      kp,
      consumerOffsets,
      msghandle)
  }
  /**
   * @author LMQ
   * @description 创建一个 receiver 的收集器来手机kafka数据
   */
  def createReceiverStream[K: ClassTag, V: ClassTag, U <: Decoder[_]: ClassTag, T <: Decoder[_]: ClassTag](
    ssc: StreamingContext,
    kp: Map[String, String],
    topics: Map[String, Int]) = {
    KafkaUtils.createStream[K, V, U, T](ssc, kp, topics, StorageLevel.MEMORY_ONLY)
  }

}