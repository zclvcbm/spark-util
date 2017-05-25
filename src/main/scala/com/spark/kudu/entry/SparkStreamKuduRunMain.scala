package com.spark.kudu.entry
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaSparkStreamManager
import kafka.serializer.StringDecoder
import org.slf4j.LoggerFactory
import org.apache.spark.common.util.Configuration
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.func.tool._
import org.apache.log4j.PropertyConfigurator
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SQLContext
import java.util.Date
import java.text.SimpleDateFormat
/**
 * @author LMQ
 * @description 将sparkstreaming的数据写进kudu。同时使用impala生成OLAP报表存成kudu。
 *
 */
object SparkStreamKuduRunMain {
  val sim = new SimpleDateFormat("yyyy-MM-dd");
  def main(args: Array[String]): Unit = {
    val batchTime = args(0).toLong
    runJob(batchTime)
  }
  def runJob(time: Long) {
    val sc = new SparkContext(new SparkConf().setAppName("SparkStreamKuduRunMain"))
    val kuducontext = new KuduContext(kudumaster, sc)
    val sparksql = new SQLContext(sc)
    import sparksql.implicits._
    val ssc = new StreamingContext(sc, Seconds(time))
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "test",
      "newgroup.last.earliest" -> "earliest", //如果是新的group id 就从最新还是最旧开始
      "kafka.last.consum" -> "consum")
    val topics = intopics.split(",").toSet
    val ds = ssc.createDirectStream[(String, String)](kp, topics, msgHandle)
    var count = 0L
    ds.foreachRDD { rdd =>
      LOG.info("##### start ##################")
      /*val delivery = rdd.filter { x =>
        x._1 == "smartadsdeliverylog" && x._2.split(",")(25).nonEmpty
      }.count
      LOG.info("曝光 .." + delivery)
      val click = rdd.filter { x =>
        x._1 == "smartadsclicklog" && x._2.split(",")(17).nonEmpty
      }.count
      LOG.info("点击 .." + click)*/
      LOG.info("总数 .." + ( rdd.count))
      val data = rdd.transtoDF(transPC).toDF()
      val dfcount = data.count
      LOG.info("df.." + dfcount)
      count = count + dfcount
      kuducontext.insertRows(data, "impala::default.smartadslog")
      LOG.info("kudu .." + count)
      val date = new Date()
      val s = date.getTime
      val statdate = sim.format(date)
      LOG.info("##### rt_rtbreport ")
      KuduImpalaUtil.execute(rt_rtbreport(statdate))
      LOG.info("##### rt_rtbreport_byhour ")
      KuduImpalaUtil.execute(rt_rtbreport_byhour(statdate))
      LOG.info("##### rt_rtbreport_byplan ")
      KuduImpalaUtil.execute(rt_rtbreport_byplan(statdate))
      LOG.info("##### rt_rtbreport_byactivity ")
      KuduImpalaUtil.execute(rt_rtbreport_byactivity(statdate))
      LOG.info("##### rt_rtbreport_bycreative ")
      KuduImpalaUtil.execute(rt_rtbreport_bycreative(statdate))
      LOG.info("##### rt_rtbreport_byplan_unit ")
      KuduImpalaUtil.execute(rt_rtbreport_byplan_unit(statdate))
      LOG.info("##### rt_rtbreport_byplan_unit_creative ")
      KuduImpalaUtil.execute(rt_rtbreport_byplan_unit_creative(statdate))
      LOG.info("##### rt_rtbreport_byslot_channel_plan ")
      KuduImpalaUtil.execute(rt_rtbreport_byslot_channel_plan(statdate))
      LOG.info("##### rt_rtbreport_bydomain_channel_plan ")
      KuduImpalaUtil.execute(rt_rtbreport_bydomain_channel_plan(statdate))
      LOG.info("##### rt_rtbreport_byhour_channel_plan ")
      KuduImpalaUtil.execute(rt_rtbreport_byhour_channel_plan(statdate))
      LOG.info(s"""time : ${new Date().getTime - s}""")
      rdd.updateOffsets(kp, "test")
      LOG.info("##### END ##################")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}