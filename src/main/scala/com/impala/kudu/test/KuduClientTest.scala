package com.impala.kudu.test

import org.apache.kudu.client.KuduClient
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import scala.collection.JavaConversions._
import org.apache.kudu.client.KuduPredicate
object KuduClientTest {
  def main(args: Array[String]): Unit = {
    val client=new KuduClientBuilder("kylin-master2").build()
    val tables=client.getTablesList
    tables.getTablesList.foreach { println }
    //val session=client.newSession()
    //val table=client.openTable("")
  }
}