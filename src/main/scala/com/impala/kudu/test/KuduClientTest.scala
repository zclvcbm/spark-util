package com.impala.kudu.test

import org.apache.kudu.client.KuduClient
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import scala.collection.JavaConversions._
import org.apache.kudu.client.KuduPredicate
object KuduClientTest {
  def main(args: Array[String]): Unit = {
    val client=new KuduClientBuilder("kylin-master2").build()
    val table=client.openTable("impala::default.kudu_pc_log")
    client.getTablesList.getTablesList.foreach { println }
    val scanner = client.newScanTokenBuilder(table).build()
    scanner.foreach { sk =>
      
      println(sk.getTablet.getPartition)
      println( sk.getTablet.getReplicas)
      
    
    }
    
                    
    
    
    
  }
}