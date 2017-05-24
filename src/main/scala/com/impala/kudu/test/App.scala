package com.impala.kudu.test

import java.sql.DriverManager
import java.util.Date

object App {
  // set the impalad host
  val IMPALAD_HOST = "192.168.10.194";
  // port 21050 is the default impalad JDBC port 
  val IMPALAD_JDBC_PORT = "21050";
  val CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/;auth=noSasl";
  val JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  def main(args: Array[String]) {
    //upsert()
   execute
  }
  def execute() {
    val sql="""select siteid,plan,activity,slot from
      kudu_pc_log group by siteid,plan,activity,slot limit 10
      """
    Class.forName(JDBC_DRIVER_NAME);
    val con = DriverManager.getConnection(CONNECTION_URL);
    val stmt = con.createStatement();
    for(i<- 1 to 4){
    	val s=new Date().getTime
    	val rs = stmt.executeQuery(sql);
    	println(s"""${new Date().getTime - s}""")
    	while (rs.next()) {
    		System.out.println(rs.getString(1));
    	}
    }
      
  }
  def upsert() {
    var sql = s"""upsert into kudu_pc_log(deliverytime,siteid,plan,activity,uid,slot) 
			values("q1","6","2","2","aaa","slot")"""
    Class.forName(JDBC_DRIVER_NAME);
    val con = DriverManager.getConnection(CONNECTION_URL);
    val stmt = con.createStatement();
    stmt.execute(sql)
  }
}
