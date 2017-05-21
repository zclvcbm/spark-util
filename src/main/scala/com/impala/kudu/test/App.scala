package com.impala.kudu.test

import java.sql.DriverManager

object App {
  // set the impalad host
  val IMPALAD_HOST = "192.168.10.194";

  // port 21050 is the default impalad JDBC port 
  val IMPALAD_JDBC_PORT = "21050";

  val CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/;auth=noSasl";

  val JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  def main(args: Array[String]) {
    upsert()

  }
  def execute(sql: String) {
    Class.forName(JDBC_DRIVER_NAME);
    val con = DriverManager.getConnection(CONNECTION_URL);
    val stmt = con.createStatement();
    val rs = stmt.executeQuery(sql);
    while (rs.next()) {
      System.out.println(rs.getString(1));
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
