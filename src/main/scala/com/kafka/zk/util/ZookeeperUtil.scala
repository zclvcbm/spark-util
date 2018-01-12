package com.kafka.zk.util

import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer
import org.apache.zookeeper.CreateMode

object ZookeeperUtil {
  val PERSISTENT = CreateMode.PERSISTENT //短暂，持久
  val EPHEMERAL = CreateMode.EPHEMERAL
  var zkClient:ZkClient=null
  def getzkClient(zk: String) = {
    if(zkClient==null){
     zkClient = new ZkClient(zk, 10000, 10000, ZKStringSerializer)
    }
    zkClient
  }
  def isExist(
    zkClient: ZkClient,
    path: String) = {
    zkClient.exists(path)
  }
  /**
   * 功能：创建目录，如果不存在就创建。
   */
  def createFileOrDir(
    zkClient: ZkClient,
    path: String,
    data: String = "") = {
    if (!zkClient.exists(path))
      try {
        zkClient.create(path, data, PERSISTENT)
      } catch {
        case t: Throwable =>
          t.printStackTrace();
          println("多级目录请使用 createMultistagePath 方法")
          "Error"
      }
    else "is exist"
  }
  /**
   * 功能：创建多级目录
   */
  def createMultistagePath(zkClient: ZkClient, path: String) {
    val paths = path.split("/")
    var curentpath = ""
    paths.foreach { file =>
      if (!file.isEmpty()) {
        curentpath = curentpath + "/" + file
        createFileOrDir(zkClient, curentpath)
      }
    }
  }
  def readData(
    zkClient: ZkClient,
    path: String) = {
    zkClient.readData(path).toString()
  }
  def writeData(
    zkClient: ZkClient,
    path: String,
    data: String) = {
    if (!zkClient.exists(path)) {
      zkClient.create(path, data, PERSISTENT)
    }
    zkClient.writeData(path, data)
  }
}