package com.database.es.util

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import java.net.InetAddress
import org.elasticsearch.common.transport.InetSocketTransportAddress
/**
 * @time ：2018-02-06
 * @func ：用于初始化和获取es的连接
 */
object ElasticsearchClientHandle {
  var client: TransportClient = null
  /**
   * @time :2018-02-01
   * @func :用于获取es的client
   */
  def getESClient(
    address: String,
    clusterName: String) = {
    initESClient(address, clusterName)
    client
  }
   /**
   * @time :2018-02-01
   * @func :用于初始化es的client
   */
  def initESClient(
    address: String,
    clusterName: String) {
    if (client == null) {
      val endpoints = address.split(",").map(_.split(":", -1)).map {
        case Array(host, port) => new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt)
        case Array(host)       => new InetSocketTransportAddress(InetAddress.getByName(host), 9300)
      }
      val settings = Settings
        .settingsBuilder()
        .put("cluster.name", clusterName)
        .build()
      client = TransportClient
        .builder()
        .settings(settings)
        .build()
        .addTransportAddresses(endpoints: _*)
    }
  }
}