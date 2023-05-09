package com.labs1904.hwe

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.{LogManager, Logger}

object MyApp {
  lazy val logger: Logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("MyApp starting...")
    var connection: Connection = null
    try {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "CHANGE ME")
      connection = ConnectionFactory.createConnection(conf)
      // Example code... change me
      val table = connection.getTable(TableName.valueOf(("slashley:users")))

      val put = new Put(Bytes.toBytes("99"))
      put.addColumn(
        Bytes.toBytes("f1"),
        Bytes.toBytes("username"),
        Bytes.toBytes("DE-HWE")
      )
      put.addColumn(
        Bytes.toBytes("f1"),
        Bytes.toBytes("name"),
        Bytes.toBytes("The Panther")
      )
      put.addColumn(
        Bytes.toBytes("f1"),
        Bytes.toBytes("sex"),
        Bytes.toBytes("F")
      )
      put.addColumn(
        Bytes.toBytes("f1"),
        Bytes.toBytes("favorite_color"),
        Bytes.toBytes("pink")
      )
      table.put(put)

      val get = new Get(Bytes.toBytes("99"))
      val result = table.get(get)

      //val get = new Get(Bytes.toBytes("10000001"))
      //get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("mail"))
      //val result = table.get(get)
      //val mail =
        //Bytes.toString(
          //result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail")))


      logger.debug(result) //logger is what actually prints to console
    } catch {
      case e: Exception => logger.error("Error in main", e)
    } finally {
      if (connection != null) connection.close()
    }
  }
}
