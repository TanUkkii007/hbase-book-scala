package ch03.client

import org.apache.hadoop.hbase.client.{Put, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper

object PutExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)

  helper.createTable(tableName, List("colfam1"))

  val connection = ConnectionFactory.createConnection(conf)

  val table = connection.getTable(tableName)

  val put = new Put(Bytes.toBytes("row1"))

  put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"))
  put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"))

  println("size ", put.size())

  table.put(put)

  table.close()
  connection.close()
  helper.close()
}
