package ch03.client

import org.apache.hadoop.hbase.client.{Get, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper

object GetExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  if (!helper.existsTable(tableName)) {
    helper.createTable(tableName, List("colfam1"))
  }

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val get = new Get(Bytes.toBytes("row1"))
  get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))

  val result = table.get(get)

  val value = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))

  println(s"Value: ${Bytes.toString(value)}")

  table.close()
  connection.close()
  helper.close()
}
