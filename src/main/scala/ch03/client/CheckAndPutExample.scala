package ch03.client

import org.apache.hadoop.hbase.client.{Put, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper

object CheckAndPutExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val put1 = new Put(Bytes.toBytes("row1"))
  put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"))
  val res1 = table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), null, put1)
  println("Put 1a applied: " + res1)

  val res2 = table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), null, put1)
  println("Put 1a applied: " + res2)

  val put2 = new Put(Bytes.toBytes("row1"))
  put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"))
  val res3 = table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"), put2)
  println("Put 2 applied: " + res3)

  val put3 = new Put(Bytes.toBytes("row2"))
  put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val3"))

  try {
    val res4 = table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"), put3)
    println("Put 3 applied: " + res4)
  } finally {
    table.close()
    connection.close()
    helper.close()
  }
}
