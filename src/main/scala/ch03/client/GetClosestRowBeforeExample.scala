package ch03.client

import org.apache.hadoop.hbase.client.{Get, Put, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper

object GetClosestRowBeforeExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  if (!helper.existsTable(tableName)) {
    helper.createTable(tableName, List("colfam1"))
  }

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val put1 = new Put(Bytes.toBytes("row1"))
  put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"))
  val put2 = new Put(Bytes.toBytes("row2"))
  put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"))
  val put3 = new Put(Bytes.toBytes("row2"))
  put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val3"))

  val get1 = new Get(Bytes.toBytes("row3"))
  get1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))
  val result1 = table.get(get1)

  println("Get 1 isEmpty: " + result1.isEmpty)
  val scanner1 = result1.cellScanner()
  while (scanner1.advance()) {
    println("Get 1 Cell: " + scanner1.current())
  }

  val get2 = new Get(Bytes.toBytes("row3"))
  get2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))
  get2.setClosestRowBefore(true)
  val result2 = table.get(get2)

  println("Get 2 isEmpty: " + result2.isEmpty)
  val scanner2 = result2.cellScanner()
  while (scanner2.advance()) {
    println("Get 2 Cell: " + scanner2.current())
  }

  val get3 = new Get(Bytes.toBytes("row2"))
  get3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))
  get3.setClosestRowBefore(true)
  val result3 = table.get(get3)

  println("Get 3 isEmpty: " + result3.isEmpty)
  val scanner3 = result3.cellScanner()
  while (scanner3.advance()) {
    println("Get 3 Cell: " + scanner3.current())
  }

  val get4 = new Get(Bytes.toBytes("row2"))
  get4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))
  val result4 = table.get(get4)

  println("Get 4 isEmpty: " + result4.isEmpty)
  val scanner4 = result4.cellScanner()
  while (scanner4.advance()) {
    println("Get 4 Cell: " + scanner4.current())
  }

  table.close()
  connection.close()
  helper.close()
}
