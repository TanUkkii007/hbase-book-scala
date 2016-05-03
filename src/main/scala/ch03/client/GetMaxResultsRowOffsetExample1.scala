package ch03.client

import java.util.Locale

import org.apache.hadoop.hbase.client.{Get, Put, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper

object GetMaxResultsRowOffsetExample1 extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val put = new Put(Bytes.toBytes("row1"))
  1 to 1000 foreach { n =>
    val num = "%04d".format(n)
    put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes(s"qual$num"), Bytes.toBytes(s"val$num"))
  }
  table.put(put)

  val get1 = new Get(Bytes.toBytes("row1"))
  get1.setMaxResultsPerColumnFamily(10)
  val result1 = table.get(get1)
  val scanner1 = result1.cellScanner()
  while (scanner1.advance()) {
    println("Get 1 Cell: " + scanner1.current())
  }

  val get2 = new Get(Bytes.toBytes("row1"))
  get2.setMaxResultsPerColumnFamily(10)
  get2.setRowOffsetPerColumnFamily(100)
  val result2 = table.get(get2)
  val scanner2 = result2.cellScanner()
  while (scanner2.advance()) {
    println("Get 2 Cell: " + scanner2.current())
  }

  table.close()
  connection.close()
  helper.close()
}
