package ch4.filters

import org.apache.hadoop.hbase.client.{Get, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.filter.{BinaryComparator, CompareFilter, QualifierFilter}
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper
import util.ByteConverter._
import scala.collection.JavaConverters._

object QualifierFilterExample extends App {
  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1", "colfam2"))

  println("Adding rows to table...")
  helper.fillTable(tableName, startRow = 1, endRow = 10, numCols = 10, pad = -1, setTimestamp = false, random = false, List("colfam1", "colfam2"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val filter = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator("col-2".toUTF8Byte))
  val scan = new Scan()
  scan.setFilter(filter)
  val scanner = table.getScanner(scan)
  println("Scanning table... ")
  scanner.asScala.foreach(println)
  scanner.close()

  val get = new Get("row-5".toUTF8Byte)
  get.setFilter(filter)
  val result = table.get(get)
  println("Result of get(): " + result)

  helper.close()
  table.close()
  connection.close()
}
