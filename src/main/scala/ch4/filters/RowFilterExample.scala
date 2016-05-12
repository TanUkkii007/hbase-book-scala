package ch4.filters

import org.apache.hadoop.hbase.client.{Scan, ConnectionFactory}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper
import util.ByteConverter._
import scala.collection.JavaConverters._

object RowFilterExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1", "colfam2"))

  println("Adding rows to table...")
  helper.fillTable(tableName, startRow = 1, endRow = 100, numCols = 100, pad = -1, setTimestamp = false, random = false, List("colfam1", "colfam2"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val scan = new Scan()
  scan.addColumn("colfam1".toUTF8Byte, "col-1".toUTF8Byte)

  val filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator("row-22".toUTF8Byte))
  scan.setFilter(filter1)
  val scanner1 = table.getScanner(scan)

  println("Scanning table #1...")
  scanner1.asScala.foreach(println)
  scanner1.close()

  val filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*-.5"))
  scan.setFilter(filter2)
  val scanner2 = table.getScanner(scan)

  println("Scanning table #2...")
  scanner2.asScala.foreach(println)
  scanner2.close()

  val filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("-5"))
  scan.setFilter(filter3)
  val scanner3 = table.getScanner(scan)

  println("Scanning table #3...")
  scanner3.asScala.foreach(println)
  scanner3.close()

  table.close()
  connection.close()
}
