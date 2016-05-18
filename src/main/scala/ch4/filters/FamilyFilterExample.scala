package ch4.filters

import org.apache.hadoop.hbase.client.{Get, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.filter.{BinaryComparator, CompareFilter, FamilyFilter}
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper
import util.ByteConverter._
import scala.collection.JavaConverters._

object FamilyFilterExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1", "colfam2", "colfam3", "colfam4"))

  println("Adding rows to table...")
  helper.fillTable(tableName, startRow = 1, endRow = 10, numCols = 2, pad = -1, setTimestamp = false, random = false, List("colfam1", "colfam2", "colfam3", "colfam4"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val filter1 = new FamilyFilter(CompareFilter.CompareOp.LESS, new BinaryComparator("colfam3".toUTF8Byte))

  val scan = new Scan()
  scan.setFilter(filter1)
  val scanner = table.getScanner(scan)
  println("Scanning table... ")
  scanner.asScala.foreach(println)
  scanner.close()

  val get1 = new Get("row-5".toUTF8Byte)
  get1.setFilter(filter1)
  val result1 = table.get(get1)
  println("Result of get(): " + result1)

  val filter2 = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("colfam3".toUTF8Byte))
  val get2 = new Get("row-5".toUTF8Byte)
  get2.addFamily("colfam1".toUTF8Byte)
  get2.setFilter(filter2)
  val result2 = table.get(get2)
  println("Result of get(): " + result2)

  helper.close()
  table.close()
  connection.close()
}
