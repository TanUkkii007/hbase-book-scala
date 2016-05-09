package ch03.client

import org.apache.hadoop.hbase.client.{ResultScanner, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper
import scala.collection.JavaConverters._
import util.ByteConverter._

object ScanExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1", "colfam2"))

  println("Adding rows to table...")

  helper.fillTable(tableName, startRow = 1, endRow = 100, numCols = 100, -1, setTimestamp = false, random = false, List("colfam1", "colfam2"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  println("Scanning table #1...")
  val scan1 = new Scan()
  val scanner1: ResultScanner = table.getScanner(scan1)
  scanner1.asScala.foreach(println)
  scanner1.close()

  println("Scanning table #2...")
  val scan2 = new Scan()
  scan2.addFamily("colfam1".toUTF8Byte)
  val scanner2 = table.getScanner(scan2)
  scanner2.asScala.foreach(println)
  scanner2.close()

  println("Scanning table #3...")
  val scan3 = new Scan()
  scan3.addColumn("colfam1".toUTF8Byte, "col-5".toUTF8Byte)
    .addColumn("colfam2".toUTF8Byte, "col-33".toUTF8Byte)
    .setStartRow("row-10".toUTF8Byte)
    .setStopRow("row-20".toUTF8Byte)
  val scanner3 = table.getScanner(scan3)
  scanner3.asScala.foreach(println)
  scanner3.close()

  println("Scanning table #4...")
  val scan4 = new Scan()
  scan4.addColumn("colfam1".toUTF8Byte, "col-5".toUTF8Byte)
    .setStartRow("row-10".toUTF8Byte)
    .setStopRow("row-20".toUTF8Byte)
  val scanner4 = table.getScanner(scan4)
  scanner4.asScala.foreach(println)
  scanner4.close()

  println("Scanning table #4...")
  val scan5 = new Scan()
  scan5.addColumn("colfam1".toUTF8Byte, "col-5".toUTF8Byte)
    .setStartRow("row-20".toUTF8Byte)
    .setStopRow("row-10".toUTF8Byte)
    .setReversed(true)
  val scanner5 = table.getScanner(scan5)
  scanner5.asScala.foreach(println)
  scanner5.close()

  table.close()
  connection.close()
  helper.close()
}
