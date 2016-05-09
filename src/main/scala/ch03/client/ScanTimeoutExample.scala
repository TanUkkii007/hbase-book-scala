package ch03.client

import org.apache.hadoop.hbase.client.{Scan, ConnectionFactory}
import org.apache.hadoop.hbase.{HConstants, TableName, HBaseConfiguration}
import util.HBaseHelper

object ScanTimeoutExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1", "colfam2"))

  println("Adding rows to table...")

  helper.fillTable(tableName, startRow = 1, endRow = 10, numCols = 10, -1, setTimestamp = false, random = false, List("colfam1", "colfam2"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val scan = new Scan()
  val scanner = table.getScanner(scan)
  val scannerTimeout = conf.getLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1)

  println("Current (local) lease period: " + scannerTimeout + "ms")
  println("Sleeping now for " + (scannerTimeout + 5000) + "ms...")

  Thread.sleep(scannerTimeout + 5000)

  println("Attempting to iterate over scanner...")

  var isBreak = false
  while (!isBreak) {
    try {
      val result = scanner.next()
      if (result == null) isBreak = true
      println(result)
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        isBreak = true
      }
    }
  }
  scanner.close()
  table.close()
  connection.close()
  helper.close()
}
