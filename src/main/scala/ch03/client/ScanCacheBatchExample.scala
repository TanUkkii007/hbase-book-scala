package ch03.client

import org.apache.hadoop.hbase.client.{Scan, ConnectionFactory}
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper
import scala.collection.JavaConverters._

object ScanCacheBatchExample extends App {
  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1", "colfam2"))

  helper.fillTable(tableName, startRow = 1, endRow = 10, numCols = 10, -1, setTimestamp = false, random = false, List("colfam1", "colfam2"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  scan(1, 1, false)
  scan(1, 0, false)
  scan(1, 0, true)
  scan(200, 1, false)
  scan(200, 0, false)
  scan(200, 0, true)
  scan(2000, 100, false)
  scan(2, 100, false)
  scan(2, 10, false)
  scan(5, 100, false)
  scan(5, 20, false)
  scan(10, 10, false)

  table.close()
  connection.close()
  helper.close()

  def scan(caching: Int, batch: Int, small: Boolean) = {
    val scan = new Scan()
      .setCaching(caching)
      .setBatch(batch)
      .setSmall(small)
      .setScanMetricsEnabled(true)

    val scanner = table.getScanner(scan)
    val count = scanner.asScala.size
    scanner.close()
    val metrics = scan.getScanMetrics
    println("Caching: " + caching + ", Batch: " + batch +
      ", Small: " + small + ", Results: " + count +
      ", RPCs: " + metrics.countOfRPCcalls)
  }
}
