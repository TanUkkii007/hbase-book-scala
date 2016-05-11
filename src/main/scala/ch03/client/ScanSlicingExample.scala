package ch03.client

import org.apache.hadoop.hbase.client.{Scan, ConnectionFactory}
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper
import scala.collection.JavaConverters._

object ScanSlicingExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1", "colfam2"))

  helper.fillTable(tableName, startRow = 1, endRow = 10, numCols = 10, pad = 2, setTimestamp = true, random = false, List("colfam1", "colfam2"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  scan(1, 11, 0, 0, 2, -1, true)
  scan(2, 11, 0, 4, 2, -1, true)
  scan(3, 5, 0, 0, 2, -1, false)
  scan(4, 11, 2, 0, 5, -1, true)
  scan(5, 11, -1, -1, -1, 1, false)
  scan(6, 11, -1, -1, -1, 10000, false)

  table.close()
  connection.close()
  helper.close()

  def scan(num: Int, caching: Int, batch: Int, offset: Int, maxResults: Int, maxResultSize: Int, dump: Boolean) = {
    val scan = new Scan()
      .setCaching(caching)
      .setBatch(batch)
      .setRowOffsetPerColumnFamily(offset)
      .setMaxResultsPerColumnFamily(maxResults)
      .setScanMetricsEnabled(true)

    val scanner = table.getScanner(scan)
    println("Scan #" + num + " running...")
    if (dump) scanner.asScala.foreach(println)
    val count = scanner.asScala.size
    scanner.close()
    val metrics = scan.getScanMetrics
    println("Caching: " + caching + ", Batch: " + batch +
      ", Offset: " + offset + ", maxResults: " + maxResults +
      ", maxSize: " + maxResultSize + ", Results: " + count +
      ", RPCs: " + metrics.countOfRPCcalls)
  }
}
