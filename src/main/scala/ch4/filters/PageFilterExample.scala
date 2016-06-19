package ch4.filters

import org.apache.hadoop.hbase.client.{Result, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.filter.PageFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper


object PageFilterExample extends App {

  val POSTFIX: Array[Byte] = Array(0x00.toByte)

  val tableName = TableName.valueOf("testtable")
  val conf = HBaseConfiguration.create()
  val helper = new HBaseHelper(conf)
  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1"))
  println("Adding rows to table...")
  helper.fillTable(tableName, 1, 1000, 10, colfams = List("colfam1"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val filter = new PageFilter(15)

  var localRows = -1
  var totalRows = 0
  var lastRow: Array[Byte] = null

  while (localRows != 0) {
    val scan = new Scan()
    scan.setFilter(filter)

    if (lastRow != null) {
      val startRow = Bytes.add(lastRow, POSTFIX)
      println("start row: " + Bytes.toStringBinary(startRow))
      scan.setStartRow(startRow)
    }

    val scanner = table.getScanner(scan)
    var result: Result = scanner.next()
    val (totalR, localR, lastR) = Stream.continually(scanner.next()).takeWhile(_ != null).foldLeft((0, 0, Array.empty[Byte])) { (acc, result) =>
      val (totalRows, localRows, _) = acc
      println((localRows + 1) + ": " + result)
      (totalRows + 1, localRows + 1, result.getRow)
    }
    localRows = localR
    lastRow = lastR
    totalRows += totalR
    scanner.close()
  }

  println("total rows: " + totalRows)

  table.close()
  connection.close()
  helper.close()
}
