package ch4.filters

import org.apache.hadoop.hbase.client.{Scan, ConnectionFactory}
import org.apache.hadoop.hbase.filter.{Filter, KeyOnlyFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper
import scala.collection.JavaConverters._

object KeyOnlyFilterExample extends App {

  val tableName = TableName.valueOf("testtable")

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)
  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1"))
  println("Adding rows to table...")
  helper.fillTableRandom(tableName,
    1, 5, 0, //row
    1, 30, 0, //col
    0, 10000, 0, //val
    setTimestamp = true, List("colfam1")
  )

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  println("Scan #1")
  val filter1 = new KeyOnlyFilter()
  scan(filter1)

  println("Scan #2")
  val filter2 = new KeyOnlyFilter(true)
  scan(filter2)

  table.close()
  connection.close()
  helper.close()

  def scan(filter: Filter) = {
    val scan = new Scan()
    scan.setFilter(filter)
    val scanner = table.getScanner(scan)
    println("Results of scan:")
    val rowCount = scanner.asScala.foldLeft(0) { (acc, result) =>
      result.rawCells().foreach { cell =>
        println(s"Cell: $cell, Value:  ${if (cell.getValueLength() > 0) Bytes.toInt(cell.getValueArray(), cell.getValueOffset(),
              cell.getValueLength()) else "n/a"}")
      }
      acc + 1
    }
    println("Total num of rows: " + rowCount)
    scanner.close()
  }
}
