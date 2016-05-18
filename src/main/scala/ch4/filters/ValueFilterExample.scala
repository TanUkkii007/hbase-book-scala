package ch4.filters

import org.apache.hadoop.hbase.client.{Get, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.filter.{SubstringComparator, CompareFilter, ValueFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper
import util.ByteConverter._
import scala.collection.JavaConverters._

object ValueFilterExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1", "colfam2"))

  println("Adding rows to table...")
  helper.fillTable(tableName, startRow = 1, endRow = 10, numCols = 10, pad = -1, setTimestamp = false, random = false, List("colfam1", "colfam2"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(".4"))

  val scan = new Scan()
  scan.setFilter(filter)
  val scanner = table.getScanner(scan)

  println("Results of scan:")

  scanner.asScala.foreach { result =>
    result.rawCells().foreach { cell =>
      println("Cell: " + cell + ", Value: " + Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength))
    }
  }
  scanner.close()

  val get = new Get("row-5".toUTF8Byte)
  get.setFilter(filter)
  val result = table.get(get)

  println("Result of get: ")

  result.rawCells().foreach { cell =>
    println("Cell: " + cell + ", Value: " + Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength))
  }

  helper.close()
  table.close()
  connection.close()
}
