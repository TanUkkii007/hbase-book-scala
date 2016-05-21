package ch4.filters

import org.apache.hadoop.hbase.client.{Get, Scan, ConnectionFactory}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper
import util.ByteConverter._
import scala.collection.JavaConverters._

object DependentColumnFilterExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1", "colfam2"))

  println("Adding rows to table...")
  helper.fillTable(tableName, startRow = 1, endRow = 10, numCols = 10, pad = -1, setTimestamp = true, random = false, List("colfam1", "colfam2"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  import CompareFilter.CompareOp
  filter(drop = true, CompareOp.NO_OP)
  filter(drop = false, CompareOp.NO_OP)
  filter(drop = true, CompareOp.EQUAL, Some(new BinaryPrefixComparator("val-5".toUTF8Byte)))
  filter(drop = false, CompareOp.EQUAL, Some(new BinaryPrefixComparator("val-5".toUTF8Byte)))
  filter(drop = true, CompareOp.EQUAL, Some(new RegexStringComparator(".*\\.5")))
  filter(drop = false, CompareOp.EQUAL, Some(new RegexStringComparator(".*\\.5")))

  helper.close()
  table.close()
  connection.close()


  def filter(drop: Boolean, operator: CompareFilter.CompareOp, comparator: Option[ByteArrayComparable] = None): Unit = {
    val filter = comparator.fold(new DependentColumnFilter("colfam1".toUTF8Byte, "col-5".toUTF8Byte, drop)) { comparator =>
      new DependentColumnFilter("colfam1".toUTF8Byte, "col-5".toUTF8Byte, drop, operator, comparator)
    }

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
      println("Cell: " + cell + ", Value: " + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()))
    }
    println("")
  }

}
