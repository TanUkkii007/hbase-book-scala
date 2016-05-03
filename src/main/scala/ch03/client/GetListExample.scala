package ch03.client

import org.apache.hadoop.hbase.client.{Get, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, TableName, HBaseConfiguration}
import util.HBaseHelper
import scala.collection.JavaConverters._

object GetListExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  if (!helper.existsTable(tableName)) {
    helper.createTable(tableName, List("colfam1"))
  }

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val cf1 = Bytes.toBytes("colfam1")
  val qf1 = Bytes.toBytes("qual1")
  val qf2 = Bytes.toBytes("qual2")
  val row1 = Bytes.toBytes("row1")
  val row2 = Bytes.toBytes("row2")

  val get1 = new Get(row1)
  get1.addColumn(cf1, qf1)

  val get2 = new Get(row2)
  get2.addColumn(cf1, qf1)

  val get3 = new Get(row2)
  get2.addColumn(cf1, qf2)

  val results = table.get(List(get1, get2, get3).asJava)

  println("First iteration...")
  results.foreach { result =>
    val row = Bytes.toString(result.getRow)
    print("Row: " + row + " ")
    if (result.containsColumn(cf1, qf1))
    println("Value: " + Bytes.toString(result.getValue(cf1, qf1)))
    if (result.containsColumn(cf1, qf2))
    println("Value: " + Bytes.toString(result.getValue(cf1, qf2)))
  }

  println("Second iteration...")

  results.foreach { result =>
    result.listCells().asScala.foreach { cell =>
      println("Row: " + Bytes.toString(cell.getRowArray, cell.getRowOffset, cell.getRowLength) + " Value: " + Bytes.toString(CellUtil.cloneValue(cell)))
    }
  }

  println("Third iteration...")
  results.foreach { result =>
    println(result)
  }

  table.close()
  connection.close()
  helper.close()
}
