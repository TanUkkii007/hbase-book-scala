package ch03.client

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.{BatchCallbackConversion, CFValues, HBaseHelper}
import util.ByteConverter._
import BatchCallbackConversion._
import scala.collection.JavaConverters._

object BatchCallbackExample extends App {
  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1", "colfam2"))

  helper.put(tableName, List("row1"), List("colfam1"), List(
    CFValues("qual1", 1, "val1"),
    CFValues("qual2", 2, "val2"),
    CFValues("qual3", 3, "val3")
  ))

  println("Before batch call...")
  helper.dump(tableName, List("row1", "row2"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val put = new Put("row2".toUTF8Byte)
  put.addColumn("colfam2".toUTF8Byte, "qual1".toUTF8Byte, 4, "val5".toUTF8Byte)

  val get1 = new Get("row1".toUTF8Byte)
  get1.addColumn("colfam1".toUTF8Byte, "qual1".toUTF8Byte)

  val delete = new Delete("row1".toUTF8Byte)
  delete.addColumns("colfam1".toUTF8Byte, "qual2".toUTF8Byte)

  val get2 = new Get("row2".toUTF8Byte)
  get2.addFamily("BOGUS".toUTF8Byte)

  val batch = List(put, get1, delete, get2)

  val results = Array.ofDim[AnyRef](batch.size)

  try {
    table.batchCallback(batch.asJava, results, (region: Array[Byte], row: Array[Byte], result: Result) => {
      println("Received callback for row[" + Bytes.toString(row) + "] -> " + result)
    })
  } catch {
    case e: Throwable => System.err.println("Error: " + e)
  }

  results.zipWithIndex.foreach { p =>
    val (result, i) = p
    println(s"Result[$i]: type = ${result.getClass.getSimpleName}; $result")
  }

  table.close()
  connection.close()
  println("After batch call...")
  helper.dump(tableName, List("row1", "row2"))
  helper.close()
}
