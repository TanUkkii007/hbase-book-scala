package ch03.client

import org.apache.hadoop.hbase.client.{Delete, ConnectionFactory}
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.{CFValues, HBaseHelper}
import util.ByteConverter._

object CheckAndDeleteExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1", "colfam2"), 100)

  helper.put(tableName, List("row1"), List("colfam1", "colfam2"), List(
    CFValues("qual1", 1, "val1"),
    CFValues("qual2", 2, "val2"),
    CFValues("qual3", 3, "val3")
  ))

  println("Before delete call...")
  helper.dump(tableName, List("row1", "row2", "row3"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val delete1 = new Delete("row1".toUTF8Byte)
  delete1.addColumns("colfam1".toUTF8Byte, "qual3".toUTF8Byte)

  val res1 = table.checkAndDelete("row1".toUTF8Byte, "colfam2".toUTF8Byte, "qual3".toUTF8Byte, null, delete1) //nonexistence test
  println("Delete 1 successful: " + res1)

  val delete2 = new Delete("row1".toUTF8Byte)
  delete2.addColumns("colfam2".toUTF8Byte, "qual3".toUTF8Byte)
  table.delete(delete2) //Delete checked column manually.

  val res2 = table.checkAndDelete("row1".toUTF8Byte, "colfam2".toUTF8Byte, "qual3".toUTF8Byte, null, delete1) //nonexistence test
  println("Delete 2 successful: " + res2)

  val delete3 = new Delete("row2".toUTF8Byte).addFamily("colfam1".toUTF8Byte)

  try {
    val res3 = table.checkAndDelete("row1".toUTF8Byte, "colfam1".toUTF8Byte, "qual1".toUTF8Byte, "val1".toUTF8Byte, delete3)
    println("Delete 3 successful: " + res3)
  } catch {
    case e: Throwable => System.err.println("Error: " + e.getMessage)
  }

  table.close()
  connection.close()
  println()
  helper.dump(tableName, List("row1"))
  helper.close()
}
