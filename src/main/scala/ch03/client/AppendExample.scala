package ch03.client

import org.apache.hadoop.hbase.client.{Append, ConnectionFactory}
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.{CFValues, HBaseHelper}
import util.ByteConverter._

object AppendExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1", "colfam2"), 100)

  helper.put(tableName, List("row1"), List("colfam1"), List(
    CFValues("qual1", 1, "oldvalue")
  ))

  println("Before append call...")
  helper.dump(tableName, List("row1"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val append = new Append("row1".toUTF8Byte)
  append.add("colfam1".toUTF8Byte, "qual1".toUTF8Byte, "newvalue".toUTF8Byte)
  append.add("colfam1".toUTF8Byte, "qual2".toUTF8Byte, "anothervalue".toUTF8Byte)

  table.append(append)

  println("After append call...")
  helper.dump(tableName, List("row1"))
  table.close()
  connection.close()
  helper.close()
}
