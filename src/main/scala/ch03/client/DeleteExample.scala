package ch03.client

import org.apache.hadoop.hbase.client.{Delete, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.{ByteConverter, CFValues, HBaseHelper}
import ByteConverter._

object DeleteExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1", "colfam2"), 100)

  helper.put(tableName, List("row1"), List("colfam1", "colfam2"), List(
    CFValues("qual1", 1, "val1"),
    CFValues("qual1", 2, "val1"),
    CFValues("qual2", 3, "val2"),
    CFValues("qual2", 4, "val2"),
    CFValues("qual3", 5, "val3"),
    CFValues("qual3", 6, "val3")
  ))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val delete = new Delete(Bytes.toBytes("row1"))
  delete.setTimestamp(1)

  delete.addColumn("colfam1".toUTF8Byte, "qual1".toUTF8Byte)
  delete.addColumn("colfam1".toUTF8Byte, "qual3".toUTF8Byte, 3)

  delete.addColumns("colfam1".toUTF8Byte, "qual1".toUTF8Byte)
  delete.addColumns("colfam1".toUTF8Byte, "qual3".toUTF8Byte, 2)

  delete.addFamily("colfam1".toUTF8Byte)
  delete.addFamily("colfam1".toUTF8Byte, 3)

  table.delete(delete)

  table.close()
  connection.close()
  println("After delete call...")
  helper.dump(tableName, List("row1"))
  helper.close()
}
