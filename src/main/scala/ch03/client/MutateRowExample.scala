package ch03.client

import org.apache.hadoop.hbase.client.{RowMutations, Delete, Put, ConnectionFactory}
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.{CFValues, HBaseHelper}
import util.ByteConverter._

object MutateRowExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1"), 3)

  helper.put(tableName, List("row1"), List("colfam1"), List(
    CFValues("qual1", 1, "val1"),
    CFValues("qual2", 2, "val2"),
    CFValues("qual3", 3, "val3")
  ))

  println("Before mutate call...")
  helper.dump(tableName, List("row1"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val put = new Put("row1".toUTF8Byte)
  put.addColumn("colfam1".toUTF8Byte, "qual1".toUTF8Byte, 4, "val99".toUTF8Byte)
  put.addColumn("colfam1".toUTF8Byte, "qual4".toUTF8Byte, 4, "val100".toUTF8Byte)

  val delete = new Delete("row1".toUTF8Byte)
  delete.addColumn("colfam1".toUTF8Byte, "qual2".toUTF8Byte)

  val mutations = new RowMutations("row1".toUTF8Byte)
  mutations.add(put)
  mutations.add(delete)

  table.mutateRow(mutations)

  table.close()
  connection.close()
  println("After mutate call...")
  helper.dump(tableName, List("row1"))
  helper.close()

}
