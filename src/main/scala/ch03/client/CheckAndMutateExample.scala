package ch03.client

import org.apache.hadoop.hbase.client.{RowMutations, Delete, Put, ConnectionFactory}
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.{CFValues, HBaseHelper}
import util.ByteConverter._

object CheckAndMutateExample extends App {
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

  println("Before check and mutate call...")
  helper.dump(tableName, List("row1"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val put = new Put("row1".toUTF8Byte)
    .addColumn("colfam1".toUTF8Byte, "qual1".toUTF8Byte, 4, "val99".toUTF8Byte)
    .addColumn("colfam1".toUTF8Byte, "qual2".toUTF8Byte, 4, "val100".toUTF8Byte)

  val delete = new Delete("row1".toUTF8Byte)
  delete.addColumn("colfam1".toUTF8Byte, "qual2".toUTF8Byte)

  val mutations = new RowMutations("row1".toUTF8Byte)
  mutations.add(put)
  mutations.add(delete)

  val res1 = table.checkAndMutate(
    "row1".toUTF8Byte,
    "colfam2".toUTF8Byte,
    "qual1".toUTF8Byte,
    CompareFilter.CompareOp.LESS,
    "val1".toUTF8Byte, mutations
  )
  println("Mutate 1 successful: " + res1)

  val put2 = new Put("row1".toUTF8Byte)
  put2.addColumn("colfam2".toUTF8Byte, "qual1".toUTF8Byte, 4, "val2".toUTF8Byte)
  table.put(put2)

  val res2 = table.checkAndMutate(
    "row1".toUTF8Byte,
    "colfam2".toUTF8Byte,
    "qual1".toUTF8Byte,
    CompareFilter.CompareOp.LESS,
    "val1".toUTF8Byte,
    mutations
  )
  println("Mutate 2 successful: " + res2)

  println("After check and mutate calls...")
  helper.dump(tableName, List("row1"))
  table.close()
  connection.close()
  helper.close()
}
