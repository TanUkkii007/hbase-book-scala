package ch03.client

import org.apache.hadoop.hbase.client.{Get, Put, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper
import scala.collection.JavaConverters._

object PutWriteBufferExample2 extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val mutator = connection.getBufferedMutator(tableName)

  val put1 = new Put(Bytes.toBytes("row1"))
  put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"))
  val put2 = new Put(Bytes.toBytes("row2"))
  put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"))
  val put3 = new Put(Bytes.toBytes("row3"))
  put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val3"))

  mutator.mutate(List(put1, put2, put3).asJava)

  val get = new Get(Bytes.toBytes("row1"))
  val res1 = table.get(get)
  println("Result: " + res1)

  mutator.flush()

  val res2 = table.get(get)
  println("Result: " + res2)

  mutator.close()
  table.close()
  connection.close()
  helper.close()
}
