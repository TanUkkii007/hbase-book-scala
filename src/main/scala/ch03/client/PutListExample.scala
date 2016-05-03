package ch03.client

import org.apache.hadoop.hbase.client.{Put, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper
import scala.collection.JavaConverters._

object PutListExample extends App {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1"))

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val put1 = new Put(Bytes.toBytes("row1"))
  put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"))

  val put2 = new Put(Bytes.toBytes("row2"))
  put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"))

  val put3 = new Put(Bytes.toBytes("row2"))
  put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val3"))

  table.put(List(put1, put2, put3).asJava)

  table.close()
  connection.close()
  helper.close()
}
