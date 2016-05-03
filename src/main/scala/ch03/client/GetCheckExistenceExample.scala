package ch03.client

import org.apache.hadoop.hbase.client.{Get, Put, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper
import scala.collection.JavaConverters._

object GetCheckExistenceExample extends App {
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

  val get1 = new Get(Bytes.toBytes("row"))
  get1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))
  get1.setCheckExistenceOnly(true)
  val result1 = table.get(get1)

  val value = result1.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))

  println(s"Get 1 Exists: ${result1.getExists}")
  println(s"Get 1 Size: ${result1.size()}")
  println(s"Get 1 Value: ${Bytes.toString(value)}")

  val get2 = new Get(Bytes.toBytes("row2"))
  get2.addFamily(Bytes.toBytes("colfam1"))
  get2.setCheckExistenceOnly(true)
  val result2 = table.get(get2)
  println(s"Get 2 Exists: ${result2.getExists}")
  println(s"Get 2 Size: ${result2.size()}")

  val get3 = new Get(Bytes.toBytes("row2"))
  get3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual9999"))
  get3.setCheckExistenceOnly(true)
  val result3 = table.get(get3)

  println(s"Get 3 Exists: ${result3.getExists}")
  println(s"Get 3 Size: ${result3.size()}")

  val get4 = new Get(Bytes.toBytes("row2"))
  get4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual9999"))
  get4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))
  get4.setCheckExistenceOnly(true)
  val result4 = table.get(get4)

  println(s"Get 4 Exists: ${result4.getExists}")
  println(s"Get 4 Size: ${result4.size()}")

  table.close()
  connection.close()
  helper.close()
}
