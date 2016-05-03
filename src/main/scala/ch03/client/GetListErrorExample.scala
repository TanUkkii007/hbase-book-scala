package ch03.client

import org.apache.hadoop.hbase.client.{Get, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.HBaseHelper
import scala.collection.JavaConverters._

object GetListErrorExample extends App {
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

  val get4 = new Get(row2)
  get4.addColumn(Bytes.toBytes("BOGUS"), qf2)

  try {
    val results = table.get(List(get1, get2, get3, get4).asJava)
    println("Result count: " + results.length)
  } finally {
    table.close()
    connection.close()
    helper.close()
  }

}
