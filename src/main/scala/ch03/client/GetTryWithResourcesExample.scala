package ch03.client

import org.apache.hadoop.hbase.client.{Get, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import util.{LoanPattern, HBaseHelper}

object GetTryWithResourcesExample extends App with LoanPattern {

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)

  if (!helper.existsTable(TableName.valueOf("testtable"))) {
    helper.createTable(TableName.valueOf("testtable"), List("colfam1"))
  }

  tryWithResource {
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf("testtable"))

    val get = new Get(Bytes.toBytes("row1"))
    get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))
    val result = table.get(get)
    val value = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))
    println("Value: " + Bytes.toString(value))

    Thread.sleep(1000)

    connection
  }(_.close())

  helper.close()
}
