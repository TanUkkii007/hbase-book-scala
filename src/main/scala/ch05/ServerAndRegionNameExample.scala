package ch05

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HTableDescriptor, HColumnDescriptor, TableName, HBaseConfiguration}
import util.HBaseHelper

object ServerAndRegionNameExample extends App {
  val conf = HBaseConfiguration.create()
  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")
  helper.dropTable(tableName)
  val connection = ConnectionFactory.createConnection(conf)
  val admin = connection.getAdmin

  val coldef1 = new HColumnDescriptor("colfam1")
  val desc = new HTableDescriptor(tableName)
    .addFamily(coldef1)
    .setValue("Description", "Chapter 5 - ServerAndRegionNameExample")

  val regions = Array(
    Bytes.toBytes("ABC"),
    Bytes.toBytes("DEF"),
    Bytes.toBytes("GHI"),
    Bytes.toBytes("KLM"),
    Bytes.toBytes("OPQ"),
    Bytes.toBytes("TUV")
  )

  admin.createTable(desc, regions)

  val locator = connection.getRegionLocator(tableName)
  val location = locator.getRegionLocation(Bytes.toBytes("Foo"))
  val info = location.getRegionInfo

  println("Region Name: " + info.getRegionNameAsString)
  println("Server Name: " + location.getServerName)

  locator.close()
  admin.close()
  connection.close()

  helper.close()
}
