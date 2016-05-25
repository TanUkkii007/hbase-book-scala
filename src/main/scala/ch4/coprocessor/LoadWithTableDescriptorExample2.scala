package ch4.coprocessor

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.ConnectionFactory
import _root_.util.HBaseHelper

object LoadWithTableDescriptorExample2 extends App {

  val conf = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(conf)

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)

  val htd = new HTableDescriptor(tableName)
    .addFamily(new HColumnDescriptor("colfam1"))
    .addCoprocessor(classOf[RegionObserverExample].getCanonicalName, null, Coprocessor.PRIORITY_USER, null)

  val admin = connection.getAdmin
  admin.createTable(htd)

  println(admin.getTableDescriptor(tableName))
  admin.close()
  connection.close()
  helper.close()
}
