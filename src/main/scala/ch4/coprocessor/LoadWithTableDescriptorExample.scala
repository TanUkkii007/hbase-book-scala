package ch4.coprocessor

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase._
import _root_.util.HBaseHelper

object LoadWithTableDescriptorExample extends App {

  val conf = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(conf)

  val helper = new HBaseHelper(conf)

  val tableName = TableName.valueOf("testtable")

  helper.dropTable(tableName)

  val htd = new HTableDescriptor(tableName)
  htd.addFamily(new HColumnDescriptor("colfam1"))
  htd.setValue("COPROCESSOR$1", "|" + classOf[RegionObserverExample].getCanonicalName + "|" + Coprocessor.PRIORITY_USER)

  val admin = connection.getAdmin
  admin.createTable(htd)

  println(admin.getTableDescriptor(tableName))
  admin.close()
  connection.close()
  helper.close()
}
