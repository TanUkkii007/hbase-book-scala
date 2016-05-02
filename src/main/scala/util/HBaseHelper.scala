package util

import java.io.Closeable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}

class HBaseHelper(val configuration: Configuration) extends Closeable {

  val connection: Connection = ConnectionFactory.createConnection(configuration)

  val admin: Admin = connection.getAdmin

  def existsTable(table: TableName): Boolean = admin.tableExists(table)

  def createTable(table: TableName, colfams: Seq[String], maxVersions: Int = 1, splitKeysOpt: Option[Array[Array[Byte]]] = None) = {
    val desc = new HTableDescriptor(table)
    colfams.foreach { cf =>
      val coldef = new HColumnDescriptor(cf)
      coldef.setMaxVersions(maxVersions)
      desc.addFamily(coldef)
    }
    splitKeysOpt.fold(admin.createTable(desc))(splitKeys => admin.createTable(desc, splitKeys))
  }

  def disableTable(table: TableName): Unit = admin.disableTable(table)

  def dropTable(table: TableName): Unit = {
    if (existsTable(table)) {
      if (admin.isTableEnabled(table)) disableTable(table)
      admin.deleteTable(table)
    }
  }

  def close(): Unit = connection.close()
}
