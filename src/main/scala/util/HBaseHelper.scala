package util

import java.io.Closeable
import scala.collection.immutable.IndexedSeq
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import scala.collection.JavaConverters._

case class CFValues(qual: String, ts: Long, value: String)

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

  def put(table: TableName, row: String, fam: String, qual: String, ts: Long, value: String): Unit = {
    val tbl = connection.getTable(table)
    val put = new Put(Bytes.toBytes(fam.toByte))
    put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), ts, Bytes.toBytes(value))
    tbl.put(put)
    tbl.close()
  }

  def put(table: TableName, rows: List[String], fams: List[String], cfValues: List[CFValues]) = {
    val tbl = connection.getTable(table)
    rows.foreach { r =>
      val put = new Put(Bytes.toBytes(r))
      fams.foreach { fam =>
        cfValues.foreach { cfv =>
          println("Adding: " + r + " " + fam + " " + cfv.qual + " " + cfv.ts + " " + cfv.value)
          put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(cfv.qual), cfv.ts, Bytes.toBytes(cfv.value))
        }
      }
      tbl.put(put)
    }
    tbl.close()
  }

  def dump(table: TableName, rows: List[String], fams: List[String] = Nil, quals: List[String] = Nil) = {
    val tbl = connection.getTable(table)
    val gets = rows.map { row =>
      val get = new Get(Bytes.toBytes(row))
      get.setMaxVersions()
      fams.foreach { fam =>
        quals.foreach { qual =>
          get.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual))
        }
      }
      get
    }
    val results = tbl.get(gets.asJava)
    results.foreach { result =>
      result.rawCells().foreach { cell =>
        println("Cell: " + cell + ", Value: " + Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength))
      }
    }
    tbl.close()
  }

  def fillTable(table: TableName, startRow: Int, endRow: Int, numCols: Int, pad: Int, setTimestamp: Boolean, random: Boolean, colfams: List[String]): Unit = {
    val tbl = connection.getTable(table)
    val rand = new Random()
    startRow to endRow foreach { row =>
      1 to numCols foreach { col =>
        val put = new Put(Bytes.toBytes(s"row-${padNum(row, pad)}"))
        colfams.foreach { cf =>
          val colName = s"col-${padNum(col, pad)}"
          val value = if (random) rand.nextInt(numCols).toString else padNum(row, pad) + "." + padNum(col, pad)
          if (setTimestamp)
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), Bytes.toBytes(value))
          else
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), Bytes.toBytes(value))
        }
        tbl.put(put)
      }
    }
    tbl.close()
  }

  def padNum(num: Int, pad: Int): String = if (pad > 0) num.toString.reverse.padTo(pad, '0').reverse.toString else num.toString

  def close(): Unit = connection.close()
}
