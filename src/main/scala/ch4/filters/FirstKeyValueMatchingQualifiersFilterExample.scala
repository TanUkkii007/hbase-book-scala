package ch4.filters

import org.apache.hadoop.hbase.client.{Scan, ConnectionFactory}
import org.apache.hadoop.hbase.filter.FirstKeyValueMatchingQualifiersFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import util.{ByteConverter, HBaseHelper}
import scala.collection.JavaConverters._

object FirstKeyValueMatchingQualifiersFilterExample extends App with ByteConverter {

  val tableName = TableName.valueOf("testtable")

  val conf = HBaseConfiguration.create()

  val helper = new HBaseHelper(conf)
  helper.dropTable(tableName)
  helper.createTable(tableName, List("colfam1"))
  println("Adding rows to table...")
  helper.fillTableRandom(tableName,
    1, 50, 0, //row
    1, 10, 0, //col
    0, 100, 0, //val
    setTimestamp = true, List("colfam1")
  )

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(tableName)

  val quals = Set(
    "col-2".toUTF8Byte,
    "col-4".toUTF8Byte,
    "col-6".toUTF8Byte,
    "col-8".toUTF8Byte
  )
  val filter = new FirstKeyValueMatchingQualifiersFilter(quals.asJava)

  val scan = new Scan()
  scan.setFilter(filter)
  val scanner = table.getScanner(scan)

  println("Results of scan:")

  val rowCount = scanner.asScala.foldLeft(0) { (acc, result) =>
    result.rawCells().foreach { cell =>
      println("Cell: " + cell + ", Value: " + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()))
    }
    acc + 1
  }
  println("Total num of rows: " + rowCount)
  scanner.close()

  table.close()
  connection.close()
  helper.close()
}
