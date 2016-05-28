package ch4.coprocessor

import java.io.IOException

import coprocessor.generated.RowCounterProtos
import coprocessor.generated.RowCounterProtos.RowCountService
import org.apache.hadoop.hbase.client.{Scan, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Coprocessor, TableName, HBaseConfiguration}
import util.{CFValues, HBaseHelper}
import util.ByteConverter._
import scala.collection.JavaConverters._

object EndpointProxyExample extends App {
  val conf = HBaseConfiguration.create()
  val tableName = TableName.valueOf("testtable")
  val connection = ConnectionFactory.createConnection(conf)

  val helper = new HBaseHelper(conf)
  println("dropping table")
  helper.dropTable(tableName)
  println("creating table")
  helper.createTableWithCoprocessor(tableName, List("colfam1", "colfam2"), classOf[RowCountEndpoint].getCanonicalName, Coprocessor.PRIORITY_USER, maxVersions = 3)
  println("Adding rows to table...")
  helper.put(
    tableName,
    List("row1", "row2", "row3", "row4", "row5"),
    List("colfam1", "colfam2"),
    List(CFValues("qual1", 1, "val1"), CFValues("qual2", 2, "val2"))
  )
  println("Before endpoint call...")
  helper.dump(tableName, List("row1", "row2", "row3", "row4", "row5"))

  val admin = connection.getAdmin

  println(admin.getTableDescriptor(tableName))

  try {
    admin.split(tableName, "row3".toUTF8Byte)
  } catch {
    case e: IOException => e.printStackTrace()
  }

  val table = connection.getTable(tableName)

  // wait for the split to be done
  while (admin.getTableRegions(tableName).size() < 2) {
    Thread.sleep(1000)
  }

  try {
    val hri = admin.getTableRegions(tableName).get(0)
    val scan = new Scan(hri.getStartKey, hri.getEndKey)
      .setMaxVersions()
    val scanner = table.getScanner(scan)
    scanner.asScala.foreach(result => println(s"Reasult: $result"))

    val channel = table.coprocessorService(Bytes.toBytes("row1"))
    val service = RowCountService.newBlockingStub(channel)
    val request = RowCounterProtos.CountRequest.newBuilder().build()
    val response = service.getCellCount(null, request)
    val cellsInRegion = if (response.hasCount) response.getCount else -1
    println("Region Cell Count: " + cellsInRegion)

    val request2 = RowCounterProtos.CountRequest.newBuilder().build()
    val response2 = service.getRowCount(null, request2)

    val rowsInRegion = if (response2.hasCount) response2.getCount else -1
    println("Region Row Count: " + rowsInRegion)
  } catch {
    case e: Throwable => e.printStackTrace()
  }

  connection.close()
  table.close()
  admin.close()
  helper.close()
}
