package ch4.coprocessor

import java.io.IOException

import coprocessor.generated.RowCounterProtos.{CountResponse, RowCountService, CountRequest}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, Coprocessor, TableName, HBaseConfiguration}
import util.{CFValues, HBaseHelper}
import util.ByteConverter._
import scala.collection.JavaConverters._

object EndpointBatchExample extends App {

  val conf = HBaseConfiguration.create()
  val tableName = TableName.valueOf("testtable")
  val connection = ConnectionFactory.createConnection(conf)

  val helper = new HBaseHelper(conf)
  println("dropping table")
  helper.dropTable(tableName)
  println("creating table")
  helper.createTableWithCoprocessor(tableName, List("colfam1", "colfam2"), classOf[RowCountEndpoint].getCanonicalName, Coprocessor.PRIORITY_USER)
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

  // wait for the split to be done
  while (admin.getTableRegions(tableName).size() < 2) {
    Thread.sleep(1000)
  }

  val table = connection.getTable(tableName)

  try {
    val request = CountRequest.getDefaultInstance
    val results = table.batchCoprocessorService(
      RowCountService.getDescriptor.findMethodByName("getRowCount"),
      request,
      HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW,
      CountResponse.getDefaultInstance
    )

    val total = results.entrySet().asScala.foldLeft(0L) { (count, entry) =>
      val response = entry.getValue
      if (response.hasCount) response.getCount else 0
    }

    results.entrySet().asScala.foreach(entry => println("Region: " + Bytes.toString(entry.getKey()) + ", Count: " + entry.getValue()))
    println("Total Count: " + total)
  } catch {
    case e: Throwable => e.printStackTrace()
  }

  connection.close()
  table.close()
  admin.close()
  helper.close()

}
