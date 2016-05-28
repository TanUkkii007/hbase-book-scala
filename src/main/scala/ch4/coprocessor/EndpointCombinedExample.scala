package ch4.coprocessor

import java.io.IOException

import coprocessor.generated.RowCounterProtos
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.coprocessor.Batch
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, Coprocessor, TableName, HBaseConfiguration}
import util.{CFValues, HBaseHelper}
import util.ByteConverter._
import scala.collection.JavaConverters._


object EndpointCombinedExample extends App {
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

  val table = connection.getTable(tableName)

  try {
    val request = RowCounterProtos.CountRequest.getDefaultInstance
    val results = table.coprocessorService(
      classOf[RowCounterProtos.RowCountService],
      HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW,
      (counter: RowCounterProtos.RowCountService) => {
        val rowCallback = new BlockingRpcCallback[RowCounterProtos.CountResponse]()
        counter.getRowCount(null, request, rowCallback)

        val cellCallback = new BlockingRpcCallback[RowCounterProtos.CountResponse]()
        counter.getCellCount(null, request, cellCallback)

        val rowResponse = rowCallback.get()
        val rowCount = if (rowResponse.hasCount) rowResponse.getCount else 0L

        val cellResponse = cellCallback.get()
        val cellCount = if (cellResponse.hasCount) cellResponse.getCount else 0L

        new org.apache.hadoop.hbase.util.Pair(rowCount, cellCount)
      }
    )

    val (totalRows, totalKeyValues) = results.entrySet().asScala.foldLeft((0L, 0L)) { (count, entry) =>
      (count._1 + entry.getValue.getFirst, count._2 + entry.getValue.getSecond)
    }

    results.entrySet().asScala.foreach(entry => "Region: " + Bytes.toString(entry.getKey()) + ", Count: " + entry.getValue())

    System.out.println("Total Row Count: " + totalRows)
    System.out.println("Total Cell Count: " + totalKeyValues)
  } catch {
    case e: Throwable => e.printStackTrace()
  }

  connection.close()
  table.close()
  admin.close()
  helper.close()

  implicit def batchCallConversion[T, R](f: T => R): Batch.Call[T, R] = new Batch.Call[T, R] {
    def call(instance: T): R = f(instance)
  }
}
