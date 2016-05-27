package ch4.coprocessor

import java.io.IOException

import _root_.coprocessor.generated.RowCounterProtos
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.coprocessor.Batch
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import _root_.util.{ByteConverter, CFValues, HBaseHelper}
import ByteConverter._
import scala.collection.JavaConverters._

object EndpointExample extends App {

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
    val request = RowCounterProtos.CountRequest.getDefaultInstance
    val results = table.coprocessorService(classOf[RowCounterProtos.RowCountService], null, null, (counter: RowCounterProtos.RowCountService) => {
      val rpcCallback = new BlockingRpcCallback[RowCounterProtos.CountResponse]()
      counter.getRowCount(null, request, rpcCallback)
      val response = rpcCallback.get()
      if (response.hasCount) response.getCount else 0L
    })

    results.entrySet().asScala.foreach { entry =>
      println("Region: " + Bytes.toString(entry.getKey()) + ", Count: " + entry.getValue())
    }
    println("Total Count: " + results.entrySet().asScala.map(_.getValue).sum)
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
