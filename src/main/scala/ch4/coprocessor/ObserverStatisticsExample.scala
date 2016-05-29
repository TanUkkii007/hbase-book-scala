package ch4.coprocessor

import java.io.IOException

import _root_.coprocessor.generated.ObserverStatisticsProtos.{StatisticsResponse, ObserverStatisticsService, StatisticsRequest}
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.{Coprocessor, TableName, HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.coprocessor.Batch
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback
import org.apache.hadoop.hbase.util.Bytes
import util.{LoanPattern, CFValues, HBaseHelper}
import util.ByteConverter._
import scala.collection.JavaConverters._

object ObserverStatisticsExample extends App with LoanPattern {

  val conf = HBaseConfiguration.create()
  val tableName = TableName.valueOf("testtable")
  val connection = ConnectionFactory.createConnection(conf)

  val helper = new HBaseHelper(conf)
  println("dropping table")
  helper.dropTable(tableName)
  println("creating table")
  helper.createTableWithCoprocessor(tableName, List("colfam1", "colfam2"), classOf[ObserverStatisticsEndpoint].getCanonicalName, Coprocessor.PRIORITY_USER, maxVersions = 3)
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


  tryWithResource {
    val table = connection.getTable(tableName)
    printStatistics(table, print = false, clear = true)

    println("Apply single put...")
    val put = new Put("row10".toUTF8Byte)
    put.addColumn("colfam1".toUTF8Byte, "qual10".toUTF8Byte, "val10".toUTF8Byte)
    table.put(put)
    printStatistics(table, print = true, clear = true)

    println("Do single get...")
    val get = new Get("row10".toUTF8Byte)
    get.addColumn("colfam1".toUTF8Byte, "qual10".toUTF8Byte)
    table.get(get)
    printStatistics(table, print = true, clear = true)

    println("Scan single row...")
    val scan = new Scan()
      .setStartRow("row10".toUTF8Byte)
      .setStopRow("row11".toUTF8Byte)
    val scanner = table.getScanner(scan)
    println("  -> after getScanner()...")
    printStatistics(table, print = true, clear = true)
    val result = scanner.next()
    println("  -> after next()...")
    printStatistics(table, print = true, clear = true)
    scanner.close()
    println("  -> after close()...")
    printStatistics(table, print = true, clear = true)

    println("Scan multiple rows...")
    val scan2 = new Scan()
    val scanner2 = table.getScanner(scan)
    println("  -> after getScanner()...")
    printStatistics(table, print = true, clear = true)
    val result2 = scanner2.next()
    printStatistics(table, print = true, clear = true)
    scanner2.close()
    printStatistics(table, print = true, clear = true)
    println("  -> after close()...")
    printStatistics(table, print = true, clear = true)

    println("Apply single put with mutateRow()...")
    val mutations = new RowMutations("row1".toUTF8Byte)
    val put2 = new Put("row1".toUTF8Byte)
    put2.addColumn("colfam1".toUTF8Byte, "qual10".toUTF8Byte, "val10".toUTF8Byte)
    mutations.add(put2)
    table.mutateRow(mutations)
    printStatistics(table, print = true, clear = true)

    println("Apply single column increment...")
    val increment = new Increment("row10".toUTF8Byte)
    increment.addColumn("colfam1".toUTF8Byte, "qual11".toUTF8Byte, 1)
    table.increment(increment)
    printStatistics(table, print = true, clear = true)

    println("Apply multi column increment...")
    val increment2 = new Increment("row10".toUTF8Byte)
    increment2.addColumn("colfam1".toUTF8Byte, "qual12".toUTF8Byte, 1)
    increment2.addColumn("colfam1".toUTF8Byte, "qual13".toUTF8Byte, 1)
    table.increment(increment2)
    printStatistics(table, print = true, clear = true)

    println("Apply single incrementColumnValue...")
    table.incrementColumnValue("row10".toUTF8Byte, "colfam1".toUTF8Byte, "qual12".toUTF8Byte, 1)
    printStatistics(table, print = true, clear = true)

    println("Call single exists()...")
    table.exists(get)
    printStatistics(table, print = true, clear = true)

    println("Apply single delete...")
    val delete = new Delete("row10".toUTF8Byte)
    delete.addColumn("colfam1".toUTF8Byte, "qual10".toUTF8Byte)
    printStatistics(table, print = true, clear = true)

    println("Apply single append...")
    val append = new Append("row10".toUTF8Byte)
    append.add("colfam1".toUTF8Byte, "qual15".toUTF8Byte, "-valnew".toUTF8Byte)
    table.append(append)
    printStatistics(table, print = true, clear = true)

    println("Apply checkAndPut (failing)...")
    val put3 = new Put("row10".toUTF8Byte)
    put3.addColumn("colfam1".toUTF8Byte, "qual17".toUTF8Byte, "val17".toUTF8Byte)
    printStatistics(table, print = true, clear = true)

    println("Apply checkAndPut (succeeding)...")
    val cap = table.checkAndPut("row10".toUTF8Byte, "colfam1".toUTF8Byte, "qual16".toUTF8Byte, null, put)
    println("  -> success: " + cap)
    printStatistics(table, print = true, clear = true)

    println("Apply checkAndDelete (failing)...")
    val delete2 = new Delete("row10".toUTF8Byte)
    delete2.addColumn("colfam1".toUTF8Byte, "qual17".toUTF8Byte)
    val cad = table.checkAndDelete("row10".toUTF8Byte, "colfam1".toUTF8Byte, "qual15".toUTF8Byte, null, delete2)
    println("  -> success: " + cad)
    printStatistics(table, print = true, clear = true)

    println("Apply checkAndDelete (succeeding)...")
    val cad2 = table.checkAndDelete("row10".toUTF8Byte, "colfam1".toUTF8Byte, "qual18".toUTF8Byte, null, delete2)
    println("  -> success: " + cad2)
    printStatistics(table, print = true, clear = true)

    println("Apply checkAndMutate (failing)...")
    val mutations2 = new RowMutations("row10".toUTF8Byte)
    val put4 = new Put("row10".toUTF8Byte)
    put4.addColumn("colfam1".toUTF8Byte, "qual20".toUTF8Byte, "val20".toUTF8Byte)
    val delete3 = new Delete("row10".toUTF8Byte)
    delete3.addColumn("colfam1".toUTF8Byte, "qual17".toUTF8Byte)
    mutations2.add(put4)
    mutations2.add(delete3)
    val cam = table.checkAndMutate(
      "row10".toUTF8Byte,
      "colfam1".toUTF8Byte,
      "qual10".toUTF8Byte,
      CompareFilter.CompareOp.GREATER,
      "val10".toUTF8Byte,
      mutations2
    )
    println("  -> success: " + cam)
    printStatistics(table, print = true, clear = true)

    println("Apply checkAndMutate (succeeding)...")
    val cam2 = table.checkAndMutate(
      "row10".toUTF8Byte,
      "colfam1".toUTF8Byte,
      "qual10".toUTF8Byte,
      CompareFilter.CompareOp.EQUAL,
      "val10".toUTF8Byte,
      mutations2
    )
    println("  -> success: " + cam2)
    printStatistics(table, print = true, clear = true)

    table
  } { table =>
    connection.close()
    table.close()
    admin.close()
    helper.close()
  }

  private def printStatistics(table: Table, print: Boolean, clear: Boolean): Unit = {
    val request = StatisticsRequest.newBuilder().setClear(clear).build()
    val results = table.coprocessorService(
      classOf[ObserverStatisticsService],
      HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW,
      (statistics: ObserverStatisticsService) => {
        val rpcCallback = new BlockingRpcCallback[StatisticsResponse]()
        statistics.getStatistics(null, request, rpcCallback)
        val response = rpcCallback.get()
        response.getAttributeList.asScala.map(pair => pair.getName -> pair.getValue).toMap
      }
    )

    if (print) {
      results.entrySet().asScala.foreach { entry =>
        println("Region: " + Bytes.toString(entry.getKey))
        entry.getValue.foreach {
          case (key, value) => println(s" $key: $value")
        }
      }
    }
  }

  implicit def batchCallConversion[T, R](f: T => R): Batch.Call[T, R] = new Batch.Call[T, R] {
    def call(instance: T): R = f(instance)
  }
}
