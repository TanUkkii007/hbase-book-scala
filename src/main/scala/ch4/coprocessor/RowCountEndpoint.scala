package ch4.coprocessor

import java.io.IOException

import com.google.protobuf.{Service, RpcCallback, RpcController}
import coprocessor.generated.RowCounterProtos
import coprocessor.generated.RowCounterProtos.{CountResponse, CountRequest, RowCountService}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.{Filter, FirstKeyOnlyFilter}
import org.apache.hadoop.hbase.protobuf.ResponseConverter
import org.apache.hadoop.hbase.{CellUtil, Cell, CoprocessorEnvironment, Coprocessor}
import org.apache.hadoop.hbase.coprocessor.{CoprocessorException, RegionCoprocessorEnvironment, CoprocessorService}
import util.LoanPattern
import scala.collection.JavaConverters._

class RowCountEndpoint extends RowCountService with Coprocessor with CoprocessorService with LoanPattern {

  var env: RegionCoprocessorEnvironment = _

  def start(env: CoprocessorEnvironment): Unit = env match {
    case rcEnv: RegionCoprocessorEnvironment => this.env = rcEnv
    case _ => throw new CoprocessorException("Must be loaded on a table region!")
  }

  def stop(env: CoprocessorEnvironment): Unit = {}

  def getService: Service = this

  def getRowCount(controller: RpcController, request: CountRequest, done: RpcCallback[CountResponse]): Unit = {
    val response = try {
      val count = getCount(Some(new FirstKeyOnlyFilter), countCells = false)
      val response = RowCounterProtos.CountResponse.newBuilder()
        .setCount(count).build()
      Some(response)
    } catch {
      case ioe: IOException => ResponseConverter.setControllerException(controller, ioe); None
    }
    done.run(response.get)
  }

  def getCellCount(controller: RpcController, request: CountRequest, done: RpcCallback[CountResponse]): Unit = {
    val response = try {
      val count = getCount(None, countCells = true)
      val response = RowCounterProtos.CountResponse.newBuilder()
        .setCount(count).build()
      Some(response)
    } catch {
      case ioe: IOException => ResponseConverter.setControllerException(controller, ioe); None
    }
    done.run(response.get)
  }

  private def getCount(filter: Option[Filter], countCells: Boolean): Long = {
    var count = 0L
    val scan = new Scan()
    scan.setMaxVersions(1)
    filter.foreach(f => scan.setFilter(f))

    tryWithResource(env.getRegion.getScanner(scan)) { scanner =>
      val results = new java.util.ArrayList[Cell]()
      var hasMore = true
      var lastRow: Option[Array[Byte]] = None
      while (hasMore) {
        hasMore = scanner.next(results)
        results.asScala.foreach { cell =>
          if (!countCells) {
            if (lastRow.isEmpty || !CellUtil.matchingRow(cell, lastRow.get)) {
              lastRow = Some(CellUtil.cloneRow(cell))
              count += 1
            }
          } else count += 1
        }
        results.clear()
      }
    }
    count
  }
}
