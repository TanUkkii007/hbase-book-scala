package ch4.coprocessor

import java.util

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.{Put, Get}
import org.apache.hadoop.hbase.coprocessor.{RegionCoprocessorEnvironment, ObserverContext, BaseRegionObserver}
import org.apache.hadoop.hbase.regionserver.HRegion
import org.apache.hadoop.hbase.util.Bytes

object RegionObserverExample {
  final val LOG = LogFactory.getLog(classOf[HRegion])

  final val FIXED_ROW = Bytes.toBytes("@@@GETTIME@@@")
}

class RegionObserverExample extends BaseRegionObserver {
  import RegionObserverExample._

  override def preGetOp(e: ObserverContext[RegionCoprocessorEnvironment], get: Get, results: util.List[Cell]): Unit = {
    LOG.debug("Got preGet for row: " + Bytes.toStringBinary(get.getRow))

    if (Bytes.equals(get.getRow, FIXED_ROW)) {
      val put = new Put(get.getRow)
      put.addColumn(FIXED_ROW, FIXED_ROW, Bytes.toBytes(System.currentTimeMillis()))
      val scanner = put.cellScanner()
      scanner.advance()
      val cell = scanner.current()

      LOG.debug("Had a match, adding fake cell: " + cell)

      results.add(cell)
    }
  }
}
