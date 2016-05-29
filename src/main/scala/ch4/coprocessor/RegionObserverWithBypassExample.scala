package ch4.coprocessor

import java.util

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.{KeyValue, CellUtil, Cell}
import org.apache.hadoop.hbase.client.{Put, Get}
import org.apache.hadoop.hbase.coprocessor.{RegionCoprocessorEnvironment, ObserverContext, BaseRegionObserver}
import org.apache.hadoop.hbase.regionserver.HRegion
import org.apache.hadoop.hbase.util.Bytes

object RegionObserverWithBypassExample {
  final val LOG = LogFactory.getLog(classOf[HRegion])

  final val FIXED_ROW = Bytes.toBytes("@@@GETTIME@@@")
}

class RegionObserverWithBypassExample extends BaseRegionObserver {
  import RegionObserverWithBypassExample._

  override def preGetOp(e: ObserverContext[RegionCoprocessorEnvironment], get: Get, results: util.List[Cell]): Unit = {
    LOG.debug("Got preGet for row: " + Bytes.toStringBinary(get.getRow))

    if (Bytes.equals(get.getRow, FIXED_ROW)) {
      val time = System.currentTimeMillis()
      val cell = CellUtil.createCell(get.getRow, FIXED_ROW, FIXED_ROW, time, KeyValue.Type.Put.getCode, Bytes.toBytes(time))

      LOG.debug("Had a match, adding fake cell: " + cell)

      results.add(cell)

      e.bypass()
    }
  }
}
