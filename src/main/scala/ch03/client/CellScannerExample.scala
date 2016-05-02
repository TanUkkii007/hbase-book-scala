package ch03.client

import org.apache.hadoop.hbase.CellScanner
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object CellScannerExample extends App {
  val put = new Put(Bytes.toBytes("testrow"))
  put.addColumn(Bytes.toBytes("fam-1"), Bytes.toBytes("qual-1"), Bytes.toBytes("val-1"))
  put.addColumn(Bytes.toBytes("fam-1"), Bytes.toBytes("qual-2"), Bytes.toBytes("val-2"))
  put.addColumn(Bytes.toBytes("fam-2"), Bytes.toBytes("qual-3"), Bytes.toBytes("val-3"))

  val scanner: CellScanner = put.cellScanner()
  while (scanner.advance()) {
    val cell = scanner.current()
    println("Cell: " + cell)
  }
}
