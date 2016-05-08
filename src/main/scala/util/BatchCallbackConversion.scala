package util

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.coprocessor.Batch
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback

trait BatchCallbackConversion {
  implicit def toBatchCallback(f: (Array[Byte], Array[Byte], Result) => Unit): Callback[Result] = new Batch.Callback[Result] {
    def update(region: Array[Byte], row: Array[Byte], result: Result): Unit = f(region, row, result)
  }
}

object BatchCallbackConversion extends BatchCallbackConversion