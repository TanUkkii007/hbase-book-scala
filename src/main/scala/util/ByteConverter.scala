package util

import org.apache.hadoop.hbase.util.Bytes

trait ByteConverter {
  implicit class StringToUTF8Byte(text: String) {
    def toUTF8Byte = Bytes.toBytes(text)
  }
}

object ByteConverter extends ByteConverter