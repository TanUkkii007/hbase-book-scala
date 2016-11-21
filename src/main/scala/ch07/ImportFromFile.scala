package ch07

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{Put, Mutation}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.Mapper
import scala.collection.JavaConverters._

object ImportFromFile {

  private final val log = LogFactory.getLog(this.getClass)

  object Counters extends Enumeration {
    val LINES = Value("LINES")
  }

  class ImportMapper extends Mapper[LongWritable, Text, ImmutableBytesWritable, Mutation] {
    private var family: Array[Byte] = _
    private var qualifier: Array[Byte] = _

    override def setup(context: Mapper[LongWritable, Text, ImmutableBytesWritable, Mutation]#Context): Unit = {
      val column = context.getConfiguration.get("conf.column")
      val colKey = KeyValue.parseColumn(Bytes.toBytes(column))
      family = colKey(0)
      if (colKey.length > 1) {
        qualifier = colKey(1)
      }
    }

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, ImmutableBytesWritable, Mutation]#Context): Unit = {
      try {
        val lineString = value.toString
        val rowKey = DigestUtils.md5(lineString)
        val put = new Put(rowKey)
        put.addColumn(family, qualifier, Bytes.toBytes(lineString))
        context.write(new ImmutableBytesWritable(rowKey), put)

      }
      super.map(key, value, context)
    }
  }

}
