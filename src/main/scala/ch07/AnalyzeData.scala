package ch07

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableMapper
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{Text, IntWritable}
import io.circe._
import io.circe.parser._
import org.apache.hadoop.mapreduce.Mapper
import scala.collection.JavaConverters._

object AnalyzeData {

  class AnalyzeMapper extends TableMapper[Text, IntWritable] {
    val ONE = new IntWritable(1)

    override def map(key: ImmutableBytesWritable, value: Result, context: Mapper[ImmutableBytesWritable, Result, Text, IntWritable]#Context): Unit = {
      context.getCounter("Counters", "ROWS").increment(1)
      var v: String = ""

      try {
        value.listCells().asScala.foreach { cell =>
          v = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          val author = for {
            json <- parse(v).right.toOption
            obj <- json.asObject
            authorJson <- obj("author")
            author <- authorJson.asString
          } yield author

          if (context.getConfiguration.get("conf.debug") != null) {
            println("Author: " + author.get)
          }
          context.write(new Text(author.get), ONE)
          context.getCounter("Counters", "VALID").increment(1)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
          System.err.println("Row: " + Bytes.toStringBinary(key.get()) + ", JSON: " + value)
          context.getCounter("Counters", "ERROR").increment(1)
      }
    }
  }

}
