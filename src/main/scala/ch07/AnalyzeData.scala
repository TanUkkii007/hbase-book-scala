package ch07

import java.lang.Iterable

import org.apache.commons.cli.{CommandLine, HelpFormatter, PosixParser, Options}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{KeyValue, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Scan, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableMapper}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{Text, IntWritable}
import io.circe._
import io.circe.parser._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Reducer, Mapper}
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.log4j.{Level, Logger}
import scala.collection.JavaConverters._

object AnalyzeData {
  val NAME = "AnalyzeData"

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

  class AnalyzeReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val count = values.asScala.size
      if (context.getConfiguration.get("conf.debug") != null) {
        println("Author: " + key.toString() + ", Count: " + count)
      }
      context.write(key, new IntWritable(count))
    }
  }

  def parseArgs(args: Array[String]): CommandLine = {
    val options = new Options()
    val o1 = new org.apache.commons.cli.Option("t", "table", true, "table to read from (must exist)")
    o1.setArgName("table-name")
    o1.setRequired(true)
    options.addOption(o1)
    val o2 = new org.apache.commons.cli.Option("c", "column", true, "column to read data from (must exist)")
    o2.setArgName("family:qualifier")
    o2.setRequired(true)
    options.addOption(o2)
    val o3 = new org.apache.commons.cli.Option("o", "output", true, "the directory to write to")
    o3.setArgName("path-in-HDFS")
    o3.setRequired(true)
    options.addOption(o3)
    options.addOption("d", "debug", false, "switch on DEBUG log level")
    val parser = new PosixParser()
    val cmd = try {
      parser.parse(options, args)
    } catch {
      case e: Exception =>
        println("ERROR: " + e.getMessage + "\n")
        val formatter = new HelpFormatter()
        formatter.printHelp(NAME + " ", options, true)
        sys.exit(-1)
    }
    if (cmd.hasOption("d")) {
      val log = Logger.getLogger("mapreduce")
      log.setLevel(Level.DEBUG)
      println("DEBUG ON")
    }
    cmd
  }

  def main(args: Array[String]) = {
    val conf = HBaseConfiguration.create()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    val cmd = parseArgs(otherArgs)
    if (cmd.hasOption("d")) {
      conf.set("conf.debug", "true")
    }
    val table = cmd.getOptionValue("t")
    val column = cmd.getOptionValue("c")
    val output = cmd.getOptionValue("o")

    val scan = new Scan()
    if (column != null) {
      val colKey = KeyValue.parseColumn(Bytes.toBytes(column))
      if (colKey.length > 1) {
        scan.addColumn(colKey(0), colKey(1))
      } else {
        scan.addFamily(colKey(0))
      }
    }

    val job = Job.getInstance(conf, "Analyze data in " + table)
    job.setJarByClass(getClass)
    TableMapReduceUtil.initTableMapperJob(table, scan, classOf[AnalyzeMapper], classOf[Text], classOf[IntWritable], job)
    job.setReducerClass(classOf[AnalyzeReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setNumReduceTasks(1)
    FileOutputFormat.setOutputPath(job, new Path(output))

    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }

}
