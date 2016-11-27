package ch07

import org.apache.commons.cli.{CommandLine, HelpFormatter, PosixParser, Options}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.{Put, Mutation}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{Writable, Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper}
import org.apache.hadoop.util.GenericOptionsParser
import org.slf4j.Logger
import scala.collection.JavaConverters._

object ImportFromFile {

  private final val log = LogFactory.getLog(this.getClass)

  val NAME = "ImportFromFile"

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
        context.getCounter("Counters", "LINES").increment(1)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  private def parseArgs(args: Array[String]): CommandLine = {
    val options = new Options()
    val o1 = new org.apache.commons.cli.Option("t", "table", true, "table to import into (must exist)")
    o1.setArgName("table-name")
    o1.setRequired(true)
    options.addOption(o1)
    val o2 = new org.apache.commons.cli.Option("c", "column", true, "column to store row data into (must exist)")
    o2.setArgName("family:qualifier")
    o2.setRequired(true)
    options.addOption(o2)
    val o3 = new org.apache.commons.cli.Option("i", "input", true, "the directory or file to read from")
    o3.setArgName("path-in-HDFS")
    o3.setRequired(true)
    options.addOption(o3)
    val o4 = new org.apache.commons.cli.Option("d", "debug", false, "switch on DEBUG log level")
    options.addOption(o4)
    val parser = new PosixParser()
    val cmd: CommandLine = try {
      parser.parse(options, args)
    } catch {
      case e: Exception =>
        System.err.println("ERROR: " + e.getMessage() + "\n")
        val formatter = new HelpFormatter()
        formatter.printHelp(NAME + " ", options, true)
        sys.exit(-1)
    }

    if (cmd.hasOption("d")) {

    }
    cmd
  }

  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    val cmd = parseArgs(otherArgs)

    if (cmd.hasOption("d")) {
      conf.set("conf.debug", "true")
    }

    val table = cmd.getOptionValue("t")
    val input = cmd.getOptionValue("i")
    val column = cmd.getOptionValue("c")
    conf.set("conf.column", column)

    val job = Job.getInstance(conf, s"Import from file $input into table $table")
    job.setJarByClass(getClass)
    job.setMapperClass(classOf[ImportMapper])
    job.setOutputFormatClass(classOf[TableOutputFormat[_]])
    job.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, table)
    job.setOutputKeyClass(classOf[Writable])
    job.setNumReduceTasks(0)
    FileInputFormat.addInputPath(job, new Path(input))

    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }

}
