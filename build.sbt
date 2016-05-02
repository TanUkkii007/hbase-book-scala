name := "hbase-book-scala"

val hbaseVersion = "1.1.4"

val hadoopVersion = "2.6.2"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "org.apache.hbase" % "hbase-common" % hbaseVersion,
  "org.apache.hbase" % "hbase-client" % hbaseVersion,
  "org.apache.hbase" % "hbase-protocol" % hbaseVersion,
  "org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion
)
