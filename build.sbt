name := "hbase-book-scala"

val hbaseVersion = "1.1.4"

val hadoopVersion = "2.6.2"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "org.apache.hbase" % "hbase-common" % hbaseVersion,
  "org.apache.hbase" % "hbase-client" % hbaseVersion,
  "org.apache.hbase" % "hbase-server" % hbaseVersion,
  "org.apache.hbase" % "hbase-protocol" % hbaseVersion,
  "org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion,
  "org.apache.zookeeper" % "zookeeper" % "3.4.6",
  "com.google.protobuf" % "protobuf-java" % "2.5.0",
  "org.apache.htrace" % "htrace-core" % "3.1.0-incubating",
  "io.netty" % "netty-all" % "4.0.23.Final",
  "com.google.guava" % "guava" % "12.0.1",
  "com.yammer.metrics" % "metrics-core" % "2.2.0"
)
