## How to run `ch07.ImportFromFile` example

```sh
sbin/start-dfs.sh
```

```
<!-- hbase-site.xml -->

<property>
  <name>hbase.rootdir</name>
  <value>hdfs://localhost:8020/hbase</value>
</property>
```

```sh
bin/start-hbase.sh
```

```sh
echo 'create "testtable", "data"' | bin/hbase shell
```

```sh
bin/hadoop fs -mkdir hdfs://localhost/hbase-books/ch07
```

```sh
bin/hadoop fs -put hbase-book-scala/src/main/scala/ch07/test-data.txt /hbase-books/ch07/
```

```sh
bin/hadoop jar hbase-book-scala/target/scala-2.11/hbase-book-scala-assembly-0.1-SNAPSHOT.jar ch07.ImportFromFile -t testtable -i /hbase-books/ch07/test-data.txt -c data:json
```

```
16/11/25 23:48:29 INFO jvm.JvmMetrics: Initializing JVM Metrics with processName=JobTracker, sessionId=
16/11/25 23:48:30 INFO input.FileInputFormat: Total input paths to process : 1
16/11/25 23:48:30 INFO mapreduce.JobSubmitter: number of splits:1
16/11/25 23:48:30 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_local729399385_0001
16/11/25 23:48:31 INFO mapreduce.Job: The url to track the job: http://localhost:8080/
16/11/25 23:48:31 INFO mapreduce.Job: Running job: job_local729399385_0001
16/11/25 23:48:31 INFO mapred.LocalJobRunner: OutputCommitter set in config null
16/11/25 23:48:31 INFO mapred.LocalJobRunner: OutputCommitter is org.apache.hadoop.hbase.mapreduce.TableOutputCommitter
16/11/25 23:48:31 INFO mapred.LocalJobRunner: Waiting for map tasks
16/11/25 23:48:31 INFO mapred.LocalJobRunner: Starting task: attempt_local729399385_0001_m_000000_0
16/11/25 23:48:31 INFO util.ProcfsBasedProcessTree: ProcfsBasedProcessTree currently is supported only on Linux.
16/11/25 23:48:31 INFO mapred.Task:  Using ResourceCalculatorProcessTree : null
16/11/25 23:48:31 INFO mapred.MapTask: Processing split: hdfs://localhost/hbase-books/ch07/test-data.txt:0+1015410

16/11/25 23:48:31 INFO mapreduce.TableOutputFormat: Created table instance for testtable
16/11/25 23:48:31 INFO mapred.LocalJobRunner: 
16/11/25 23:48:32 INFO mapreduce.Job: Job job_local729399385_0001 running in uber mode : false
16/11/25 23:48:32 INFO mapreduce.Job:  map 0% reduce 0%
16/11/25 23:48:32 INFO client.ConnectionManager$HConnectionImplementation: Closing zookeeper sessionid=0x1589bd795030009
16/11/25 23:48:32 INFO zookeeper.ZooKeeper: Session: 0x1589bd795030009 closed
16/11/25 23:48:32 INFO zookeeper.ClientCnxn: EventThread shut down
16/11/25 23:48:32 INFO mapred.Task: Task:attempt_local729399385_0001_m_000000_0 is done. And is in the process of committing
16/11/25 23:48:32 INFO mapred.LocalJobRunner: map
16/11/25 23:48:32 INFO mapred.Task: Task 'attempt_local729399385_0001_m_000000_0' done.
16/11/25 23:48:32 INFO mapred.LocalJobRunner: Finishing task: attempt_local729399385_0001_m_000000_0
16/11/25 23:48:32 INFO mapred.LocalJobRunner: map task executor complete.
16/11/25 23:48:33 INFO mapreduce.Job:  map 100% reduce 0%
16/11/25 23:48:33 INFO mapreduce.Job: Job job_local729399385_0001 completed successfully
16/11/25 23:48:33 INFO mapreduce.Job: Counters: 21
	File System Counters
		FILE: Number of bytes read=68948930
		FILE: Number of bytes written=69824822
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=1015410
		HDFS: Number of bytes written=0
		HDFS: Number of read operations=3
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=0
	Map-Reduce Framework
		Map input records=993
		Map output records=993
		Input split bytes=112
		Spilled Records=0
		Failed Shuffles=0
		Merged Map outputs=0
		GC time elapsed (ms)=3
		Total committed heap usage (bytes)=217055232
	Counters
		LINES=993
	File Input Format Counters 
		Bytes Read=1015410
	File Output Format Counters 
		Bytes Written=0
```