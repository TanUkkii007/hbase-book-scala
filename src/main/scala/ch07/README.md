# How to run Ch07 example

## Preparation

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


## Building jars

```
sbt assembly
```

## Running ImportFromFile

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


## Running AnalyzeData

```sh
bin/hadoop jar hbase-book-scala/target/scala-2.11/hbase-book-scala-assembly-0.1-SNAPSHOT.jar ch07.AnalyzeData -t testtable -c data:json -o analyze1
```

```
16/11/28 23:25:01 INFO mapreduce.JobSubmitter: number of splits:1
16/11/28 23:25:01 INFO Configuration.deprecation: io.bytes.per.checksum is deprecated. Instead, use dfs.bytes-per-checksum
16/11/28 23:25:01 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_local4973542_0001
16/11/28 23:25:02 INFO mapreduce.Job: The url to track the job: http://localhost:8080/
16/11/28 23:25:02 INFO mapreduce.Job: Running job: job_local4973542_0001
16/11/28 23:25:02 INFO mapred.LocalJobRunner: OutputCommitter set in config null
16/11/28 23:25:02 INFO output.FileOutputCommitter: File Output Committer Algorithm version is 1
16/11/28 23:25:02 INFO mapred.LocalJobRunner: OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
16/11/28 23:25:02 INFO mapred.LocalJobRunner: Waiting for map tasks
16/11/28 23:25:02 INFO mapred.LocalJobRunner: Starting task: attempt_local4973542_0001_m_000000_0
16/11/28 23:25:02 INFO output.FileOutputCommitter: File Output Committer Algorithm version is 1
16/11/28 23:25:02 INFO util.ProcfsBasedProcessTree: ProcfsBasedProcessTree currently is supported only on Linux.
16/11/28 23:25:02 INFO mapred.Task:  Using ResourceCalculatorProcessTree : null
16/11/28 23:25:02 INFO mapred.MapTask: Processing split: HBase table split(table name: testtable, scan: , start row: , end row: , region location: 192.168.0.15)
16/11/28 23:25:02 INFO mapreduce.TableInputFormatBase: Input split length: 1 M bytes.
16/11/28 23:25:02 INFO mapred.MapTask: (EQUATOR) 0 kvi 26214396(104857584)
16/11/28 23:25:02 INFO mapred.MapTask: mapreduce.task.io.sort.mb: 100
16/11/28 23:25:02 INFO mapred.MapTask: soft limit at 83886080
16/11/28 23:25:02 INFO mapred.MapTask: bufstart = 0; bufvoid = 104857600
16/11/28 23:25:02 INFO mapred.MapTask: kvstart = 26214396; length = 6553600
16/11/28 23:25:02 INFO mapred.MapTask: Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
16/11/28 23:25:03 INFO mapreduce.Job: Job job_local4973542_0001 running in uber mode : false
16/11/28 23:25:03 INFO mapreduce.Job:  map 0% reduce 0%
16/11/28 23:25:03 INFO mapred.LocalJobRunner: 
16/11/28 23:25:03 INFO mapred.MapTask: Starting flush of map output
16/11/28 23:25:03 INFO mapred.MapTask: Spilling map output
16/11/28 23:25:03 INFO mapred.MapTask: bufstart = 0; bufend = 14004; bufvoid = 104857600
16/11/28 23:25:03 INFO mapred.MapTask: kvstart = 26214396(104857584); kvend = 26210428(104841712); length = 3969/6553600
16/11/28 23:25:03 INFO mapred.MapTask: Finished spill 0
16/11/28 23:25:03 INFO mapred.Task: Task:attempt_local4973542_0001_m_000000_0 is done. And is in the process of committing
16/11/28 23:25:03 INFO mapred.LocalJobRunner: map
16/11/28 23:25:03 INFO mapred.Task: Task 'attempt_local4973542_0001_m_000000_0' done.
16/11/28 23:25:03 INFO mapred.LocalJobRunner: Finishing task: attempt_local4973542_0001_m_000000_0
16/11/28 23:25:03 INFO mapred.LocalJobRunner: map task executor complete.
16/11/28 23:25:03 INFO mapred.LocalJobRunner: Waiting for reduce tasks
16/11/28 23:25:03 INFO mapred.LocalJobRunner: Starting task: attempt_local4973542_0001_r_000000_0
16/11/28 23:25:03 INFO output.FileOutputCommitter: File Output Committer Algorithm version is 1
16/11/28 23:25:03 INFO util.ProcfsBasedProcessTree: ProcfsBasedProcessTree currently is supported only on Linux.
16/11/28 23:25:03 INFO mapred.Task:  Using ResourceCalculatorProcessTree : null
16/11/28 23:25:03 INFO mapred.ReduceTask: Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@4baad975
16/11/28 23:25:03 INFO reduce.MergeManagerImpl: MergerManager: memoryLimit=334338464, maxSingleShuffleLimit=83584616, mergeThreshold=220663392, ioSortFactor=10, memToMemMergeOutputsThreshold=10
16/11/28 23:25:03 INFO reduce.EventFetcher: attempt_local4973542_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
16/11/28 23:25:03 INFO reduce.LocalFetcher: localfetcher#1 about to shuffle output of map attempt_local4973542_0001_m_000000_0 decomp: 15992 len: 15996 to MEMORY
16/11/28 23:25:03 INFO reduce.InMemoryMapOutput: Read 15992 bytes from map-output for attempt_local4973542_0001_m_000000_0
16/11/28 23:25:03 INFO reduce.MergeManagerImpl: closeInMemoryFile -> map-output of size: 15992, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->15992
16/11/28 23:25:03 INFO reduce.EventFetcher: EventFetcher is interrupted.. Returning
16/11/28 23:25:03 INFO mapred.LocalJobRunner: 1 / 1 copied.
16/11/28 23:25:03 INFO reduce.MergeManagerImpl: finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
16/11/28 23:25:03 INFO mapred.Merger: Merging 1 sorted segments
16/11/28 23:25:03 INFO mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 15985 bytes
16/11/28 23:25:03 INFO reduce.MergeManagerImpl: Merged 1 segments, 15992 bytes to disk to satisfy reduce memory limit
16/11/28 23:25:03 INFO reduce.MergeManagerImpl: Merging 1 files, 15996 bytes from disk
16/11/28 23:25:03 INFO reduce.MergeManagerImpl: Merging 0 segments, 0 bytes from memory into reduce
16/11/28 23:25:03 INFO mapred.Merger: Merging 1 sorted segments
16/11/28 23:25:03 INFO mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 15985 bytes
16/11/28 23:25:03 INFO mapred.LocalJobRunner: 1 / 1 copied.
16/11/28 23:25:03 INFO Configuration.deprecation: mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
16/11/28 23:25:03 INFO mapred.Task: Task:attempt_local4973542_0001_r_000000_0 is done. And is in the process of committing
16/11/28 23:25:03 INFO mapred.LocalJobRunner: 1 / 1 copied.
16/11/28 23:25:03 INFO mapred.Task: Task attempt_local4973542_0001_r_000000_0 is allowed to commit now
16/11/28 23:25:03 INFO output.FileOutputCommitter: Saved output of task 'attempt_local4973542_0001_r_000000_0' to hdfs://localhost/user/yusuke/analyze1/_temporary/0/task_local4973542_0001_r_000000
16/11/28 23:25:03 INFO mapred.LocalJobRunner: reduce > reduce
16/11/28 23:25:03 INFO mapred.Task: Task 'attempt_local4973542_0001_r_000000_0' done.
16/11/28 23:25:03 INFO mapred.LocalJobRunner: Finishing task: attempt_local4973542_0001_r_000000_0
16/11/28 23:25:03 INFO mapred.LocalJobRunner: reduce task executor complete.
16/11/28 23:25:04 INFO mapreduce.Job:  map 100% reduce 100%
16/11/28 23:25:04 INFO mapreduce.Job: Job job_local4973542_0001 completed successfully
16/11/28 23:25:04 INFO mapreduce.Job: Counters: 37
	File System Counters
		FILE: Number of bytes read=368063172
		FILE: Number of bytes written=371634784
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=0
		HDFS: Number of bytes written=11944
		HDFS: Number of read operations=7
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=4
	Map-Reduce Framework
		Map input records=993
		Map output records=993
		Map output bytes=14004
		Map output materialized bytes=15996
		Input split bytes=76
		Combine input records=0
		Combine output records=0
		Reduce input groups=987
		Reduce shuffle bytes=15996
		Reduce input records=993
		Reduce output records=987
		Spilled Records=1986
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=8
		Total committed heap usage (bytes)=588775424
	Counters
		ROWS=993
		VALID=993
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=0
	File Output Format Counters 
		Bytes Written=11944
```