package ch4.coprocessor

import java.io.IOException
import java.util

import _root_.coprocessor.generated.ObserverStatisticsProtos
import _root_.coprocessor.generated.ObserverStatisticsProtos.{StatisticsResponse, StatisticsRequest}
import com.google.common.collect.ImmutableList
import com.google.protobuf.{RpcCallback, RpcController, Service}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hbase.coprocessor.RegionObserver.MutationType
import org.apache.hadoop.hbase.filter.ByteArrayComparable
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.io.{Reference, FSDataInputStreamWrapper}
import org.apache.hadoop.hbase.io.hfile.CacheConfig
import org.apache.hadoop.hbase.protobuf.ResponseConverter
import org.apache.hadoop.hbase.regionserver.Region.Operation
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader
import org.apache.hadoop.hbase.regionserver.wal.{WALEdit, HLogKey}
import org.apache.hadoop.hbase.util.Pair
import org.apache.hadoop.hbase.wal.WALKey
import org.apache.hadoop.hbase.{CoprocessorEnvironment, Cell, HRegionInfo, Coprocessor}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.coprocessor._
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest
import org.apache.hadoop.hbase.regionserver._

class ObserverStatisticsEndpoint extends ObserverStatisticsProtos.ObserverStatisticsService
  with Coprocessor with CoprocessorService with RegionObserver {

  private var env: RegionCoprocessorEnvironment = _
  private var stats: Map[String, Int] = Map()

  def start(env: CoprocessorEnvironment): Unit = env match {
    case rcEnv: RegionCoprocessorEnvironment => this.env = rcEnv
    case _ => new CoprocessorException("Must be loaded on a table region!")
  }

  def stop(env: CoprocessorEnvironment): Unit = {}

  def getService: Service = this

  def getStatistics(controller: RpcController, request: StatisticsRequest, done: RpcCallback[StatisticsResponse]): Unit = {
    val response = try {
      val builder = ObserverStatisticsProtos.StatisticsResponse.newBuilder()
      val pair = ObserverStatisticsProtos.NameInt32Pair.newBuilder()
      stats.foreach {
        case (key, value) => {
          pair.setName(key)
          pair.setValue(value)
          builder.addAttribute(pair.build())
        }
      }
      val response = builder.build()
      if (request.hasClear && request.getClear) synchronized {
        stats = Map.empty
      }
      Some(response)
    } catch {
      case e: Exception => ResponseConverter.setControllerException(controller, new IOException(e)); None
    }
    done.run(response.get)
  }

  private def addCallCount(call: String): Unit = synchronized {
    val count = stats.get(call) match {
      case Some(c) => c + 1
      case None => 1
    }
    stats = stats + (call -> count)
  }


  def preOpen(c: ObserverContext[RegionCoprocessorEnvironment]): Unit = addCallCount("preOpen`")

  def postOpen(c: ObserverContext[RegionCoprocessorEnvironment]): Unit = addCallCount("postOpen")

  def postLogReplay(c: ObserverContext[RegionCoprocessorEnvironment]): Unit = addCallCount("postLogReplay")

  def preFlushScannerOpen(c: ObserverContext[RegionCoprocessorEnvironment], store: Store, memstoreScanner: KeyValueScanner, s: InternalScanner): InternalScanner = {
    addCallCount("preFlushScannerOpen")
    s
  }

  def preFlush(c: ObserverContext[RegionCoprocessorEnvironment]): Unit = addCallCount("preFlush1")

  def preFlush(c: ObserverContext[RegionCoprocessorEnvironment], store: Store, scanner: InternalScanner): InternalScanner = {
    addCallCount("preFlush2")
    scanner
  }

  def postFlush(c: ObserverContext[RegionCoprocessorEnvironment]): Unit = addCallCount("postFlush1")

  def postFlush(c: ObserverContext[RegionCoprocessorEnvironment], store: Store, resultFile: StoreFile): Unit = addCallCount("postFlush2")

  def preCompactSelection(c: ObserverContext[RegionCoprocessorEnvironment], store: Store, candidates: util.List[StoreFile], request: CompactionRequest): Unit = addCallCount("preCompactSelection1")

  def preCompactSelection(c: ObserverContext[RegionCoprocessorEnvironment], store: Store, candidates: util.List[StoreFile]): Unit = addCallCount("preCompactSelection2")

  def postCompactSelection(c: ObserverContext[RegionCoprocessorEnvironment], store: Store, selected: ImmutableList[StoreFile], request: CompactionRequest): Unit = addCallCount("postCompactSelection1")

  def postCompactSelection(c: ObserverContext[RegionCoprocessorEnvironment], store: Store, selected: ImmutableList[StoreFile]): Unit = addCallCount("postCompactSelection2")

  def preCompact(c: ObserverContext[RegionCoprocessorEnvironment], store: Store, scanner: InternalScanner, scanType: ScanType, request: CompactionRequest): InternalScanner = {
    addCallCount("preCompact1")
    scanner
  }

  def preCompact(c: ObserverContext[RegionCoprocessorEnvironment], store: Store, scanner: InternalScanner, scanType: ScanType): InternalScanner = {
    addCallCount("preCompact2")
    scanner
  }

  def preCompactScannerOpen(c: ObserverContext[RegionCoprocessorEnvironment], store: Store, scanners: util.List[_ <: KeyValueScanner], scanType: ScanType, earliestPutTs: Long, s: InternalScanner, request: CompactionRequest): InternalScanner = {
    addCallCount("preCompactScannerOpen1")
    s
  }

  def preCompactScannerOpen(c: ObserverContext[RegionCoprocessorEnvironment], store: Store, scanners: util.List[_ <: KeyValueScanner], scanType: ScanType, earliestPutTs: Long, s: InternalScanner): InternalScanner = {
    addCallCount("preCompactScannerOpen2")
    s
  }

  def postCompact(c: ObserverContext[RegionCoprocessorEnvironment], store: Store, resultFile: StoreFile, request: CompactionRequest): Unit = addCallCount("postCompact1")

  def postCompact(c: ObserverContext[RegionCoprocessorEnvironment], store: Store, resultFile: StoreFile): Unit = addCallCount("postCompact2")

  def preSplit(c: ObserverContext[RegionCoprocessorEnvironment]): Unit = addCallCount("preSplit")

  def preSplit(c: ObserverContext[RegionCoprocessorEnvironment], splitRow: Array[Byte]): Unit = addCallCount("preSplit")

  def postSplit(c: ObserverContext[RegionCoprocessorEnvironment], l: Region, r: Region): Unit = addCallCount("postSplit")

  def preSplitBeforePONR(ctx: ObserverContext[RegionCoprocessorEnvironment], splitKey: Array[Byte], metaEntries: util.List[Mutation]): Unit = addCallCount("preSplitBeforePONR")

  def preSplitAfterPONR(ctx: ObserverContext[RegionCoprocessorEnvironment]): Unit = addCallCount("preSplitAfterPONR")

  def preRollBackSplit(ctx: ObserverContext[RegionCoprocessorEnvironment]): Unit = addCallCount("preRollBackSplit")

  def postRollBackSplit(ctx: ObserverContext[RegionCoprocessorEnvironment]): Unit = addCallCount("postRollBackSplit")

  def postCompleteSplit(ctx: ObserverContext[RegionCoprocessorEnvironment]): Unit = addCallCount("postCompleteSplit")

  def preClose(c: ObserverContext[RegionCoprocessorEnvironment], abortRequested: Boolean): Unit = addCallCount("preClose")

  def postClose(c: ObserverContext[RegionCoprocessorEnvironment], abortRequested: Boolean): Unit = addCallCount("postClose")

  def preGetClosestRowBefore(c: ObserverContext[RegionCoprocessorEnvironment], row: Array[Byte], family: Array[Byte], result: Result): Unit = addCallCount("preGetClosestRowBefore")

  def postGetClosestRowBefore(c: ObserverContext[RegionCoprocessorEnvironment], row: Array[Byte], family: Array[Byte], result: Result): Unit = addCallCount("postGetClosestRowBefore")

  def preGetOp(c: ObserverContext[RegionCoprocessorEnvironment], get: Get, result: util.List[Cell]): Unit = addCallCount("preGetOp")

  def postGetOp(c: ObserverContext[RegionCoprocessorEnvironment], get: Get, result: util.List[Cell]): Unit = addCallCount("postGetOp")

  def preExists(c: ObserverContext[RegionCoprocessorEnvironment], get: Get, exists: Boolean): Boolean = {
    addCallCount("preExists")
    exists
  }

  def postExists(c: ObserverContext[RegionCoprocessorEnvironment], get: Get, exists: Boolean): Boolean = {
    addCallCount("postExists")
    exists
  }

  def prePut(c: ObserverContext[RegionCoprocessorEnvironment], put: Put, edit: WALEdit, durability: Durability): Unit = addCallCount("prePut")

  def postPut(c: ObserverContext[RegionCoprocessorEnvironment], put: Put, edit: WALEdit, durability: Durability): Unit = addCallCount("postPut")

  def preDelete(c: ObserverContext[RegionCoprocessorEnvironment], delete: Delete, edit: WALEdit, durability: Durability): Unit = addCallCount("preDelete")

  def prePrepareTimeStampForDeleteVersion(c: ObserverContext[RegionCoprocessorEnvironment], mutation: Mutation, cell: Cell, byteNow: Array[Byte], get: Get): Unit = addCallCount("prePrepareTimeStampForDeleteVersion")

  def postDelete(c: ObserverContext[RegionCoprocessorEnvironment], delete: Delete, edit: WALEdit, durability: Durability): Unit = addCallCount("postDelete")

  def preBatchMutate(c: ObserverContext[RegionCoprocessorEnvironment], miniBatchOp: MiniBatchOperationInProgress[Mutation]): Unit = addCallCount("preBatchMutate")

  def postBatchMutate(c: ObserverContext[RegionCoprocessorEnvironment], miniBatchOp: MiniBatchOperationInProgress[Mutation]): Unit = addCallCount("postBatchMutate")

  def postStartRegionOperation(ctx: ObserverContext[RegionCoprocessorEnvironment], operation: Operation): Unit = addCallCount("postStartRegionOperation")

  def postCloseRegionOperation(ctx: ObserverContext[RegionCoprocessorEnvironment], operation: Operation): Unit = addCallCount("postCloseRegionOperation")

  def postBatchMutateIndispensably(ctx: ObserverContext[RegionCoprocessorEnvironment], miniBatchOp: MiniBatchOperationInProgress[Mutation], success: Boolean): Unit = addCallCount("postBatchMutateIndispensably")

  def preCheckAndPut(c: ObserverContext[RegionCoprocessorEnvironment], row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], compareOp: CompareOp, comparator: ByteArrayComparable, put: Put, result: Boolean): Boolean = {
    addCallCount("preCheckAndPut")
    result
  }

  def preCheckAndPutAfterRowLock(c: ObserverContext[RegionCoprocessorEnvironment], row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], compareOp: CompareOp, comparator: ByteArrayComparable, put: Put, result: Boolean): Boolean = {
    addCallCount("preCheckAndPutAfterRowLock")
    result
  }

  def postCheckAndPut(c: ObserverContext[RegionCoprocessorEnvironment], row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], compareOp: CompareOp, comparator: ByteArrayComparable, put: Put, result: Boolean): Boolean = {
    addCallCount("postCheckAndPut")
    result
  }

  def preCheckAndDelete(c: ObserverContext[RegionCoprocessorEnvironment], row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], compareOp: CompareOp, comparator: ByteArrayComparable, delete: Delete, result: Boolean): Boolean = {
    addCallCount("preCheckAndDelete")
    result
  }

  def preCheckAndDeleteAfterRowLock(c: ObserverContext[RegionCoprocessorEnvironment], row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], compareOp: CompareOp, comparator: ByteArrayComparable, delete: Delete, result: Boolean): Boolean = {
    addCallCount("preCheckAndDeleteAfterRowLock")
    result
  }

  def postCheckAndDelete(c: ObserverContext[RegionCoprocessorEnvironment], row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], compareOp: CompareOp, comparator: ByteArrayComparable, delete: Delete, result: Boolean): Boolean = {
    addCallCount("postCheckAndDelete")
    result
  }

  def preIncrementColumnValue(c: ObserverContext[RegionCoprocessorEnvironment], row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], amount: Long, writeToWAL: Boolean): Long = {
    addCallCount("preIncrementColumnValue")
    amount
  }

  def postIncrementColumnValue(c: ObserverContext[RegionCoprocessorEnvironment], row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], amount: Long, writeToWAL: Boolean, result: Long): Long = {
    addCallCount("postIncrementColumnValue")
    amount
  }

  def preAppend(c: ObserverContext[RegionCoprocessorEnvironment], append: Append): Result = {
    addCallCount("preAppend")
    null
  }

  def preAppendAfterRowLock(c: ObserverContext[RegionCoprocessorEnvironment], append: Append): Result = {
    addCallCount("preAppendAfterRowLock")
    null
  }

  def postAppend(c: ObserverContext[RegionCoprocessorEnvironment], append: Append, result: Result): Result = {
    addCallCount("postAppend")
    result
  }

  def preIncrement(c: ObserverContext[RegionCoprocessorEnvironment], increment: Increment): Result = {
    addCallCount("preIncrement")
    null
  }

  def preIncrementAfterRowLock(c: ObserverContext[RegionCoprocessorEnvironment], increment: Increment): Result = {
    addCallCount("preIncrementAfterRowLock")
    null
  }

  def postIncrement(c: ObserverContext[RegionCoprocessorEnvironment], increment: Increment, result: Result): Result = {
    addCallCount("postIncrement")
    result
  }

  def preScannerOpen(c: ObserverContext[RegionCoprocessorEnvironment], scan: Scan, s: RegionScanner): RegionScanner = {
    addCallCount("preScannerOpen")
    s
  }

  def preStoreScannerOpen(c: ObserverContext[RegionCoprocessorEnvironment], store: Store, scan: Scan, targetCols: util.NavigableSet[Array[Byte]], s: KeyValueScanner): KeyValueScanner = {
    addCallCount("preStoreScannerOpen")
    s
  }

  def postScannerOpen(c: ObserverContext[RegionCoprocessorEnvironment], scan: Scan, s: RegionScanner): RegionScanner = {
    addCallCount("postScannerOpen")
    s
  }

  def preScannerNext(c: ObserverContext[RegionCoprocessorEnvironment], s: InternalScanner, result: util.List[Result], limit: Int, hasNext: Boolean): Boolean = {
    addCallCount("preScannerNext")
    hasNext
  }

  def postScannerNext(c: ObserverContext[RegionCoprocessorEnvironment], s: InternalScanner, result: util.List[Result], limit: Int, hasNext: Boolean): Boolean = {
    addCallCount("postScannerNext")
    hasNext
  }

  def postScannerFilterRow(c: ObserverContext[RegionCoprocessorEnvironment], s: InternalScanner, currentRow: Array[Byte], offset: Int, length: Short, hasMore: Boolean): Boolean = {
    addCallCount("postScannerFilterRow")
    hasMore
  }

  def preScannerClose(c: ObserverContext[RegionCoprocessorEnvironment], s: InternalScanner): Unit = addCallCount("preScannerClose")

  def postScannerClose(c: ObserverContext[RegionCoprocessorEnvironment], s: InternalScanner): Unit = addCallCount("postScannerClose")

  def preWALRestore(ctx: ObserverContext[_ <: RegionCoprocessorEnvironment], info: HRegionInfo, logKey: WALKey, logEdit: WALEdit): Unit = addCallCount("preWALRestore1")

  def preWALRestore(ctx: ObserverContext[RegionCoprocessorEnvironment], info: HRegionInfo, logKey: HLogKey, logEdit: WALEdit): Unit = addCallCount("preWALRestore2")

  def postWALRestore(ctx: ObserverContext[_ <: RegionCoprocessorEnvironment], info: HRegionInfo, logKey: WALKey, logEdit: WALEdit): Unit = addCallCount("postWALRestore1")

  def postWALRestore(ctx: ObserverContext[RegionCoprocessorEnvironment], info: HRegionInfo, logKey: HLogKey, logEdit: WALEdit): Unit = addCallCount("postWALRestore2")

  def preBulkLoadHFile(ctx: ObserverContext[RegionCoprocessorEnvironment], familyPaths: util.List[Pair[Array[Byte], String]]): Unit = addCallCount("preBulkLoadHFile")

  def postBulkLoadHFile(ctx: ObserverContext[RegionCoprocessorEnvironment], familyPaths: util.List[Pair[Array[Byte], String]], hasLoaded: Boolean): Boolean = {
    addCallCount("postBulkLoadHFile")
    hasLoaded
  }

  def preStoreFileReaderOpen(ctx: ObserverContext[RegionCoprocessorEnvironment], fs: FileSystem, p: Path, in: FSDataInputStreamWrapper, size: Long, cacheConf: CacheConfig, r: Reference, reader: Reader): Reader = {
    addCallCount("preStoreFileReaderOpen")
    addCallCount(s"- preStoreFileReaderOpen -${p.getName}")
    reader
  }

  def postStoreFileReaderOpen(ctx: ObserverContext[RegionCoprocessorEnvironment], fs: FileSystem, p: Path, in: FSDataInputStreamWrapper, size: Long, cacheConf: CacheConfig, r: Reference, reader: Reader): Reader = {
    addCallCount("postStoreFileReaderOpen")
    addCallCount(s"- postStoreFileReaderOpen-${p.getName}")
    reader
  }

  def postMutationBeforeWAL(ctx: ObserverContext[RegionCoprocessorEnvironment], opType: MutationType, mutation: Mutation, oldCell: Cell, newCell: Cell): Cell = {
    addCallCount("postMutationBeforeWAL")
    addCallCount(s"- postMutationBeforeWAL-$opType")
    newCell
  }

  def postInstantiateDeleteTracker(ctx: ObserverContext[RegionCoprocessorEnvironment], delTracker: DeleteTracker): DeleteTracker = {
    addCallCount("postInstantiateDeleteTracker")
    delTracker
  }
}
