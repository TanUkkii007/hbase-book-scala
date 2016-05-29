package ch4.coprocessor

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HRegionInfo, HTableDescriptor}
import org.apache.hadoop.hbase.coprocessor.{MasterCoprocessorEnvironment, ObserverContext, BaseMasterObserver}
import org.apache.hadoop.hbase.regionserver.HRegion

object MasterObserverExample {
  final val log: Log = LogFactory.getLog(classOf[HRegion])
}

class MasterObserverExample extends BaseMasterObserver {
  import MasterObserverExample._

  override def postCreateTable(ctx: ObserverContext[MasterCoprocessorEnvironment], desc: HTableDescriptor, regions: Array[HRegionInfo]): Unit = {
    log.debug("Got postCreateTable callback")

    val tableName = desc.getTableName

    log.debug("Created table: " + tableName + ", region count: " + regions.length)

    val services = ctx.getEnvironment.getMasterServices
    val masterFileSystem = services.getMasterFileSystem
    val fileSystem = masterFileSystem.getFileSystem

    val blobPath = new Path(tableName.getQualifierAsString + "-blobs")
    fileSystem.mkdirs(blobPath)

    log.debug("Created " + blobPath + ": " + fileSystem.exists(blobPath))
  }
}
