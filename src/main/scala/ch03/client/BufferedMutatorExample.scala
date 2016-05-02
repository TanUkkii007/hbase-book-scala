package ch03.client

import java.util.concurrent.Executors

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import util.{LoanPattern, HBaseHelper}

import scala.collection.immutable.IndexedSeq
import scala.concurrent.{ExecutionContextExecutor, ExecutionContext, Future}
import scala.util.{Failure, Success}

object BufferedMutatorExample extends App with LoanPattern {
  import ExceptionListenerConversion._

  val log = LogFactory.getLog(classOf[BufferedMutator])

  val POOL_SIZE = 10
  val TASK_COUNT = 100
  val TABLE = TableName.valueOf("testtable")
  val FAMILY = Bytes.toBytes("colfam1")

  val configuration = HBaseConfiguration.create()

  val helper = new HBaseHelper(configuration)

  helper.dropTable(TABLE)
  helper.createTable(TABLE, List("colfam1"))

  def listener(e: RetriesExhaustedWithDetailsException, mutator: BufferedMutator) = {
    0 until e.getNumExceptions foreach(i => log.info(s"Failed to sent put: ${e.getRow(i)}"))
  }

  val params = new BufferedMutatorParams(TABLE).listener(listener _)

  val conn = ConnectionFactory.createConnection(configuration)
  val mutator = conn.getBufferedMutator(params)

  val workerPool = Executors.newFixedThreadPool(POOL_SIZE)
  implicit val ec = ExecutionContext.fromExecutor(workerPool)

  val futures: IndexedSeq[Future[Unit]] = 0 until TASK_COUNT map { _ =>
    Future {
      val p = new Put(Bytes.toBytes("row1"))
      p.addColumn(FAMILY, Bytes.toBytes("qual1"), Bytes.toBytes("val1"))
      mutator.mutate(p)
    }
  }

  Future.sequence(futures).onComplete {
    case Success(_) => log.info("Buffered Write success"); mutator.close(); conn.close(); workerPool.shutdown(); helper.close()
    case Failure(e) => log.info("Exception while creating or freeing resources", e); mutator.close(); conn.close(); workerPool.shutdown(); helper.close()
  }

}

object ExceptionListenerConversion {
  implicit def toExceptionListener(f: (RetriesExhaustedWithDetailsException, BufferedMutator) => Unit): ExceptionListener = new BufferedMutator.ExceptionListener() {
    override def onException(exception: RetriesExhaustedWithDetailsException, mutator: BufferedMutator): Unit = f(exception, mutator)
  }
}