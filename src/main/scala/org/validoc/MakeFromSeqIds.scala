package org.validoc

import java.util.concurrent.{ScheduledExecutorService, ScheduledThreadPoolExecutor}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, _}
import scala.io.Source
import scala.util.{Success, Try}
import scalaz.concurrent.Task
import scalaz.stream.async.mutable.Queue
import scalaz.stream.{merge, sink, _}

class BackOff[T](delay: FiniteDuration)(conditionToDelay: T => Boolean)(implicit schedulerPool: ScheduledExecutorService) extends (T => Process[Task, T]) {
  protected def backoff(ts: T): Process[Task, T] = Process.emit(ts).append[Task, T](time.sleep(delay))

  protected def normal(ts: T) = Process.emit(ts)

  def apply(in: T): Process[Task, T] = if (conditionToDelay(in)) backoff(in) else normal(in)
}

object BackOff {
  def backOffIfInsufficientSize[T](minSize: Int, delayTime: FiniteDuration)(implicit schedulerPool: ScheduledExecutorService) = new BackOff[Seq[T]](delayTime)(_.size < minSize)

  def backOffIfLotsOfErrors[T](maxErrors: Int, delayTime: FiniteDuration)(implicit schedulerPool: ScheduledExecutorService) = new BackOff[Seq[Try[T]]](delayTime)(_.filter(_.isFailure).size > maxErrors)
}


object MakeFromSeqIds extends App {

  import StreamOps._
  import BackOff._

  val productionIds: Process0[List[ProductionId]] = Process.emit(Source.fromFile("data.txt").getLines().map(ProductionId(_)).toList)

  val loadFromThor = (id: ProductionId) => Future(ThorData(id.raw))
  val loadFromMercury = (id: ProductionId) => Future(MercuryData(id.raw))
  val indexer = (document: Document) => ()

  def productionIdQueue(size: Int): Queue[ProductionId] = async.boundedQueue[ProductionId](size)


  def makeProcess(productionIds: Process[Task, List[ProductionId]],
                  loadFromThor: ProductionId => Future[ThorData],
                  loadFromMercury: ProductionId => Future[MercuryData],
                  indexer: Document => Unit)(implicit schedulerPool: ScheduledExecutorService) = {
    val thorProductionIdQueue: Queue[ProductionId] = productionIdQueue(500)
    val mercuryProductionIdQueue = productionIdQueue(10)
    val documentsQueue = async.boundedQueue[Document](10)

    def indexerSink[T]: Sink[Task, T] = sink.lift[Task, T]((doc: T) => Task(println(doc.toString)))

    def getProductionIdsAndPutOnQueue[T](source: Process[Task, List[T]], queue: Queue[T]) =
      source.flatMap(Process.emitAll).to(queue.enqueue)

    def takeOffQueueBackOffIfNeededTransformPutOnQueue[In, Out](fromQueue: Queue[In], fn: In => Future[Out], toQueue: Queue[Out]) =
      fromQueue.dequeueBatch(2).
        flatMap(backOffIfInsufficientSize(2, 5 seconds)).
        through(fn.asChannelOfSeqTry).
        flatMap(backOffIfLotsOfErrors(4, 5 seconds)).
        flatMap(s => Process.emitAll(s.collect { case Success(s) => s })).
        to(toQueue.enqueue)


    def makeMergedProcess[T](n: Int)(ps: Process[Task, T]*) = {
      val resultant = merge.mergeN(n)(Process(ps: _*))
      ps.foreach(_.onComplete(resultant.kill))
      resultant
    }


    merge.mergeN(10)(Process(
      getProductionIdsAndPutOnQueue(productionIds, thorProductionIdQueue),
      takeOffQueueBackOffIfNeededTransformPutOnQueue(thorProductionIdQueue, loadFromThor, documentsQueue),

      getProductionIdsAndPutOnQueue(productionIds, mercuryProductionIdQueue),
      takeOffQueueBackOffIfNeededTransformPutOnQueue(mercuryProductionIdQueue, loadFromMercury, documentsQueue),

      documentsQueue.dequeue.to(indexerSink)))
  }

  val p1 = productionIds.toSource
  p1.run.run
  println("Productions id finished: " + p1.isHalt)

  val p2 = productionIds.flatMap(Process.emitAll).to(productionIdQueue(20).enqueue)
  p2.run.run
  println("Productions id  to queue finished: " + p2.isHalt)

  val p = makeProcess(productionIds, loadFromThor, loadFromMercury, indexer)(new ScheduledThreadPoolExecutor(10))
  p.run.run
  println("Finished")
}
