package org.validoc


import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scalaz.{-\/, EitherT, \/, \/-}
import scalaz.concurrent.Task
import scalaz.stream.async.mutable.Queue
import scalaz.stream.{Channel, Process, Sink, async, channel, merge, sink, io}
import scala.concurrent.ExecutionContext.Implicits.global


object HelloStream extends App {

  import StreamOps._


  val productionIds: Process[Task, ProductionId] = io.linesR("data.txt").map(ProductionId(_))
  val loadFromThor = (id: ProductionId) => Future(ThorData(id.raw))
  val loadFromMercury = (id: ProductionId) => Future(MercuryData(id.raw))
  val indexer = (document: Document) => ()

  def productionIdQueue(size: Int): Queue[ProductionId] = async.boundedQueue[ProductionId](size)

  def makeProcess(productionIds: Process[Task, ProductionId],
                  loadFromThor: ProductionId => Future[ThorData],
                  loadFromMercury: ProductionId => Future[MercuryData],
                  indexer: Document => Unit) = {
    val thorProductionIdQueue: Queue[ProductionId] = productionIdQueue(10)
    val mercuryProductionIdQueue = productionIdQueue(10)
    val documentsQueue = async.boundedQueue[Document](10)


    def indexerSink[T]: Sink[Task, T] = sink.lift[Task, T]((doc: T) => Task(println(doc.toString)))

    val p = thorProductionIdQueue.dequeue.through(loadFromThor.asChannel)


    merge.mergeN(10)(Process(productionIds.to(thorProductionIdQueue.enqueue),
      thorProductionIdQueue.dequeue.through(loadFromThor.asChannel).to(documentsQueue.enqueue),
      productionIds.to(mercuryProductionIdQueue.enqueue),
      mercuryProductionIdQueue.throughChannelToQueue(loadFromMercury.asChannel, documentsQueue),
      documentsQueue.dequeue.to(indexerSink)))
  }

  makeProcess(productionIds, loadFromThor, loadFromMercury, indexer).run.run
}
