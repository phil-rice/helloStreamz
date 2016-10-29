package org.validoc

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scalaz.{-\/, EitherT, \/, \/-}
import scalaz.concurrent.Task
import scalaz.stream.async.mutable.Queue
import scalaz.stream.{Process, channel, _}

object StreamOps {

  implicit class FuturePimper[T](fut: () => Future[T])(implicit ec: ExecutionContext) {
    def asTask: Task[T] =
      Task.async {
        register =>
          fut().onComplete {
            case Success(v) => register(\/-(v))
            case Failure(ex) => register(-\/(ex))
          }
      }
  }


  implicit class FutureFnPimper[Req, Res](fn: Req => Future[Res]) {
    def asFnToTask(implicit ec: ExecutionContext): Req => Task[Res] = req => (() => fn(req)).asTask

    def asChannel(implicit ec: ExecutionContext): Channel[Task, Req, Res] = channel.lift(asFnToTask)
  }

  implicit class FutureEitherPimper[Req, A, B](fn: Req => EitherT[Future, A, B]) {
    def asFnToTask(implicit ec: ExecutionContext): Req => Task[\/[A, B]] = req => (() => fn(req).run).asTask

    def asChannel(implicit ec: ExecutionContext): Channel[Task, Req, \/[A, B]] = channel.lift(asFnToTask)
  }

  implicit class ChannelPimper[In, Out](channel: Channel[Task, In, Out]) {
    def channelMap[NewOut](fn: Out => NewOut): Channel[Task, In, NewOut] = channel.map(f => (in: In) => f(in).map(fn))
  }

  implicit class QueuePimper[T](queue: Queue[T]) {
    def throughChannelToQueue[Out](channel: Channel[Task, T, Out], outQueue: Queue[Out]): Process[Task, Unit] = {
      queue.dequeue.through(channel).to(outQueue.enqueue)
    }
  }

}
