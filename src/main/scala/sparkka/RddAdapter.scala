package sparkka

import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.spark.rdd.RDD

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Need some akka in your spark? Just define a flow and combine it with an rdd
  */
object RddAdapter {
  def apply[I, O, M](rdd: RDD[I], flow: Flow[I, O, M])(implicit mat: Materializer): RDD[Either[M, O]] = {
    rdd.mapPartitions { iter =>
      val sink = Sink.queue[O]()

      val (mat, pullable) = Source.fromIterator(() => iter)
        .viaMat(flow)(Keep.right)
        .toMat(sink)(Keep.both)
        .run()

      new Iterator[Either[M, O]] {
        private var hasValues = true
        private var needsMaterializedValue = true
        private var maybeNext = Option.empty[Either[M, O]]
        override def hasNext: Boolean = {
          if (hasValues) {
            maybeNext = Await.result(pullable.pull(), Duration.Inf).map(Right(_))
            hasValues = maybeNext.nonEmpty
          } else if (needsMaterializedValue) {
            needsMaterializedValue = false
            maybeNext = Option(Left(mat))
          }
          hasValues || needsMaterializedValue
        }
        override def next(): Either[M, O] = {
          //iterator usage should always call hasNext before calling next, which will populate this
          maybeNext.get
        }
      }
    }
  }
}
