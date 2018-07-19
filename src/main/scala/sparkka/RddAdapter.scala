package sparkka

import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Need some akka in your spark? Just define a flow and combine it with an rdd
  */
object RddAdapter {
  def apply[I, O, M](rdd: SparkkaRdd[I], flow: SparkkaExecutor => Flow[I, O, M]): SparkkaRdd[Either[M, O]] = {
    val func = (ec: SparkkaExecutor) => {
      rdd.run(ec).mapPartitions { iter =>
        import ec._

        val sink = Sink.queue[O]()

        val (flowMaterializedValue, pullable) = Source.fromIterator(() => iter)
          .viaMat(flow(ec))(Keep.right)
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
              hasValues = true
              maybeNext = Option(Left(flowMaterializedValue))
            } else {
              hasValues = false
            }
            hasValues
          }

          override def next(): Either[M, O] = {
            //iterator usage should always call hasNext before calling next, which will populate this
            maybeNext.get
          }
        }
      }
    }
    SparkkaRdd(func)
  }
}
