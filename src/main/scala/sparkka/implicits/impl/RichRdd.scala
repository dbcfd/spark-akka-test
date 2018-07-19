package sparkka.implicits.impl

import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import sparkka.{RddAdapter, SparkkaExecutor, SparkkaRdd}

/**
  * Enrichment to make passing an rdd through a flow more dsl like
  * @param rdd
  * @tparam T
  */
final class RichRdd[T](val rdd: SparkkaRdd[T]) extends AnyVal {
  def via[O, M](flow: SparkkaExecutor => Flow[T, O, M]): SparkkaRdd[Either[M, O]] = {
    RddAdapter(rdd, flow)
  }
}
