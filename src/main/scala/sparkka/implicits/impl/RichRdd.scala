package sparkka.implicits.impl

import akka.stream.scaladsl.Flow
import org.apache.spark.rdd.RDD
import sparkka.RddAdapter

final class RichRdd[T](val rdd: RDD[T]) extends AnyVal {
  def via[O, M](flow: Flow[T, O, M]): RDD[Either[M, O]] = {
    RddAdapter(rdd, flow)
  }
}
