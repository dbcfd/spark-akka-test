package sparkka

import org.apache.spark.rdd.RDD

/**
  * To late bind the executor, define rdd's in the form of a transform, created from a sparkka executor
  * @param f
  * @tparam T
  */
case class SparkkaRdd[T](f: SparkkaExecutor => RDD[T]) {
  def run(ec: SparkkaExecutor): RDD[T] = f(ec)
}