package sparkka

import org.apache.spark.rdd.RDD

case class SparkkaRdd[T](f: SparkkaExecutor => RDD[T]) {
  def run(ec: SparkkaExecutor): RDD[T] = f(ec)
}