package sparkka.implicits

import org.apache.spark.rdd.RDD

import scala.language.implicitConversions

trait RddImplicits {
  @inline implicit def rddToRichRdd[T](rdd: RDD[T]): impl.RichRdd[T] = new impl.RichRdd[T](rdd)
}
