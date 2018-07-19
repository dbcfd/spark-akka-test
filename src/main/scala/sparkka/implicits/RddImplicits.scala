package sparkka.implicits

import sparkka.SparkkaRdd

import scala.language.implicitConversions

trait RddImplicits {
  @inline implicit def rddToRichRdd[T](rdd: SparkkaRdd[T]): impl.RichRdd[T] = new impl.RichRdd[T](rdd)
}
