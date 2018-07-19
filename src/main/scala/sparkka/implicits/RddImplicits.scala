package sparkka.implicits

import org.apache.spark.rdd.RDD
import sparkka.SparkkaRdd

import scala.language.implicitConversions

trait RddImplicits {
  @inline implicit def rddToRichRdd[T](rdd: SparkkaRdd[T]): impl.RichRdd[T] = new impl.RichRdd[T](rdd)
}
