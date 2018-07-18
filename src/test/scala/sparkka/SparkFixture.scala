package sparkka

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Suite, SuiteMixin}

trait SparkFixture extends SuiteMixin { this: Suite =>
  def withSparkContext(name: String)(f: SparkContext => Any): Any = {
    val sparkConf = new SparkConf()
      .setAppName(s"sparkka.$name")
      .setMaster("local[*]")
    val sparkContext = SparkContext.getOrCreate(sparkConf)

    try {
      f(sparkContext)
    } finally {
      sparkContext.stop()
    }
  }
}
