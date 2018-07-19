package sparkka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

class TestRddAdapter extends FlatSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures with SparkFixture {
  implicit val patience = PatienceConfig(
    timeout(10.seconds).value,
    interval(1.seconds).value
  )

  "SparkkaAdapter" should "run a spark rdd through akka" in withSparkContext("run-spark") { ctx =>

    val rdd = SparkkaRdd(ec => ctx.makeRDD(1 to 5))

    val adapted = RddAdapter(rdd, ec => Flow[Int].map(_ * 2))

    val res = adapted.run(SparkkaExecutor.Default).collect {
      case Right(v) => v
    }.aggregate(Vector.empty[Int])(_ :+ _, _ ++ _)

    res.sorted should be (Vector(2, 4, 6, 8, 10))
  }

  it should "run a spark rdd through akka with implicits" in withSparkContext("run-implicit") { ctx =>
    import implicits._

    val rdd = SparkkaRdd(ec => ctx.makeRDD(1 to 5))

    val adapted = rdd.via(ec => Flow[Int].map(_ * 2))

    val res = adapted.run(SparkkaExecutor.Default).collect {
      case Right(v) => v
    }.aggregate(Vector.empty[Int])(_ :+ _, _ ++ _)

    res.sorted should be (Vector(2, 4, 6, 8, 10))
  }

}
