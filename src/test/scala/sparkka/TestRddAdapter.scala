package sparkka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.testkit.TestKit
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class TestRddAdapter extends TestKit(ActorSystem("RddAdapter")) with FlatSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures with SparkFixture {
  override def afterAll(): Unit = {
    this.system.terminate().futureValue
  }
  implicit val mat = ActorMaterializer()

  implicit val patience = PatienceConfig(
    timeout(10.seconds).value,
    interval(1.seconds).value
  )

  "SparkkaAdapter" should "run a spark rdd through akka" in withSparkContext("run-spark") { ctx =>

    val rdd = ctx.makeRDD(1 to 5)

    val flow = Flow[Int].map(_ * 2)

    val adapted = RddAdapter(rdd, flow)

    val res = adapted.collect {
      case Right(v) => v
    }.aggregate(Vector.empty[Int])(_ :+ _, _ ++ _)

    res should be (Vector(2, 4, 6, 8, 10))
  }

  it should "run a spark rdd through akka with implicits" in withSparkContext("run-implicit") { ctx =>
    import implicits._

    val rdd = ctx.makeRDD(1 to 10)

    val flow = Flow[Int].map(_ * 2)

    val adapted = rdd.via(flow)

    val res = adapted.collect {
      case Right(v) => v
    }.aggregate(Vector.empty[Int])(_ :+ _, _ ++ _)

    res should be (Vector(2, 4, 6, 8, 10))
  }

}
