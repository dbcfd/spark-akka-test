package sparkka

import akka.actor.ActorSystem
import akka.stream.javadsl.RunnableGraph
import akka.stream.{ActorMaterializer, ClosedShape, SinkShape}
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Sink, Source}
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.Future

class TestFizzBuzzer extends TestKit(ActorSystem("FizzBuzzer")) with FlatSpecLike with Matchers with ScalaFutures with BeforeAndAfterAll {
  override def afterAll(): Unit = {
    this.system.terminate().futureValue
  }
  implicit val mat = ActorMaterializer()

  def run(
    source: Source[FizzBuzzState, akka.NotUsed],
    fizzBuzzer: FizzBuzzer
  ): Future[Map[FizzBuzzState, Int]] = {
    val sink = Sink.fold[Map[FizzBuzzState, Int], (FizzBuzzState, Int)](Map.empty[FizzBuzzState, Int]) {
      case (agg, (k, v)) =>
        val existing = agg.getOrElse(k, 0)
        agg.updated(k, existing + v)
    }
    val g = GraphDSL.create(source, sink)( (_, s) => s) { implicit bldr => (src, snk) =>
      import GraphDSL.Implicits._

      val fb = bldr.add(new FizzBuzzer)

      src ~> fb

      val merge = bldr.add(Merge[(FizzBuzzState, Int)](fb.outlets.length))

      val aggFizz = bldr.add(Flow[Int].fold(0)(_ + _).map(FizzBuzzState.Fizz -> _))
      fb.out(0) ~> aggFizz ~> merge

      val aggBuzz = bldr.add(Flow[Int].fold(0)(_ + _).map(FizzBuzzState.Buzz -> _))
      fb.out(1) ~> aggBuzz ~> merge

      val aggFizzBuzz = bldr.add(Flow[Int].fold(0)(_ + _).map(FizzBuzzState.FizzBuzz -> _))
      fb.out(2) ~> aggFizzBuzz ~> merge

      merge ~> snk

      ClosedShape
    }

    RunnableGraph.fromGraph(g).run(mat)
  }

  "FizzBuzzer" should "count fizzes and buzzes" in {
    val source = Source.fromIterator(() => Iterator(FizzBuzzState.Fizz, FizzBuzzState.Buzz, FizzBuzzState.FizzBuzz, FizzBuzzState.Buzz, FizzBuzzState.Fizz))

    run(source, new FizzBuzzer).futureValue should be (Map(
      FizzBuzzState.Fizz -> 2,
      FizzBuzzState.Buzz -> 2,
      FizzBuzzState.FizzBuzz -> 1
    ))
  }
}
