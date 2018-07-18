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
    source: Source[FizzOrBuzzOrFizzBuzz, akka.NotUsed],
    fizzBuzzer: FizzBuzzer
  ): Future[Map[FizzOrBuzzOrFizzBuzz, Vector[Int]]] = {
    val sink = Sink.fold(Map.empty[FizzOrBuzzOrFizzBuzz, Vector[Int]]) {
      case (agg, (e: FizzOrBuzzOrFizzBuzz, v: Vector[Int])) =>
        val existing = agg.getOrElse(e, Vector.empty)
        agg.updated(e, existing ++ v)
    }
    val g = GraphDSL.create(source, sink)( (_, s) => s) { implicit bldr => (src, snk) =>
      import GraphDSL.Implicits._

      source ~> fizzBuzzer.inlet

      val merge = bldr.add(Merge[(FizzOrBuzzOrFizzBuzz, Vector[Int])](3))

      val aggFizz = bldr.add(Flow[Int].fold(Vector.empty[Int])(_ :+ _).map(FizzBuzz.Fizz -> _))
      fizzBuzzer.outletFizz ~> aggFizz ~> merge

      val aggBuzz = bldr.add(Flow[Int].fold(Vector.empty[Int])(_ :+ _).map(FizzBuzz.Buzz -> _))
      fizzBuzzer.outletBuzz ~> aggBuzz ~> merge

      val aggFizzBuzz = bldr.add(Flow[Int].fold(Vector.empty[Int])(_ :+ _).map(FizzBuzz.FizzBuzz -> _))
      fizzBuzzer.outletFizzBuzz ~> aggFizzBuzz ~> merge

      merge ~> snk

      ClosedShape
    }

    RunnableGraph.fromGraph(g).run(mat)
  }

  "FizzBuzzer" should "count fizzes and buzzes" in {
    val source = Source.fromIterator(() => Iterator(FizzBuzz.Fizz, FizzBuzz.Buzz, FizzBuzz.FizzBuzz, FizzBuzz.Buzz, FizzBuzz.Fizz))

    run(source, new FizzBuzzer).futureValue should be (Map(
      FizzBuzz.Fizz -> 2,
      FizzBuzz.Buzz -> 2,
      FizzBuzz.FizzBuzz -> 1
    ))
  }
}
