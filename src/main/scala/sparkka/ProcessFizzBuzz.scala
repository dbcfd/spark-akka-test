package sparkka

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, GraphDSL, Merge}
import akka.stream.{ActorMaterializer, FlowShape, Materializer}
import org.apache.spark.{SparkConf, SparkContext}

object ProcessFizzBuzz extends App {
  import implicits._

  /**
    * Create a flow using fizzbuzzer that then aggregates counts as long as we haven't seen all types
    * @param ec
    * @return
    */
  def fizzBuzzFlow(ec: SparkkaExecutor): Flow[FizzBuzzState, Map[FizzBuzzState, Int], akka.NotUsed] = {
    val g = GraphDSL.create() { implicit bldr =>
      import GraphDSL.Implicits._

      val in = bldr.add(Flow[FizzBuzzState])

      val splitter = bldr.add(FizzBuzzer())

      in ~> splitter

      //add indicator to which flow this came from
      val fizzHandler = bldr.add(Flow[Int].map(FizzBuzzState.Fizz -> _))
      val buzzHandler = bldr.add(Flow[Int].map(FizzBuzzState.Buzz -> _))
      val fizzBuzzHandler = bldr.add(Flow[Int].map(FizzBuzzState.FizzBuzz -> _))

      splitter.out(0) ~> fizzHandler
      splitter.out(1) ~> buzzHandler
      splitter.out(2) ~> fizzBuzzHandler

      //combine the split flows so we can create a single outlet
      val merge = bldr.add(Merge[(FizzBuzzState, Int)](splitter.outlets.length))

      fizzHandler ~> merge
      buzzHandler ~> merge
      fizzBuzzHandler ~> merge

      //aggregate as long as we haven't seen all t ypes
      val scan = bldr.add(Flow[(FizzBuzzState, Int)].scan(Map.empty[FizzBuzzState, Int]) {
        case (agg, t) if agg.size == FizzBuzzState.values =>
          Map.empty[FizzBuzzState, Int] + t
        case (agg, (k, v)) =>
          val upd = agg.getOrElse(k, 0) + v
          agg.updated(k, upd)
      })

      merge ~> scan

      FlowShape(in.in, scan.out)
    }

    Flow.fromGraph(g)
  }

  implicit val system = ActorSystem("ProcessFizzBuzz")
  implicit val mat = ActorMaterializer()

  val sparkConf = new SparkConf()
    .setAppName("sparkka.ProcessFizzBuzz")
    .setMaster("local[*]")
  val sparkContext = SparkContext.getOrCreate(sparkConf)

  val result = SparkkaRdd(ec => sparkContext.makeRDD(
    (1 to 50).map { _ =>
      //generate the strings
      val v = scala.util.Random.nextInt(100)
      val fizzPart = if (v % 3 == 0) {
        "fizz"
      } else {
        ""
      }
      val buzzPart = if (v % 5 == 0) {
        "buzz"
      } else {
        ""
      }
      s"$fizzPart$buzzPart"
    }
  ).flatMap(FizzBuzzState.unapply))
    .via( (ec: SparkkaExecutor) => fizzBuzzFlow(ec))
    .run(SparkkaExecutor.Default)
    .aggregate(Map.empty[FizzBuzzState, Vector[Int]])(
      //aggregation op
      {
        case (agg, Left(_)) => agg
        case (agg, Right(m)) =>
          m.foldLeft(agg) {
            case (upd, (k, v)) =>
              val existing = upd.getOrElse(k, Vector.empty)
              upd.updated(k, existing :+ v)
          }
      },
      //combine op
      {
        case (a, b) =>
          a.foldLeft(b) {
            case (agg, (k, v)) =>
              val existing = agg.getOrElse(k, Vector.empty)
              agg.updated(k, existing ++ v)
          }
      }
    )

  result.foreach {
    case (st, values) =>
      println(s"State $st : $values")
  }

  sys.exit(0)

}
