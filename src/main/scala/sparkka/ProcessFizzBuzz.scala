package sparkka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.spark.{SparkConf, SparkContext}

object ProcessFizzBuzz extends App {
  import implicits._

  implicit val system = ActorSystem("ProcessFizzBuzz")
  implicit val mat = ActorMaterializer()

  val sparkConf = new SparkConf()
    .setAppName("sparkka.ProcessFizzBuzz")
    .setMaster("local[*]")
  val sparkContext = SparkContext.getOrCreate(sparkConf)

  val rdd = sparkContext.makeRDD(
    (1 to 50).map { _ =>
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
  ).collect {
    case "fizz" => FizzBuzz.Fizz
    case "buzz" => FizzBuzz.Buzz
    case "fizzbuzz" -> FizzBuzz.FizzBuzz
  }.via(fizzBuzzFlow)

}
