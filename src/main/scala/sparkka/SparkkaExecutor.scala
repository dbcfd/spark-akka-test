package sparkka

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}

object SparkkaExecutor {
  implicit val system = ActorSystem("Sparkka")
  implicit val materializer = ActorMaterializer()

  /**
    * Reference the values that live in this object, and thus will live on each node
    */
  case object Default extends SparkkaExecutor {
    implicit def system = SparkkaExecutor.system
    implicit def materializer = SparkkaExecutor.materializer
  }
}

/**
  * Use a trait so that we can correctly serialize it in the RDD, since ActorSystem isn't serializable
  */
trait SparkkaExecutor {
  implicit def system: ActorSystem
  implicit def materializer: Materializer
}
