package sparkka

import akka.stream.{Attributes, Inlet, Outlet, UniformFanOutShape}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}

import scala.concurrent.{Future, Promise}

object FizzBuzzer {
  case class Stats(fizzes: Int, buzzes: Int, fizzbuzzes: Int) {
    def markFizz(): Stats = copy(fizzes = fizzes + 1)
    def markBuzz(): Stats = copy(buzzes = buzzes + 1)
    def markFizzBuzz(): Stats = copy(fizzbuzzes = fizzbuzzes + 1)
  }

  def apply(): FizzBuzzer = {
    new FizzBuzzer
  }
}

class FizzBuzzer extends GraphStageWithMaterializedValue[UniformFanOutShape[FizzOrBuzzOrFizzBuzz, Int], Future[FizzBuzzer.Stats]] {
  val inlet = Inlet[FizzOrBuzzOrFizzBuzz]("fb-in")
  val outletFizz = Outlet[Int](name = "fizz-out")
  val outletBuzz = Outlet[Int](name = "buzz-out")
  val outletFizzBuzz = Outlet[Int](name = "fizzbuzz-out")

  val shape = UniformFanOutShape(inlet, outletFizz, outletBuzz, outletFizzBuzz)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[FizzBuzzer.Stats]) = {
    val promiseStats = Promise[FizzBuzzer.Stats]()

    val logic = new GraphStageLogic(shape) {
      var fizzDemand = false
      var fizzCount = 0
      var buzzDemand = false
      var buzzCount = 0
      var fizzbuzzDemand = false
      var fizzbuzzCount = 0
      var total = 0

      setHandler(inlet, new InHandler {
        override def onPush(): Unit = {
          total
          grab(inlet) match {
            case m: Fizz with Buzz =>
              fizzbuzzCount += 1
              if (!isClosed(outletFizzBuzz) && isAvailable(outletFizzBuzz) && fizzbuzzDemand) {
                push(outletFizzBuzz, fizzbuzzCount)
                fizzbuzzCount = 0
                fizzbuzzDemand = false
              } else {
                tryPull(inlet)
              }
            case m: Fizz =>
              fizzCount += 1
              if (!isClosed(outletFizzBuzz) && isAvailable(outletFizzBuzz) && fizzDemand) {
                push(outletFizzBuzz, fizzCount)
                fizzCount = 0
                fizzDemand = false
              } else {
                tryPull(inlet)
              }
            case m: Buzz =>
              buzzCount += 1
              if (!isClosed(outletBuzz) && isAvailable(outletBuzz) && buzzDemand) {
                push(outletBuzz, buzzCount)
                buzzCount = 0
                buzzDemand = false
              } else {
                tryPull(inlet)
              }
          }
        }
      })

      def shouldPull: Boolean = !(fizzDemand || buzzDemand || fizzbuzzDemand)

      setHandler(outletFizz, new OutHandler {
        override def onPull(): Unit = {
          val canPull = shouldPull
          fizzDemand = true
          if (shouldPull) {
            tryPull(inlet)
          }
        }
      })

      setHandler(outletBuzz, new OutHandler {
        override def onPull(): Unit = {
          val canPull = shouldPull
          buzzDemand = true
          if (shouldPull) {
            tryPull(inlet)
          }
        }
      })

      setHandler(outletFizzBuzz, new OutHandler {
        override def onPull(): Unit = {
          val canPull = shouldPull
          fizzbuzzDemand = true
          if (shouldPull) {
            tryPull(inlet)
          }
        }
      })
    }
  }
}
