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

/**
  * Split a set of fizz buzz inputs into multiple streams aggregating counts and stats
  */
class FizzBuzzer extends GraphStageWithMaterializedValue[UniformFanOutShape[FizzBuzzState, Int], Future[FizzBuzzer.Stats]] {
  val inlet = Inlet[FizzBuzzState]("fb-in")
  val outletBuzz = Outlet[Int](name = "buzz-out")
  val outletFizz = Outlet[Int](name = "fizz-out")
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
      var total = FizzBuzzer.Stats(0, 0, 0)

      override def postStop(): Unit = {
        promiseStats.trySuccess(total)
      }

      setHandler(inlet, new InHandler {
        override def onPush(): Unit = {
          grab(inlet) match {
            case FizzBuzzState.FizzBuzz =>
              total = total.markFizzBuzz()
              fizzbuzzCount += 1
              if (!isClosed(outletFizzBuzz) && isAvailable(outletFizzBuzz) && fizzbuzzDemand) {
                push(outletFizzBuzz, fizzbuzzCount)
                fizzbuzzCount = 0
                fizzbuzzDemand = false
              }
            case FizzBuzzState.Fizz =>
              total = total.markFizz()
              fizzCount += 1
              if (!isClosed(outletFizzBuzz) && isAvailable(outletFizzBuzz) && fizzDemand) {
                push(outletFizz, fizzCount)
                fizzCount = 0
                fizzDemand = false
              }
            case FizzBuzzState.Buzz =>
              total = total.markBuzz()
              buzzCount += 1
              if (buzzDemand && !isClosed(outletBuzz) && isAvailable(outletBuzz)) {
                push(outletBuzz, buzzCount)
                buzzCount = 0
                buzzDemand = false
              }
          }

          if(!hasBeenPulled(inlet) && (buzzDemand || fizzDemand || fizzbuzzDemand)) {
            tryPull(inlet)
          }
        }
      })

      setHandler(outletFizz, new OutHandler {
        override def onPull(): Unit = {
          fizzDemand = true
          if (!hasBeenPulled(inlet)) {
            tryPull(inlet)
          }
        }
      })

      setHandler(outletBuzz, new OutHandler {
        override def onPull(): Unit = {
          buzzDemand = true
          if (!hasBeenPulled(inlet)) {
            tryPull(inlet)
          }
        }
      })

      setHandler(outletFizzBuzz, new OutHandler {
        override def onPull(): Unit = {
          fizzbuzzDemand = true
          if (!hasBeenPulled(inlet)) {
            tryPull(inlet)
          }
        }
      })
    }

    logic -> promiseStats.future
  }
}
