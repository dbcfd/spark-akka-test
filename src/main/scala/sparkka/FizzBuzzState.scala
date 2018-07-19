package sparkka

/**
  * Enum like representation of possible fizz buzz values. Mainly because scala enums can be annoying to work with.
  */
object FizzBuzzState {
  case object Fizz extends FizzBuzzState
  case object Buzz extends FizzBuzzState
  case object FizzBuzz extends FizzBuzzState

  val values = 3

  def unapply(arg: String): Option[FizzBuzzState] = arg match {
    case "fizz" => Option(Fizz)
    case "buzz" => Option(Buzz)
    case "fizzbuzz" => Option(FizzBuzz)
    case _ => Option.empty
  }
}

sealed trait FizzBuzzState
