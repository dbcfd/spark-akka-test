package sparkka

object FizzBuzz {
  case object Fizz extends Fizz
  case object Buzz extends Buzz
  case object FizzBuzz extends Fizz with Buzz
}

sealed trait FizzOrBuzzOrFizzBuzz
sealed trait Fizz extends FizzOrBuzzOrFizzBuzz
sealed trait Buzz extends FizzOrBuzzOrFizzBuzz
