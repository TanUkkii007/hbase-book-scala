package util

trait LoanPattern {
  def tryWithResource[T](resource: => T)(f: T => Unit) = {
    try resource finally {
      f(resource)
    }
  }
}
