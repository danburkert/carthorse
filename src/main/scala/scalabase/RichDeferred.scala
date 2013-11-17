package scalabase

import scala.util.{Try, Failure, Success}

import com.stumbleupon.async.{Callback, Deferred}

/**
 * Provides an interface over [[com.stumbleupon.async.Deferred]] modeled after
 * Scala's [[scala.concurrent.Future]].
 */
class RichDeferred[T](private val deferred: Deferred[T]) {
  import RichDeferred._

  /**
   * When this deferred is completed successfully (i.e. with a value), apply the provided partial
   * function to the value if the partial function is defined at that value.
   *
   * If the deferred has already been completed with a value, the partial function will be applied
   * immediately.
   */
  def onSuccess[U](pf: PartialFunction[T, U]): Unit = onComplete {
    case Success(v) =>
      pf.applyOrElse[T, Any](v, Predef.conforms[T]) // Exploiting the cached function to avoid MatchError
    case _ =>
  }

  /**
   * When this deferred is completed with a failure (i.e. with a throwable), apply the provided
   * callback to the throwable.
   *
   * If the deferred has already been completed with a failure, the partial function will be applied
   * immediately.
   *
   * Will not be called in case that the deferred is completed with a value.
   */
  def onFailure[U](callback: PartialFunction[Throwable, U]): Unit = onComplete {
    case Failure(t) =>
      callback.applyOrElse[Throwable, Any](t, Predef.conforms[Throwable]) // Exploiting the cached function to avoid MatchError
    case _ =>
  }

  /**
    * When this deferred is completed, either through an exception, or a value, apply the provided
    * function.
    *
    * If the deferred has already been completed, the partial function will be applied immediately.
    */
  def onComplete[U](func: Try[T] => U): Unit = {
    deferred.addCallback((x: T) => func(Success(x)))
    deferred.addErrback((e: Throwable) => func(Failure(e)))
  }

  /** Monadic operations */

  /**
   * Asynchronously processes the value when it becomes available.
   *
   * Will not be called if the defer fails.
   *
   * @param f function to apply to apply with `foreach` over the resulting value.
   * @tparam U result type of function to apply.
   */
  def foreach[U](f: T => U): Unit = onComplete { _ foreach f }

  /**
   * Creates a new [[com.stumbleupon.async.Deferred]] by applying a function to the successful
   * result of this deferred. If this deferred is completed with an exception then the new deferred
   * will also contain this exception.
   *
   * @param f to apply to the successful result of this deferred.
   * @tparam S result type of function f.
   * @return a new deferred containing the result of f applied to the result of this deferred,
   *         or the error.
   */
  def map[S](f: T => S): Deferred[S] = deferred addCallback f

  /**
   * Creates a new [[com.stumbleupon.async.Deferred]] by applying a function to the successful
   * result of this deferred which returns another deferred.  If this deferred is completed with an
   * exception then the new deferred will also contain this exception.
   *
   * @param f to apply to the successful result of this deferred.
   * @tparam S deferred result type of function f.
   * @return a new deferred containing the result of f applied to the result of this deferred,
   *         or the error.
   */
  def flatMap[S](f: T => Deferred[S]): Deferred[S] = deferred addCallbackDeferring f

  /**
   * Creates a new deferred by filtering the value of the current deferred with a predicate.
   *
   * If the current deferred holds a value that satisfies the predicate, the new deferred will also
   * hold that value. Otherwise, the resulting deferred will fail with a `NoSuchElementException`.
   *
   * If the current deferred fails, then the resulting deferred also fails.
   *
   * @param pred with which to filter successfull value of this deferred.
   * @return a new deferred.
   */
  def filter(pred: T => Boolean): Deferred[T] =
    map {
      r => if (pred(r)) r
           else throw new NoSuchElementException("Deferred.filter predicate is not satisfied")
    }

  /**
   * Used by for-comprehensions.
   */
  final def withFilter(p: T => Boolean): Deferred[T] = filter(p)

  /**
   * Creates a new deferred by mapping the value of the current deferred, if the given partial function is defined at that value.
   *
   * If the current deferred contains a value for which the partial function is defined, the new deferred will also hold that value.
   * Otherwise, the resulting deferred will fail with a `NoSuchElementException`.
   *
   * If the current deferred fails, then the resulting deferred also fails.
   *
   * Example:
   * {{{
   * val f = RichDeferred(Success(-5))
   * val g = f collect {
   *   case x if x < 0 => -x
   * }
   * val h = f collect {
   *   case x if x > 0 => x * 2
   * }
   * Await.result(g, Duration.Zero) // evaluates to 5
   * Await.result(h, Duration.Zero) // throw a NoSuchElementException
   * }}}
   */
  def collect[S](pf: PartialFunction[T, S]): Deferred[S] =
    map {
      r => pf.applyOrElse(r, (t: T) => throw new NoSuchElementException("Deferred.collect partial function is not defined at: " + t))
    }



  override def toString: String = "RichDeferred(" + deferred + ")"
}


object RichDeferred {
  /**
   * Converts a `Scala` [[scala.Function1]] to a [[com.stumbleupon.async.Callback]].
   *
   * @param f function to convert to a callback.
   * @tparam T argument type of function (and resulting callback).
   * @tparam R result type of function (and resulting callback).
   * @return a callback.
   */
  private implicit def functionToCallback[T, R](f: T => R): Callback[R, T] = {
    new Callback[R, T] {
      override def call(arg: T): R = f(arg)
      override val toString: String = "RichDeferred callback"
    }
  }

  /** Provides an implicit conversion from [[com.stumbleupon.async.Deferred]] to [[scalabase.RichDeferred]]. */
  implicit def deferredToRichDeferred[T](deferred: Deferred[T]): RichDeferred[T] = new RichDeferred(deferred)

  /** Provides an implicit conversion from [[scalabase.RichDeferred]] to [[com.stumbleupon.async.Deferred]]. */
  implicit def richDeferredToDeferred[T](richDeferred: RichDeferred[T]): Deferred[T] = richDeferred.deferred

  def apply[T](t: Try[T]): Deferred[T] = t match {
    case Success(value) => Deferred.fromResult(value)
    case Failure(error: Exception) => Deferred.fromError(error)
  }

  def successful[T](v: T): Deferred[T] = Deferred.fromResult(v)
  def failed[T](e: Exception): Deferred[T] = Deferred.fromError(e)
}
