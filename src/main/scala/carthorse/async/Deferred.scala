package scalabase.async

import java.{util => ju}

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

import com.stumbleupon.async.Callback
import com.stumbleupon.{ async => sua }
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeoutException
import org.slf4j.LoggerFactory

/**
 * A wrapper around [[com.stumbleupon.async.Deferred]] which provides type-safety and a convenient
 * API at the expense of slightly more memory usage and object creation overhead.
 *
 * The `Deferred` API is modeled after [[scala.concurrent.Future]], but has slightly different
 * semantics about when callbacks will be executed due to the underlying reliance on
 * [[com.stumbleupon.async.Deferred]]. If a deferred is complete, then adding a callback
 * will cause the callback to execute and complete immediately in the same thread. If a deferred is
 * incomplete the callback will be executed at a later time by the thread which delivers a value to
 * the deferred.
 */
class Deferred[T](private val repr: sua.Deferred[T]) {
  import Deferred._

  val Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * When this deferred is completed, either through an exception, or a value, apply the provided
   * function. Primarily used for side-effecting functions.  Chained calls to onComplete will
   * happen in invocation order.
   *
   * If the deferred has already been completed, the function will be applied immediately.
   *
   * If an exception is thrown while invoking the function, the exception will be logged and
   * swallowed.
   */
  def onComplete[U](f: Try[T] => U): this.type = {
    repr.addCallback { x: T =>
      try { f(Success(x)); x}
      catch { case e: Exception =>
        Logger.warn("Exception caught during successful callback of deferred.", e)
        x
      }
    }
    repr.addErrback { e: Exception =>
      try { f(Failure(e)); e }
      catch { case thrown: Exception =>
        Logger.warn("Exception caught during failure callback of deferred.", thrown)
        e
      }
    }
    this
  }

  /**
   * When this deferred is completed successfully (i.e. with a value), apply the provided partial
   * function to the value if the partial function is defined at that value. Chained calls to
   * onSuccess will happen in invocation order.
   *
   * If the deferred has already been completed with a value, the partial function will be applied
   * immediately.
   *
   * If the partial function throws an exception, the exception will be logged and swallowed.
   */
  def onSuccess[U](pf: PartialFunction[T, U]): this.type = onComplete {
    case Success(v) => pf.applyOrElse[T, Any](v, Predef.conforms[T]) // Exploiting the cached function to avoid MatchError
    case _ =>
  }

  /**
   * When this deferred is completed with a failure (i.e. with an `Exception`), apply the provided
   * callback to the exception.  Chained calls to onFailure will happen in invocation order.
   *
   * If the deferred has already been completed with a failure, the partial function will be applied
   * immediately.
   *
   * Will not be called in case that the deferred is completed with a value.
   *
   * If the partial function throws an exception, the exception will be logged and swallowed.
   */
  def onFailure[U](pf: PartialFunction[Exception, U]): this.type = onComplete {
    case Failure(t: Exception) => pf.applyOrElse[Exception, Any](t, Predef.conforms[Exception]) // Exploiting the cached function to avoid MatchError
    case _ =>
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
   * Returns a new [[com.stumbleupon.async.Deferred]] chained to this one.  When this deferred is
   * completed, its value will be passed to the chained deferred, and the chained deferred's
   * callbacks will be executed.
   */
  private def chain: sua.Deferred[T] = { val d = new sua.Deferred[T]; repr.chain(d); d }

  /**
   * Creates a new deferred by applying a function to the successful result of this deferred. If
   * this deferred is completed with an exception, then the new deferred will also contain this
   * exception.
   *
   * `map` should not be used when the result type, `S`, is a deferred. Use `flatMap` instead.
   *
   * @param f to apply to the successful result of this deferred.
   * @tparam S result type of function f.
   * @return a new deferred containing the result of f applied to the result of this deferred,
   *         or the error.
   */
  def map[S](f: T => S): Deferred[S] = chain.addCallback(f)

  /**
   * Creates a new deferred by applying a function to the successful result of this deferred which
   * returns another deferred.  If this deferred is completed with an exception, then the new
   * deferred will also contain this exception.
   *
   * @param f to apply to the successful result of this deferred.
   * @tparam S deferred result type of function f.
   * @return a new deferred containing the result of f applied to the result of this deferred,
   *         or the error.
   */
  def flatMap[S](f: T => Deferred[S]): Deferred[S] = chain.addCallbackDeferring(f.andThen(_.repr)): sua.Deferred[S]

  /**
   * Creates a new deferred by filtering the value of the current deferred with a predicate.
   *
   * If the current deferred holds a value that satisfies the predicate, the new deferred will also
   * hold that value. Otherwise, the resulting deferred will fail with a `NoSuchElementException`.
   *
   * If the current deferred fails, then the resulting deferred also fails.
   *
   * @param pred with which to filter a successful value of this deferred.
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

  /**
   * Zips the values of `this` and `that` deferred, and creates a new deferred holding the tuple of
   * their results.
   *
   * If either deferred fails, the resulting deferred will fail and contain a
   * [[com.stumbleupon.async.DeferredGroupException]].
   */
  def zip[U](that: Deferred[U]): Deferred[(T, U)] = {
    // sua.Deferred is not covariant
    val ds: Seq[sua.Deferred[Any]] =
      List(this.repr.asInstanceOf[sua.Deferred[Any]], that.repr.asInstanceOf[sua.Deferred[Any]])
    sua.Deferred
      .groupInOrder(ds.asJavaCollection)
      .addCallback((xs: ju.ArrayList[Any]) =>
        xs.asScala.toList match { case (t: T) :: (u: U) :: Nil => t -> u })
  }

  /**
   * Synchronously block until the result is available, and return it.  If the result is a failure,
   * the contained exception will be thrown.
   *
   * @throws java.lang.InterruptedException if the thread is interrupted while waiting.
   * @throws java.lang.Exception when the deferred is a failure, the contained exception will be thrown.
   * @return the result of the deferred.
   */
  @throws(classOf[InterruptedException])
  @throws(classOf[Exception])
  def result(): T = {
    repr.join()
  }

  /**
   * Synchronously block until the result is available, or until the specified time is elapsed, and
   * return the result.
   *
   * @param atMost maximum time to wait.
   * @throws java.util.concurrent.TimeoutException if timeout elapses.
   * @throws java.lang.InterruptedException if thread is interrupted while waiting.
   * @throws java.lang.Exception when the deferred is a failure, the contained exception will be thrown.
   * @return the result of deferred.
   */
  @throws(classOf[TimeoutException])
  @throws(classOf[InterruptedException])
  @throws(classOf[Exception])
  def result(atMost: Duration): T = {
    try {
      val ms = atMost.toMillis
      if (ms <= 0) repr.join(0L)
      else repr.join(ms)
    } catch { // Convert sua.TimeoutException to juc.TimeoutException
      case e: sua.TimeoutException => throw new TimeoutException(e.getMessage)
    }
  }

  override def toString: String = repr.toString
}

object Deferred {
  /**
   * Converts a Scala function to a [[com.stumbleupon.async.Callback]].
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

  /** Provides an implicit conversion from [[com.stumbleupon.async.Deferred]] to [[scalabase.async.Deferred]]. */
  implicit def su2Scala[T](repr: sua.Deferred[T]): Deferred[T] = new Deferred[T](repr)

  /** Provides an implicit conversion from [[scalabase.async.Deferred]] to [[com.stumbleupon.async.Deferred]]. */
  implicit def scala2Su[T](deferred: Deferred[T]): sua.Deferred[T] = deferred.repr

  def apply[T](t: Try[T]): Deferred[T] = t match {
    case Success(value) => sua.Deferred.fromResult(value)
    case Failure(error: Exception) => sua.Deferred.fromError[T](error)
  }

  /**
   * Transforms a `Seq[Deferred[T]]` into a `Deferred[Seq[T]]`. Useful for reducing many `Deferred`s
   * into a single `Deferred`.  If any of the `Deferred`s fail, then the resulting deferred will
   * fail with a [[com.stumbleupon.async.DeferredGroupException]].  The relative ordering of the
   * inputs will be preserved in the result.
   */
  def sequence[A](xs: Seq[Deferred[A]]): Deferred[Seq[A]] = {
    sua.Deferred
      .groupInOrder(xs.map(_.repr).asJavaCollection)
      .addCallback((xs: ju.ArrayList[A]) => xs.asScala.toSeq)
  }

  /**
   * A non-blocking fold over the specified `Deferred`s, with the start value of the given `z`.
   * Preserves the ordering of inputs during the fold. The fold is performed on the thread where the
   * last future is complete, or on the calling thread if all futures are already completed.
   * The result will be a failure if any of the input `Deferred`s fail, or if the fold function `f`
   * throws an exception.
   */
  def fold[T, R](xs: Seq[Deferred[T]])(z: R)(f: (R, T) => R): Deferred[R] = {
    if (xs.isEmpty) Deferred.successful(z)
    else sequence(xs).map(_.foldLeft(z)(f))
  }

  /**
   * A non-blocking fold over the specified `Deferred`s, with the start value of the given `z`.
   * Preserves the ordering of inputs during the fold. The fold is performed on the thread where the
   * last future is complete, or on the calling thread if all futures are already completed.
   * The result will be a failure if any of the input `Deferred`s fail, or if the fold function `f`
   * throws an exception.
   */
  def reduce[T, R >: T](xs: Seq[Deferred[T]])(op: (R, T) => R): Deferred[R] = {
    if (xs.isEmpty) Deferred.failed(new NoSuchElementException("reduce attempted on empty collection"))
    else sequence(xs).map(_ reduceLeft op)
  }

  def successful[T](v: T): Deferred[T] = sua.Deferred.fromResult(v)

  def failed[T](e: Exception): Deferred[T] = sua.Deferred.fromError[T](e)
}
