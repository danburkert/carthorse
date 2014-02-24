package carthorse.async

import java.util.concurrent.TimeoutException

import scala.RuntimeException
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

import com.stumbleupon.async.DeferredGroupException
import com.stumbleupon.{async => sua}
import org.scalatest.{Matchers, WordSpec}

class DeferredSpec extends WordSpec with Matchers {

  "A sua.Deferred" can {
    "be explicitly converted to a Deferred" in {
      Deferred.ch2su(Deferred.successful(123)): sua.Deferred[Int]
      Deferred.ch2su[Int](Deferred.failed[Int](new RuntimeException)): sua.Deferred[Int]
    }
  }

  "A Deferred" can {
    "be implicitly converted to a sua.Deferred" in {
      sua.Deferred.fromResult(123): Deferred[Int]
      sua.Deferred.fromError[Int](new RuntimeException): Deferred[Int]
    }
  }

  "A Deferred" when {
    "successful" should {
      val value: Int = 123
      val deferred: Deferred[Int] = Deferred.successful(value)
      "pass its value in a Success to an onComplete callback" in {
        var res: Try[Int] = null
        deferred.onComplete(t => res = t)
        res should equal (Success(value))
      }
      "pass its value to an onSuccess callback" in {
        var res: Int = 0
        deferred.onSuccess { case x: Int => res = x }
        res should equal (value)
      }
      "not trigger an onFailure callback" in {
        var called = false
        deferred.onFailure { case _ => called = true }
        called should be (false)
      }
      "trigger chained callbacks in order" in {
        var success = ""
        deferred
          .onComplete { case _: Try[Int] => success += "a" }
          .onSuccess  { case _: Int => success += "b" }
          .onSuccess  { case i: Int if i == 42 => "SHOULDN'T HAPPEN" }
          .onFailure  { case _: Exception => success += "SHOULDN'T HAPPEN" }
          .onSuccess  { case _: Int => success += "c" }
        success should equal ("abc")
      }
      "swallow exceptions thrown from callbacks" in {
        var res = ""
        deferred
          .onComplete { case _: Try[Int] => res += "a"; throw new Exception("foo") }
          .onSuccess  { case _: Int => res += "b"; throw new Exception("bar") }
          .onFailure { case _: Exception => res += "!"; throw new Exception("baz") }
          .onSuccess { case i: Int if i == value => res += "c" }
        res should equal ("abc")
      }
      "trigger foreach callbacks" in {
        var called = false
        deferred.foreach(i => called = true)
        called should equal (true)
      }
      "return a new deferred when mapped over" in {
        val newVal = "foo"
        deferred.map(i => newVal).result should equal (newVal)
        deferred.result should equal (value) // check the original wasn't mutated
      }
      "return a new deferred containing the new value when flatmapped with a success" in {
        val newVal = "foo"
        deferred.flatMap(i => Deferred.successful(newVal)).result should equal (newVal)
        deferred.result should equal (value)
      }
      "return a new deferred containing the new exception when flatmapped with a failure" in {
        val ex = new Exception("flatMap fail")
        var caught = false
        try {
          deferred.flatMap(i => Deferred.failed[Int](ex)).result
        } catch {
          case e: Exception if e.getMessage == ex.getMessage => caught = true
          case e: Exception => throw e
        }
        caught should be (true)
      }
      "return a new deferred containing the expression when filtered, if the predicate passes" in {
        deferred.filter(x => x > 100).result should equal (value)
      }
      "throw a NoSuchElementException when filtered, if the predicate fails" in {
        var caught = false
        try {
          deferred.filter(x => x < 100).result
        } catch {
          case e: NoSuchElementException => caught = true
          case e: Throwable => e
        }
        caught should be (true)
      }
    }

    "unsuccessful" should {
      val e = new RuntimeException("foo")
      val deferred = Deferred.failed[Int](e)
      "pass its exception in a Failure to an onComplete callback" in {
        var res: Try[Int] = null
        deferred.onComplete(t => res = t)
        res match { case Failure(ex: RuntimeException) => ex.getMessage should equal (e.getMessage)}
      }
      "pass its exception to an onFailure callback" in {
        var res: Exception = null
        deferred.onFailure { case ex: RuntimeException => res = ex }
        res match { case ex: RuntimeException => ex.getMessage should equal (e.getMessage)}
      }
      "not trigger an onSuccess callback" in {
        var called = false
        deferred.onSuccess { case _ => called = true }
        called should be (false)
      }
      "trigger chained callbacks in order" in {
        var success = ""
        deferred
          .onComplete { case _: Try[Int] => success += "a" }
          .onSuccess  { case _: Int => success += "SHOULDN'T HAPPEN" }
          .onFailure  { case _: ArrayIndexOutOfBoundsException => success += "SHOULDN'T HAPPEN" }
          .onFailure  { case _: RuntimeException => success += "b" }
          .onFailure  { case _: Exception => success += "c" }
        success should equal ("abc")
      }
      "swallow exceptions thrown from callbacks" in {
        var res = ""
        deferred
          .onComplete { case _: Try[Int] => res += "a"; throw new Exception("foo") }
          .onFailure  { case _: Exception => res += "b"; throw new Exception("bar") }
          .onSuccess  { case _: Int => res += "!"; throw new Exception("baz") }
          .onFailure  { case ex: RuntimeException if ex.getMessage == e.getMessage => res += "c" }
        res should equal ("abc")
      }
      "not trigger foreach callbacks" in {
        var called = false
        deferred.foreach(i => called = true)
        called should equal (false)
      }
      "return a new deferred when mapped over" in {
        var caught = false
        try {
          deferred.map(i => None).result
        } catch {
          case ex: RuntimeException if ex.getMessage == e.getMessage => caught = true
          case ex: Throwable => throw ex
        }
        caught should equal (true)
      }
      "return a new deferred containing the original failure when flatmapped over" in {
      }
      "return a new deferred containing the original exception when flatmapped with a success" in {
        var caught = false
        try {
          deferred.flatMap(i => Deferred.successful("foo")).result
        } catch {
          case ex: RuntimeException if ex.getMessage == e.getMessage => caught = true
          case ex: Throwable => throw ex
        }
        caught should equal (true)
      }
      "return a new deferred containing the original exception when flatmapped with a failure" in {
        val newException = new Exception("flatMap fail")
        var caught = false
        try {
          deferred.flatMap(i => Deferred.failed[Int](newException)).result
        } catch {
          case ex: Exception if ex.getMessage == e.getMessage => caught = true
          case ex: Exception => throw e
        }
        caught should be (true)
      }
    }

    "timed out" should {
      "throw a TimeoutException" in {
        val suaDef = new sua.Deferred[Int]
        Future { Thread.sleep(1000); suaDef.callback(123) }
        val deferred = new Deferred(suaDef)
        intercept[TimeoutException] { deferred.result(100.millis) }
      }
    }
  }

  "successful deferreds" when {
    "zipped" should {
      "return a new deferred containing a tuple of the successful values" in {
        Deferred.successful(123).zip(Deferred.successful("abc")).result should equal (123 -> "abc")
      }
    }
    "sequenced" should {
      "return a new deferred containing a seq of the successful values" in {
        Deferred.sequence(Seq(
          Deferred.successful(1),
          Deferred.successful(2),
          Deferred.successful(3)
        )).result should equal (Seq(1, 2, 3))
      }
    }
  }

  "failed deferreds" when {
    "zipped" should {
      "return a new failed deferred containing a DeferredGroupException" in {
        intercept[DeferredGroupException] {
          Deferred.successful(1).zip(Deferred.failed[Int](new Exception)).result
        }
      }
    }
    "sequenced" should {
      "return a new failed deferred containing a DeferredGroupException" in {
        intercept[DeferredGroupException] {
          Deferred.sequence(Seq(
            Deferred.successful(1),
            Deferred.successful(2),
            Deferred.failed[Int](new Exception)
          )).result
        }
      }
    }
  }
}
