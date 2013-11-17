package scalabase

import org.scalatest.FunSuite

import com.stumbleupon.async.Deferred
import scala.util.Try
import scala.concurrent.Future

class RichDeferredSuite extends FunSuite {

  test("Deferred can be implicitly converted to RichDeferred.") {
    val a: RichDeferred[Int] = Deferred.fromResult(123)
    val b: RichDeferred[Int] = Deferred.fromError[Int](new RuntimeException)
  }

  test("RichDeferred can be implicitly converted to Deferred") {
    RichDeferred.successful(123): Deferred[Int]
    RichDeferred.failed(new RuntimeException): Deferred[Int]
  }

  ignore("onSuccess will be called when a deferred is successful") {
    val s: String = "foo"
    val d: RichDeferred[String] = Deferred.fromResult(s)
    println(d)
    d.onComplete{ x: Try[String] => println(x.get) }
    println(d)
    d.onComplete(x => println(x))
    println(d)
  }

  test("Scala future") {
    import scala.concurrent.ExecutionContext.Implicits.global
    val d: Future[Int] = Future.successful(123)
    println(d)
    d.onComplete(x => println(x))
    println(d)
    d.onSuccess {case s: Int => println("partial " + s)}
    println(d)
  }

}
