package io.papacharlie.streams

import com.twitter.conversions.time._
import com.twitter.util._
import org.specs2.mutable.Specification
import org.specs2.specification.Scope

class AsyncConditionTest extends Specification {
  private trait ConditionScope extends Scope {
    private val timer = new JavaTimer()
    val condition: AsyncCondition = new AsyncCondition(timer)
  }
  "AsyncConditionTest" >> {
    "must asyncWait" in new ConditionScope {
      val promise = new Promise[String]()
      val future = condition.asyncWait().ensure(promise.setValue("test"))
      condition.asyncNotifyAll()
      Await.result(promise, 5.seconds) must beEqualTo("test")
    }

    "must asyncWait(duration)" in new ConditionScope {
      val start = Time.now
      Await.result(condition.asyncWait(10.milliseconds), 5.seconds)
      Time.now - start <= 5.milliseconds
    }

    "must wait" in new ConditionScope {
      Try(Await.result(condition.asyncWait(), 10.milliseconds)) match {
        case Throw(_: TimeoutException) => ok
        case _ => throw new Exception("Did not wait")
      }
    }

    "asyncNotifyAll must not make subsequent calls to asyncWait not wait" in new ConditionScope {
      condition.asyncNotifyAll()
      val future = condition.asyncWait()
      future.isDefined must beFalse
      condition.asyncNotifyAll()
      Await.result(future, 1.second)
    }
  }
}
