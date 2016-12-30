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
      val future = condition().ensure(promise.setValue("test"))
      condition.asyncNotifyAll()
      Await.result(promise, 5.seconds) must beEqualTo("test")
    }

    "must asyncWait(duration)" in new ConditionScope {
      Await.result(condition(1.second), 5.seconds)
    }

    "must wait" in new ConditionScope {
      Try(Await.result(condition.asyncWait(), 1.second)) match {
        case Throw(ex: TimeoutException) => ok
        case _ => throw new Exception("Did not wait")
      }
    }
  }
}
