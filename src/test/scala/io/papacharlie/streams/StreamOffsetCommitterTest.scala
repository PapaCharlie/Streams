package io.papacharlie.streams

import com.twitter.conversions.time._
import com.twitter.util._
import io.papacharlie.streams.StreamOffsetCommitter.CommitterStoppedException
import org.specs2.mutable.Specification
import org.specs2.specification.Scope

class StreamOffsetCommitterTest extends Specification {
  private def isResolved(f: Future[_]): Boolean = {
    try {
      Await.result(f, 5.milliseconds)
      true
    } catch {
      case _: TimeoutException => false
    }
  }

  trait CommitterScope extends Scope {
    protected implicit val timer = new JavaTimer
    protected def maxTimeBetweenCommits: Duration = 1.second
    protected val committer = new TestStreamOffsetCommitter(maxTimeBetweenCommits)
    committer.start
    protected def offset(i: Int): String = s"offset$i"
  }

  "StreamOffsetCommitter" >> {
    "commits a batch" in new CommitterScope {
      Await.result(
        for {
          _ <- committer.recv(offset(1), Future.Unit)
          _ <- committer.recv(offset(2), Future.Unit)
          _ = committer.stop()
          _ <- committer.start
        } yield (),
        5.seconds
      )
      committer.commits.length must beEqualTo(1)
      committer.commits.head._2 must beEqualTo(offset(2))
    }

    "commits multiple batches" in new CommitterScope {
      Await.result(
        for {
          _ <- committer.recv(offset(1), Future.Unit)
          _ <- committer.recv(offset(2), Future.Unit)
          _ <- committer.recv(offset(3), Future.Unit)
          _ <- committer.recv(offset(4), Future.Unit)
          _ = committer.stop()
          _ <- committer.start
        } yield (),
        5.seconds
      )
      committer.commits.length must beEqualTo(2)
      committer.commits.head._2 must beEqualTo(offset(2))
      committer.commits(1)._2 must beEqualTo(offset(4))
    }

    "shorts a long lasting event" in new CommitterScope {
      Await.result(
        for {
          _ <- committer.recv(offset(1), Future.Unit)
          _ <- committer.recv(offset(2), Future.sleep(20.seconds))
        } yield (),
        5.seconds
      )
      Await.result(Future.sleep(2.seconds))
      committer.commits.length must beEqualTo(1)
      committer.commits.head._2 must beEqualTo(offset(1))
    }

    "waits for commits to finish before receiving new events" in {
      val promise = new Promise[Unit]()
      val committer = new StreamOffsetCommitter {
        def fatalExceptions: Boolean = true
        def maxEventsBetweenCommits: Int = 1
        def maxTimeBetweenCommits: Duration = Duration.Top
        def maxEventsInFlight: Int = Int.MaxValue
        def commit(offset: String): Future[Unit] = promise
      }
      committer.start
      val future = committer.recv("", Future.Unit)
      isResolved(future) must beFalse
      promise.setDone()
      Await.result(future, 1.second)
      ok
    }

    "does not exceed the `maxEventsInFlight` limit" in {
      val committer = new StreamOffsetCommitter {
        def fatalExceptions: Boolean = true
        def maxEventsBetweenCommits: Int = Int.MaxValue
        def maxTimeBetweenCommits: Duration = Duration.Top
        def maxEventsInFlight: Int = 1
        def commit(offset: String): Future[Unit] = Future.Unit
      }
      committer.start
      val promise1 = new Promise[Unit]()
      val future1 = committer.recv("", promise1)
      val promise2 = new Promise[Unit]()
      val future2 = committer.recv("", promise2)
      isResolved(future1) must beTrue
      isResolved(future2) must beFalse
      promise1.setDone()
      isResolved(future2) must beTrue
      Await.result(future2, 1.second)
      ok
    }

    "stops but waits for the current batch to finish" in new CommitterScope {
      Await.result(
        for {
          _ <- committer.recv(offset(1), Future.Unit)
          _ = committer.stop()
          _ <- committer.start
        } yield (),
        5.seconds
      )
      committer.commits.length must beEqualTo(1)
      committer.commits.head._2 must beEqualTo(offset(1))
    }

    "force stops" in new CommitterScope {
      Await.result(
        for {
          _ <- committer.recv(offset(1), Future.Unit)
          _ = committer.forceStop()
          _ <- committer.start
        } yield (),
        5.seconds
      )
      committer.commits.length must beEqualTo(0)
    }

    "always commits when the timer runs out" in new CommitterScope {
      Await.result(committer.recv(offset(1), Future.Unit), 5.seconds)
      Await.result(Future.sleep(3.seconds), 5.seconds)
      committer.commits.length must beEqualTo(1)
      committer.commits.head._2 must beEqualTo(offset(1))
    }

    "does not accept events after having been stopped" in new CommitterScope {
      committer.stop()
      Await.result(committer.recv("", Future.Unit).liftToTry) match {
        case Return(_) => failure("Committer accepted new value after stop() was called.")
        case Throw(_: CommitterStoppedException) => ok
        case Throw(ex) => throw ex
      }
    }
  }
}
