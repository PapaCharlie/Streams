package io.papacharlie.streams

import com.twitter.conversions.time._
import com.twitter.util._
import io.papacharlie.streams.StreamOffsetCommitter.CommitterStoppedException
import org.specs2.mutable.Specification
import org.specs2.specification.Scope

class StreamOffsetCommitterTest extends Specification {
  trait CommitterScope extends Scope {
    protected implicit val timer = new JavaTimer
    protected def maxTimeBetweenCommits: Duration = 1.second
    protected val committer = new TestStreamOffsetCommitter(maxTimeBetweenCommits)
    protected def offset(i: Int) = s"offset$i"
  }

  "StreamOffsetCommitter" >> {
    "commits a batch" in new CommitterScope {
      Await.result(
        for {
          _ <- committer.recv(Future.value(offset(1)))
          _ <- committer.recv(Future.value(offset(2)))
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
          _ <- committer.recv(Future.value(offset(1)))
          _ <- committer.recv(Future.value(offset(2)))
          _ <- committer.recv(Future.value(offset(3)))
          _ <- committer.recv(Future.value(offset(4)))
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
          _ <- committer.recv(Future.value(offset(1)))
          _ <- committer.recv(Future.value(offset(2)).delayed(20.seconds))
        } yield (),
        5.seconds
      )
      Await.result(Future.Unit.delayed(2.seconds))
      committer.commits.length must beEqualTo(1)
      committer.commits.head._2 must beEqualTo(offset(1))
    }

    "stops but waits for the current batch to finish" in new CommitterScope {
      Await.result(
        for {
          _ <- committer.recv(Future.value(offset(1)))
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
          _ <- committer.recv(Future.value(offset(1)))
          _ = committer.forceStop()
          _ <- committer.start
        } yield (),
        5.seconds
      )
      committer.commits.length must beEqualTo(0)
    }

    "always commits when the timer runs out" in new CommitterScope {
      Await.result(committer.recv(Future.value(offset(1))), 5.seconds)
      Await.result(Future.Unit.delayed(3.seconds), 5.seconds)
      committer.commits.length must beEqualTo(1)
      committer.commits.head._2 must beEqualTo(offset(1))
    }

    "does not accept events after having been stopped" in new CommitterScope {
      committer.stop()
      Await.result(committer.recv(Future.value("")).liftToTry) match {
        case Return(_) => failure("Committer accepted new value after stop() was called.")
        case Throw(_: CommitterStoppedException) => ok
        case Throw(ex) => throw ex
      }
    }
  }
}


