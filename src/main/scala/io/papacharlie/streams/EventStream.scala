package io.papacharlie.streams

import com.twitter.concurrent.AsyncStream
import com.twitter.util.Future
import org.joda.time.DateTime

abstract class EventStream {
  def eventConsumer: StreamEvent => Future[Unit]
  def committer: StreamOffsetCommitter

  protected def mkStream(): AsyncStream[StreamEvent]

  private def foldStart: (DateTime, Int) = (DateTime.now(), 0)
  private def commitFold(
    acc: (DateTime, Int),
    event: Future[StreamEvent]
  ): Future[(DateTime, Int)] = {
    
  }

  lazy val consume: Future[Unit] = {
    mkStream()
      // Lazily consume the events
      .map(event => eventConsumer(event) before Future.value(event))
      // Actually start consuming and committing
      .foldLeftF(foldStart)(commitFold)
      .unit
  }
}

