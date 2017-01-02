package io.papacharlie.streams

import com.twitter.concurrent.AsyncStream
import com.twitter.util.Future

abstract class EventStream {
  def eventConsumer: StreamEvent => Future[Unit]
  def committer: StreamOffsetCommitter

  protected def mkStream(): AsyncStream[StreamEvent]

  lazy val consume: Future[Unit] = {
    mkStream()
      // Lazily consume the events
      .map(event => eventConsumer(event) before Future.value(event.streamOffset))
      // Actually start consuming and committing. Since `committer.recv` returns "slow" Futures
      // while committing, this foreachF will not call for more events until the old ones have been
      // committed.
      .foreachF(committer.recv)
      .unit
  }
}
