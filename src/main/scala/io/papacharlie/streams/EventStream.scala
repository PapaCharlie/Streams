package io.papacharlie.streams

import com.twitter.concurrent.AsyncStream
import com.twitter.util.Future

abstract class EventStream {
  def eventConsumer: StreamEvent => Future[Unit]
  def committer: StreamOffsetCommitter

  protected def mkStream(): AsyncStream[StreamEvent]

  /**
   * Start asking for more events from the stream and consuming them. Commits will happen based on
   * the commit policy defined in [[committer]].
   *
   * This Future will become defined if [[committer.stop()]] or [[committer.forceStop()]] are
   * called, of if [[committer.fatalExceptions]] is set to true and [[eventConsumer]] returns a
   * failed Future.
   */
  lazy val consume: Future[Unit] = {
    // Start consuming the stream, and giving the committer new events.
    mkStream().foreachF(event => committer.recv(event.streamOffset, eventConsumer(event)))
    // Start the committer
    committer.start
  }
}
