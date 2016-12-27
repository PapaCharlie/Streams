package io.papacharlie.streams

import com.twitter.util.{Duration, Future}

abstract class StreamOffsetCommitter {
  /**
   * The maximum amount of time to wait between commits
   */
  def maxTimeBetweenCommits: Duration

  /**
   * The maximum number of processed events to leave uncommitted
   */
  def maxEventsBetweenCommits: Int

  def processInBatches: Boolean

  /**
   * Based on `lastCheckpoint` and `eventsConsumedSinceCheckpoint`, decide if `event`'s offset
   * needs to be committed.
   *
   * @return A Future indicating whether or not the offset was committed
   */
  def commit(offset: String): Future[Unit]
}
