package io.papacharlie.streams

import com.twitter.conversions.time._
import com.twitter.util.{Duration, Future, Time}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

class TestStreamOffsetCommitter(val maxTimeBetweenCommits: Duration) extends StreamOffsetCommitter {
  def maxEventsBetweenCommits = 2
  def processInBatches = false
  def commit(offset: String) = Future.value(_commits.add((Time.now, offset)))

  private[this] val _commits: ConcurrentLinkedQueue[(Time, String)] = new ConcurrentLinkedQueue()
  def commits: Seq[(Time, String)] = _commits.asScala.toIndexedSeq
}
