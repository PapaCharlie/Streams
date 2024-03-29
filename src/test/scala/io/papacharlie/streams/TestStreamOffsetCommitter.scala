package io.papacharlie.streams

import com.twitter.util.{Duration, Future, Time}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

class TestStreamOffsetCommitter(val maxTimeBetweenCommits: Duration) extends StreamOffsetCommitter {
  def fatalExceptions: Boolean = true
  def maxEventsBetweenCommits = 2
  def maxEventsInFlight: Int = Int.MaxValue
  def commit(offset: String) = Future.value(_commits.add((Time.now, offset)))

  private[this] val _commits: ConcurrentLinkedQueue[(Time, String)] = new ConcurrentLinkedQueue()
  def commits: Seq[(Time, String)] = _commits.asScala.toIndexedSeq
}
