package io.papacharlie.streams

import com.twitter.concurrent.{AsyncMutex, Offer}
import com.twitter.util._
import io.papacharlie.streams.StreamOffsetCommitter.CommitterStoppedException
import scala.collection.mutable

abstract class StreamOffsetCommitter {
  private[this] implicit val timer = new JavaTimer()

  /**
   * The maximum amount of time to wait between commits
   */
  def maxTimeBetweenCommits: Duration
  private[this] def newTimeout: Future[Unit] = Future.Unit.delayed(maxTimeBetweenCommits)

  /**
   * The maximum number of processed events to leave uncommitted
   */
  def maxEventsBetweenCommits: Int

  /**
   * When true, the committed will wait for exactly [[maxEventsBetweenCommits]] to be committed
   * before fetching a new batch
   */
  def processInBatches: Boolean

  /**
   * Based on `lastCheckpoint` and `eventsConsumedSinceCheckpoint`, decide if `event`'s offset
   * needs to be committed.
   *
   * @return A Future indicating whether or not the offset was committed.
   */
  def commit(offset: String): Future[Unit]

  /**
   * Receive a newly started event.
   *
   * Returns a Future that's immediately resolved if there is no pending commit and more events can
   * be accepted, otherwise a Future that becomes resolved once the pending events are committed.
   */
  def recv(event: Future[String]): Future[Unit] = mutex.acquireAndRun {
    if (isStopped) {
      throw new CommitterStoppedException("Cannot call `recv` after having called `stop()`!")
    }
    // Always start the timed committer. Since `start` is lazy, this will only happen once
    start
    // Either it's a fresh start or we've just finished a batch
    if (eventQueue.isEmpty) {
      nextCommit = newTimeout
      // Always tell `loop` to check for the new commit schedule when this lock is released
      newCommitReady.asyncNotifyAll()
    }

    eventQueue.enqueue(event)

    // Wait for as many of these events to complete as possible
    if (eventQueue.length >= maxEventsBetweenCommits) commit()
    // Or accept more events
    else Future.Unit
  }

  /**
   * This function performs the commit if there are events to be committed, and will also reset the
   * next scheduled commit.
   *
   * It removes the contiguous sequence of completed offsets starting at the head of [[eventQueue]],
   * and commits the last offset in that sequence, if it exists.
   *
   * Any events that did not finish processing will be committed in the next commit.
   *
   * Note: Acquire [[mutex]] before calling.
   */
  private[this] def commit(): Future[Unit] = {
    val (completed, remaining) =
      (eventQueue.takeWhile(_.isDefined), eventQueue.dropWhile(_.isDefined))
    eventQueue = remaining
    val committed = completed.lastOption match {
      case Some(offset) => offset.flatMap(commit)
      case None => Future.Unit
    }
    committed ensure {
      nextCommit = Future.never
    }
  }

  /**
   * This loop will always wake up when a commit is scheduled, or when [[recv]] tells it to check
   * for its new schedule.
   */
  private[this] def loop(): Future[Unit] = {
    Offer.prioritize(
      newCommitReady().toOffer.mapConstFunction(loop()),
      _stop.toOffer.mapConstFunction(mutex.acquireAndRun(commit())),
      mutex.acquireAndRunSync(nextCommit).flatten.toOffer.mapConstFunction(
        mutex.acquireAndRun(commit())
      )
    ).sync().flatten
  }

  /**
   * A Future that becomes defined only after [[stop()]] is called, and any batch in flight while
   * [[stop()]] was called is committed.
   */
  lazy val start: Future[Unit] = loop()
  def stopped: Future[Unit] = start
  def isStopped: Boolean = _stop.isDefined

  private[this] val _stop: Promise[Unit] = new Promise()
  def stop(): Unit = if (_stop.setDone()) newCommitReady.asyncNotifyAll()
  def forceStop(): Unit = Await.result(
    mutex.acquireAndRunSync {
      // Just drop all events, processed or not
      eventQueue = mutable.Queue()
      if (_stop.setDone()) newCommitReady.asyncNotifyAll()
    }
  )

  /** Used to synchronize between [[recv]] and [[loop()]] */
  private[this] val newCommitReady: AsyncCondition = new AsyncCondition(timer)

  private[this] val mutex: AsyncMutex = new AsyncMutex()
  @volatile private[this] var nextCommit: Future[Unit] = Future.never
  @volatile private[this] var eventQueue: mutable.Queue[Future[String]] = mutable.Queue()
}

object StreamOffsetCommitter {
  class CommitterStoppedException(message: String, throwable: Option[Throwable] = None)
    extends Exception(message, throwable.orNull)
}
