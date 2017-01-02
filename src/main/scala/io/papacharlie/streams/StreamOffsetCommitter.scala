package io.papacharlie.streams

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.util._
import io.papacharlie.streams.StreamOffsetCommitter.CommitterStoppedException
import java.util.concurrent.atomic.AtomicBoolean

abstract class StreamOffsetCommitter {
  private[this] implicit val timer = new JavaTimer()

  /**
   * The maximum amount of time to wait between commits
   */
  def maxTimeBetweenCommits: Duration
  private[this] def timeoutOffer: Offer[Unit] = Offer.timeout(maxTimeBetweenCommits)

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
   * What to do when an exception is encountered:
   *
   * If true: Fail the stream and stop consuming
   * If false: Keep consuming and commit failed events
   */
  def fatalExceptions: Boolean

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
   *
   * Please make sure to wait for the resulting Future to be resolved before calling this method
   * again.
   */
  def recv(event: Future[String]): Future[Unit] = {
    if (isStopped) {
      Future.exception(
        new CommitterStoppedException("Cannot call `recv` after having called `stop()`!")
      )
    } else {
      val promise = new Promise[Unit]()
      eventsBroker.sendAndSync((event, promise)) before promise
    }
  }

  /**
   * This loop will always wake up when a commit is scheduled, or when [[recv]] tells it to check
   * for its new schedule.
   */
  private[this] def loop(
    events: Seq[Future[String]],
    processed: Int,
    timeout: Offer[Unit],
    toCommit: Option[String],
    commitPromise: Option[Promise[Unit]]
  ): Future[Unit] = {
    val eventOffer =
      if (processed >= maxEventsBetweenCommits) Offer.never
      else eventsBroker.recv.map { case (event, promise) =>
        val newTimeout = if (events.isEmpty) timeoutOffer else timeout
        if (processed == maxEventsBetweenCommits - 1) {
          loop(events :+ event, processed, newTimeout, toCommit, Some(promise))
        } else {
          promise.setDone()
          loop(events :+ event, processed, newTimeout, toCommit, None)
        }
      }
    Offer.prioritize(
      events.headOption.getOrElse(Future.never).toOffer.map {
        case Return(o) =>
          loop(events.tail, processed + 1, timeout, toCommit = Some(o), commitPromise)
        case Throw(ex) if fatalExceptions =>
          throw ex
        case Throw(_) =>
          loop(events.tail, processed + 1, timeout, toCommit = toCommit, commitPromise)
      },
      (if (processed >= maxEventsBetweenCommits) Offer.const(()) else timeout).mapConstFunction {
        toCommit
          .map(commit)
          .getOrElse(Future.Unit)
          .before {
            commitPromise.foreach(_.setDone())
            loop(events, processed = 0, timeoutOffer, None, None)
          }
      },
      eventOffer,
      _stop.toOffer.mapConstFunction(toCommit.map(commit).getOrElse(Future.Unit)),
      _forceStop.toOffer.mapConst(Future.Unit)
    ).sync().flatten
  }

  private[this] val eventsBroker: Broker[(Future[String], Promise[Unit])] = new Broker()

  /**
   * A Future that becomes defined only after [[stop()]] is called, and any batch in flight while
   * [[stop()]] was called is committed.
   */
  lazy val start: Future[Unit] = loop(Seq.empty, 0, Offer.never, None, None)
  def stopped: Future[Unit] = start

  private[this] val _stopped: AtomicBoolean = new AtomicBoolean(false)
  def isStopped: Boolean = _stopped.get

  private[this] val _stop: Promise[Unit] = new Promise()
  def stop(): Unit = {
    _stop.setDone()
    _stopped.set(true)
  }

  private[this] val _forceStop: Promise[Unit] = new Promise()
  def forceStop(): Unit = {
    _forceStop.setDone()
    _stopped.set(true)
  }
}

object StreamOffsetCommitter {
  class CommitterStoppedException(message: String, throwable: Option[Throwable] = None)
    extends Exception(message, throwable.orNull)
}
