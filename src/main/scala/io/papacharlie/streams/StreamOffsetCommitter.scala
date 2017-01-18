package io.papacharlie.streams

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.util._
import io.papacharlie.streams.StreamOffsetCommitter.CommitterStoppedException
import java.util.concurrent.atomic.AtomicBoolean

abstract class StreamOffsetCommitter {
  private[this] implicit val timer = new JavaTimer()

  /** The maximum amount of time to wait between commits */
  def maxTimeBetweenCommits: Duration
  private[this] def newTimeout: Offer[Unit] = Offer.timeout(maxTimeBetweenCommits)

  /** The maximum number of processed events to leave uncommitted */
  def maxEventsBetweenCommits: Int

  /** Maximum number of events to be processing at once */
  def maxEventsInFlight: Int

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
  def recv(offset: String, event: Future[Unit]): Future[Unit] = {
    if (isStopped) {
      Future.exception(
        new CommitterStoppedException("Cannot call `recv` after having called `stop()`!")
      )
    } else {
      val promise = new Promise[Unit]()
      eventsBroker.sendAndSync(offset -> event -> promise) before promise
    }
  }

  /**
   * The main loop that triggers commits based on certain conditions.
   *
   * @param events      Events currently in flight
   * @param processed   Number of processed, uncommitted events
   * @param timeout     The next scheduled timeout ([[Offer.never]] if `events` and `processed`
   *                    are empty and 0 respectively)
   * @param toCommit    Next offset to commit. None if no events have been processed.
   * @param recvPromise The call to `recv` may be required not to resolve according to its spec.
   */
  private[this] def loop(
    events: Seq[(String, Future[Unit])],
    processed: Int,
    timeout: Offer[Unit],
    toCommit: Option[String],
    recvPromise: Option[Promise[Unit]]
  ): Future[Unit] = {
    /*
     * Consumes processed events in the order they were received, not in the order they were
     * completed.
     *
     * Once an event is processed, recurse with `toCommit` set to that event's offset.
     */
    val processedEventOffer = events.headOption match {
      case None => Offer.never
      case Some((offset, result)) => result.toOffer.map {
        case Throw(ex) if fatalExceptions => throw ex
        case _ =>
          if (events.length == maxEventsInFlight && processed < maxEventsBetweenCommits) {
            recvPromise.foreach(_.setDone())
            loop(events.tail, processed + 1, timeout, Some(offset), None)
          } else {
            loop(events.tail, processed + 1, timeout, Some(offset), recvPromise)
          }
      }
    }

    /*
     * If enough events have been processed, immediately trigger a commit, otherwise wait for the
     * timeout.
     */
    val commitOffer =
      (if (processed >= maxEventsBetweenCommits) Offer.const(()) else timeout).const {
        toCommit match {
          case Some(c) => commit(c).before {
            recvPromise.foreach(_.setDone())
            loop(events, processed = 0, Offer.never, None, None)
          }
          case None => loop(events, processed, timeout, None, recvPromise)
        }
      }

    /*
     * If `maxEventsBetweenCommits` have been processed, no new events can be processed until a
     * commit happens, therefore `eventOffer` is Offer.never and `commitOffer` is Offer.const(()),
     * i.e. commit immediately. If `maxEventsInFlight` are currently being processed, no new events
     * can be accepted until a slot becomes free.
     *
     * Else more events can be accepted, so wait for a new one from the `eventsBroker` and recurse
     * with the new promise of the offset.
     */
    val eventOffer =
      if (events.length >= maxEventsInFlight || processed >= maxEventsBetweenCommits) {
        Offer.never
      } else {
        eventsBroker.recv.map { case (event, promise) =>
          val t = if (events.isEmpty) newTimeout else timeout
          if (processed == maxEventsBetweenCommits - 1) {
            loop(events :+ event, processed, t, toCommit, Some(promise))
          } else {
            promise.setDone()
            loop(events :+ event, processed, t, toCommit, None)
          }
        }
      }

    Offer.prioritize(
      processedEventOffer,
      commitOffer,
      eventOffer,
      _stop.toOffer.const(toCommit.map(commit).getOrElse(Future.Unit)),
      _forceStop.toOffer.const(Future.Unit)
    ).sync().flatten
  }

  private[this] val eventsBroker: Broker[((String, Future[Unit]), Promise[Unit])] = new Broker

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
