package io.papacharlie.streams

import com.twitter.util.{Duration, Future, Promise, Timer}
import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator

/**
 * Imperative-style condition variables, backed by [[com.twitter.util.Promise]]s. Waiters wait on
 * some Promise to become resolved, i.e. until a call to [[asyncNotifyAll]] is made.
 *
 * Note that `asyncNotify` does not exist, this is because all waiters are waiting on the same
 * Promise, until it becomes resolved. Once it does, a new Promise is created and subsequent waiters
 * will wait on the new promise.
 */
class AsyncCondition(timer: Timer) {
  private implicit val _timer = timer
  private[this] val condition: AtomicReference[Option[Promise[Unit]]] = new AtomicReference(None)

  /**
   * Notify all waiters to check the condition they are waiting on.
   */
  def asyncNotifyAll(): Unit = condition.updateAndGet(notifyThenNullify)

  /**
   * Resolve the underlying promise and set it to None.
   */
  private val notifyThenNullify = new UnaryOperator[Option[Promise[Unit]]] {
    def apply(t: Option[Promise[Unit]]): Option[Promise[Unit]] = {
      t.foreach(_.setDone())
      None
    }
  }

  def apply(): Future[Unit] = asyncWait()
  /**
   * Wait for a call to [[asyncNotifyAll()]].
   *
   * This Future will never resolve if no call to [[asyncNotifyAll()]] is made.
   */
  def asyncWait(): Future[Unit] = condition.updateAndGet(getOrCreate).get // Get is safe, see below

  /** If the condition is None, set it to a new Promise */
  private val getOrCreate = new UnaryOperator[Option[Promise[Unit]]] {
    def apply(t: Option[Promise[Unit]]): Option[Promise[Unit]] = t.orElse(Some(new Promise()))
  }

  def apply(duration: Duration): Future[Unit] = asyncWait(duration)
  /**
   * Wait for at most `duration` for a call to [[asyncNotifyAll()]]
   *
   * @param duration Maximum amount of time to wait for a notification.
   */
  def asyncWait(duration: Duration): Future[Unit] = asyncWait().or(Future.Unit.delayed(duration))
}
