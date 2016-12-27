//package io.papacharlie.streams
//
//import com.ifttt.diary.Settings
//import com.ifttt.diary.storage.EventStorageClient
//import com.ifttt.diary.util.MetricsScope
//import com.ifttt.thrifttt.diary.Event
//import com.twitter.bijection.Base64String
//import com.twitter.bijection.Conversion.asMethod
//import com.twitter.concurrent.Offer
//import com.twitter.finagle.service.Backoff
//import com.twitter.util._
//import java.util.concurrent.atomic.AtomicBoolean
//import scala.collection.immutable.Queue
//
///**
// * An "actor" that's responsible for receiving events from an EventBroker and storing them.
// *
// * @param settings        The app's settings (see [[com.ifttt.diary.Settings]] and application.conf)
// * @param broker          Source of events
// * @param storageClient   Destination for events
// * @param deadLetterQueue Queue for events that fail to store too many times
// */
//class EventConsumer(
//  settings: Settings,
//  broker: EventBroker,
//  storageClient: EventStorageClient,
//  deadLetterQueue: DeadLetterQueue
//) {
//  import EventBroker._
//  import EventConsumer._
//
//  // Needed by `Offer.timeout`
//  implicit private val timer = new JavaTimer
//
//  // The following metrics are lazy because the shardId may not defined at instantiation. Making
//  // them lazy means they get registered under the right scope
//  private lazy val metrics = new MetricsScope("consumer", shardId)
//  private lazy val eventProcessedCounter = metrics.counter("event_processed")
//  private lazy val eventRetriesStat = metrics.stat("event_retries")
//  private lazy val eventInvalidCounter = metrics.counter("event_invalid")
//  import settings.consumer.userIdBlacklist
//
//  /**
//   * An exponential backoff policy for event retries. This is an immutable stream of durations, so
//   * we can just take entries off the front each time we end up in a retry loop.
//   */
//  private val initialBackoffPolicy: Stream[Duration] = Backoff.exponentialJittered(
//    settings.consumer.backoffInitial,
//    settings.consumer.backoffMaximum
//  )
//
//  /** Has this consumer completely stopped doing work? */
//  val isCompletelyShutDown = new AtomicBoolean(false)
//
//  /** Enter the main loop. */
//  def run(): Future[Unit] = {
//    logInfo("Starting up")
//    consumeLoop(PendingBatch(), newTimeout())
//  }
//
//  /**
//   * The main run loop. We listen for events from [[broker]] and timeouts and act accordingly. Each
//   * branch is responsible for calling [[consumeLoop]], [[batchCompleteLoop]], or [[checkpointLoop]]
//   * as appropriate to keep things going. This loop is responsible for batching events together,
//   * and flushing the batch when it ether reaches a certain size or after a certain amount of time.
//   *
//   * @param batch The set of events that we're currently working on storing
//   */
//  private[consumer] def consumeLoop(
//    batch: PendingBatch,
//    checkpointTimeout: Offer[Unit]
//  ): Future[Unit] = {
//    logDebug(s"Entering consume loop with ${batch.length} events pending")
//    if (batch.length >= settings.kinesis.maximumBatchSize && !batch.resolved) {
//      logDebug("Reached batch limit; resolving batch")
//      batchCompleteLoop(batch, checkpointTimeout)
//    } else {
//      // Use `prioritize` instead of `choose` so that the checkpoint timeout takes precedence over
//      // other events.
//      Offer.prioritize(
//        checkpointTimeout.map { _ =>
//          if (batch.isEmpty) {
//            logDebug("Reached checkpoint timeout, but the current batch is empty and committed")
//            consumeLoop(PendingBatch(), newTimeout())
//          } else if (batch.resolved) {
//            logDebug(
//              "Reached checkpoint timeout, but no unresolved batch in flight; " +
//                "committing most recently resolved batch"
//            )
//            saveProgress(batch.lastCheckpoint) before consumeLoop(PendingBatch(), newTimeout())
//          } else {
//            logInfo("Reached checkpoint timeout; resolving batch in order to save checkpoint")
//            checkpointLoop(batch)
//          }
//        },
//        broker.recv.map {
//          case NewEvent(event, _) if userIdBlacklist(event.userId) =>
//            // Just drop the event
//            consumeLoop(batch, checkpointTimeout)
//          case NewEvent(event, checkpoint) =>
//            logDebug(s"Storing event ${event.id}")
//            eventProcessedCounter.incr()
//            if (batch.resolved)
//              consumeLoop(PendingBatch(Queue(event), checkpoint), checkpointTimeout)
//            else
//              consumeLoop(batch.enqueue(event, checkpoint), checkpointTimeout)
//          case e: InvalidEventData =>
//            logError(e.err, s"Invalid event in stream: ${e.eventData.as[Base64String].str}")
//            eventInvalidCounter.incr()
//            consumeLoop(batch, checkpointTimeout)
//          case GracefulShutdown =>
//            logInfo("Preparing to shut down")
//            if (batch.isEmpty) Future.Unit
//            else if (batch.resolved) saveProgress(batch.lastCheckpoint)
//            else checkpointLoop(batch, shuttingDown = true)
//          case AbruptShutdown =>
//            Future.value(shutDown())
//        }
//      ).sync().flatten
//    }
//  }
//
//  /**
//   * Secondary loop -- when we've pulled in a batch's worth of records, we should wait to request
//   * more until we're done processing the batch.
//   *
//   * @param batch             The list of events that we're currently working on storing
//   * @param checkpointTimeout The timeout marking when we next need to save a checkpoint
//   * @param backoff           The current backoff policy
//   */
//  private def batchCompleteLoop(
//    batch: PendingBatch,
//    checkpointTimeout: Offer[Unit],
//    backoff: Stream[Duration] = initialBackoffPolicy
//  ): Future[Unit] = {
//    resolveBatch(batch, backoff).flatMap {
//      case None =>
//        logDebug("All events in batch saved; resuming main loop")
//        consumeLoop(batch.resolve, checkpointTimeout)
//      case Some(unresolvedBatch) =>
//        logWarning(s"Retrying batch of ${unresolvedBatch.length} events to complete batch")
//        batchCompleteLoop(unresolvedBatch, checkpointTimeout, backoff.tail)
//    }
//  }
//
//  /**
//   * Secondary loop -- when the checkpoint timeout has elapsed, we should wait to request more
//   * events until we've processed the ones we have and saved our progress in Kinesis. The reason for
//   * the distinction between this and [[batchCompleteLoop]] is that the Kinesis docs recommend not
//   * committing too often, but we also don't want to have a huge number of events in flight at once.
//   *
//   * @param batch        The list of events we're currently working on storing
//   * @param shuttingDown Are we currently shutting down? If so, don't re-enter the main loop when
//   *                     we're done
//   * @param backoff      The current backoff policy
//   */
//  private def checkpointLoop(
//    batch: PendingBatch,
//    shuttingDown: Boolean = false,
//    backoff: Stream[Duration] = initialBackoffPolicy
//  ): Future[Unit] = {
//    resolveBatch(batch, backoff).flatMap {
//      case None =>
//        logInfo("All events in batch saved; saving checkpoint")
//        saveProgress(batch.lastCheckpoint) before {
//          if (shuttingDown) Future.value(shutDown())
//          else consumeLoop(PendingBatch(), newTimeout())
//        }
//      case Some(unresolvedBatch) =>
//        logWarning(s"Retrying batch of ${unresolvedBatch.length} events for checkpoint")
//        checkpointLoop(unresolvedBatch, shuttingDown, backoff.tail)
//    }
//  }
//
//  /**
//   * Wait for all of the given events to be stored.
//   *
//   * @param batch   A list of in-flight attempts to store events
//   * @param backoff The current backoff policy. If we need to retry, we'll wait the length of
//   *                the first duration in the stream and then drop that duration so that the
//   *                next retry takes longer.
//   * @return A new list of in-flight attempts to store events, consisting of any events that needed
//   *         to be retried. If this list is empty, we successfully resolved the batch.
//   */
//  private def resolveBatch(
//    batch: PendingBatch,
//    backoff: Stream[Duration]
//  ): Future[Option[PendingBatch]] = {
//    logDebug(s"Waiting for ${batch.length} events to persist")
//    if (batch.nonEmpty) {
//      storageClient.storeEvents(batch.events).liftToTry.flatMap {
//        case Return(_) =>
//          eventRetriesStat.add(batch.retryCount)
//          Future.None
//        case Throw(err) if batch.retryCount < settings.consumer.maxRetryCount =>
//          logWarning(s"Failed to persist batch of ${batch.length} events")
//          logWarning(s"Backing off for ${backoff.head}")
//          logDebug("Batch persist error.", err)
//          Future.sleep(backoff.head).map(_ => Some(batch.retried))
//        case Throw(err) =>
//          log.error(
//            err,
//            s"Persisting batch of ${batch.length} failed ${settings.consumer.maxRetryCount} times. Sending to DeadLetterQueue"
//          )
//          deadLetterQueue.inbox ! DeadLetterQueue.DeadBatch(batch.events)
//          Future.None
//      }
//    } else Future.None
//  }
//
//  /**
//   * Attempt to tell Kinesis how far we've gotten in processing messages.
//   *
//   * @param lastOffset The offset of the last stored message
//   */
//  private def saveProgress(lastOffset: String): Future[Unit] = {
//    broker.saveProgress(lastOffset).liftToTry map {
//      case Return(_) => logInfo(s"Saved checkpoint $lastOffset")
//      case Throw(err) => logError(err, s"Failed to save checkpoint $lastOffset")
//    }
//  }
//
//  /**
//   * Mark this consumer as shut down and stop accepting work.
//   */
//  private def shutDown(): Unit = {
//    logInfo("Finished shutting down")
//    isCompletelyShutDown.set(true)
//  }
//
//  /**
//   * @return a new timeout that will expire when it's time to save our progress to Dynamo.
//   */
//  private def newTimeout(): Offer[Unit] = Offer.timeout(settings.kinesis.timeBetweenCheckpoints)
//
//  /**
//   * Logging methods that tag the given message with the current shard id. Note that `msg` isn't
//   * evaluated unless the message is printed.
//   */
//  private def logDebug(msg: => String) = log.ifDebug(s"EventConsumer($shardId): $msg")
//  private def logDebug(msg: => String, err: Throwable) =
//    log.ifDebug(err, s"EventConsumer($shardId): $msg")
//  private def logInfo(msg: => String) = log.ifInfo(s"EventConsumer($shardId): $msg")
//  private def logWarning(msg: => String) = log.ifWarning(s"EventConsumer($shardId): $msg")
//  private def logError(err: Throwable, msg: => String) =
//    log.ifError(err, s"EventConsumer($shardId): $msg")
//  private def shardId = broker.shardId.get.getOrElse("...")
//}
//
//object EventConsumer {
//
//  /**
//   * The current batch of events we're waiting on storing, and how many times we've attempted to
//   * store it.
//   */
//  case class PendingBatch(
//    events: Queue[Event] = Queue.empty,
//    lastCheckpoint: String = "",
//    retryCount: Int = 0,
//    resolved: Boolean = false
//  ) {
//    def length = events.length
//    def isEmpty = events.isEmpty
//    def nonEmpty = events.nonEmpty
//    def retried = copy(retryCount = retryCount + 1)
//    def enqueue(event: Event, offset: String) =
//      copy(events = events.enqueue(event), lastCheckpoint = offset)
//    def resolve = copy(resolved = true)
//  }
//}
