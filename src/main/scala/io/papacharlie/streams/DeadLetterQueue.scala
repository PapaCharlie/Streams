package io.papacharlie.streams

import com.ifttt.diary.util.Implicits._
import com.ifttt.diary.util.MetricsScope
import com.ifttt.thrifttt.diary.conversions.EventConversions
import com.ifttt.thrifttt.diary.Event
import com.twitter.bijection.Base64String
import com.twitter.bijection.Conversion.asMethod
import com.twitter.concurrent.Broker
import com.twitter.util.Stopwatch

/**
 * An interface to a Kinesis queue holding event batches that we've repeatedly failed to store and
 * index. This should run on its own thread, since we're using Kinesis's blocking client API.
 *
 * @param kinesisClient The client to use for writing events to Kinesis
 * @param streamName    The name of the stream we're writing to
 */
class DeadLetterQueue(kinesisClient: SyncKinesisClient, streamName: String) extends Runnable {
  import DeadLetterQueue._

  val inbox = new Broker[Message]

  private val metrics = new MetricsScope("dead_letter_queue")
  private val eventStoredStat = metrics.stat("event_stored")

  private var shuttingDown = false

  def run(): Unit = {
    log.ifInfo("DeadLetterQueue: Starting up")

    while (!shuttingDown) {
      inbox.recv.map {
        case DeadBatch(events) =>
          log.ifInfo("Received dead batch.")
          events.foreach { event =>
            log.ifInfo(s"DeadLetterQueue: Received dead event: ${event.id}")
            val serializedEvent = EventConversions.serialize(event)
            try {
              val elapsed = Stopwatch.start()
              val res = kinesisClient.putRecord(
                streamName, serializedEvent, event.id.toJava.toString
              )
              eventStoredStat.add(elapsed().inMillis)
              res
            } catch {
              case err: Throwable =>
                val encodedEvent = serializedEvent.as[Base64String].str
                log.error(err, s"Failed to write event to dead-letter stream: $encodedEvent")
            }
          }
        case ShutDown =>
          shuttingDown = true
      }.syncWait()
    }

    log.ifInfo("DeadLetterQueue: Shut down")
  }
}

object DeadLetterQueue {
  sealed trait Message
  case class DeadBatch(events: Seq[Event]) extends Message
  case object ShutDown extends Message
}
