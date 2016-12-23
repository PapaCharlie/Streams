package io.papacharlie.streams

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput, ShutdownReason}
import com.ifttt.thrifttt.diary.Event
import com.ifttt.thrifttt.diary.conversions.EventConversions
import com.twitter.bijection.Conversion.asMethod
import com.twitter.concurrent.{Broker, Offer}
import com.twitter.util.{Future, Return, Throw, Try}
import java.io.IOException
import java.util.concurrent.atomic.AtomicReference
import org.xerial.snappy.Snappy
import scala.collection.JavaConverters._

/**
 * A Kinesis record processor that's responsible for listening to a stream of messages, parsing them into Event
 * instances, and making them available one at a time through a Broker.
 */
class EventBroker extends IRecordProcessor {
  import EventBroker._

  private val broker = new Broker[Message]
  private val checkpointer: AtomicReference[Option[IRecordProcessorCheckpointer]] =
    new AtomicReference(None)
  val shardId: AtomicReference[Option[String]] = new AtomicReference(None)

  /** Initialization method called by Kinesis. */
  def initialize(initializationInput: InitializationInput): Unit = {
    shardId.set(Some(initializationInput.getShardId))
  }

  /** Ask for a message. */
  def recv: Offer[Message] = broker.recv

  /**
   * Save how far we've gotten in the stream. This should only be called when all previously-emitted events have been
   * handled.
   */
  def saveProgress(lastProcessedSequenceNumber: String): Future[Unit] = Future {
    checkpointer.get match {
      case Some(cp) => cp.checkpoint(lastProcessedSequenceNumber)
      case None => assert(
        assertion = false,
        "Checkpointer must be initialized before sending records to consumer"
      )
    }
  }

  /**
   * Processing method called by Kinesis. Provides a batch of records which we then deserialize and
   * feed into the broker.
   */
  def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    checkpointer.set(Some(processRecordsInput.getCheckpointer))

    for (record <- processRecordsInput.getRecords.asScala) {
      val message = record.getData.as[Array[Byte]]

      val event = Try(Snappy.uncompress(message)).flatMap(EventConversions.deserialize).rescue {
        case ex: IOException =>
          log.debug(ex, "Received uncompressed event")
          EventConversions.deserialize(message)
      }

      event match {
        case Return(e) => broker !! NewEvent(e, record.getSequenceNumber)
        case Throw(err) => broker !! InvalidEventData(message, err)
      }
    }
  }

  /**
   * Consumer shutdown handler called by Kinesis, either because processing of this shard is moving to another client or
   * because the shard is being removed.
   */
  def shutdown(shutdownInput: ShutdownInput): Unit = {
    checkpointer.set(Some(shutdownInput.getCheckpointer))

    shutdownInput.getShutdownReason match {
      case ShutdownReason.TERMINATE => broker !! GracefulShutdown
      case ShutdownReason.ZOMBIE => broker !! AbruptShutdown
    }
  }
}

object EventBroker {
  sealed trait Message
  case object AbruptShutdown extends Message
  case object GracefulShutdown extends Message
  case class InvalidEventData(eventData: Array[Byte], err: Throwable) extends Message
  case class NewEvent(event: Event, offset: String) extends Message
}
