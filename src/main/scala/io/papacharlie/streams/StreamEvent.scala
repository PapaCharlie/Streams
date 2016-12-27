package io.papacharlie.streams

import com.amazonaws.services.kinesis.model.Record
import com.twitter.util.Promise
import java.nio.ByteBuffer
import org.joda.time.DateTime

class StreamEvent(
  val data: ByteBuffer,
  val arrivalDate: DateTime,
  val streamId: String,
  val streamOffset: String
) {
  private[streams] val promise: Promise[String] = Promise()

  def this(record: Record) = this(
    record.getData,
    new DateTime(record.getApproximateArrivalTimestamp.getTime),
    record.getPartitionKey,
    record.getSequenceNumber
  )

  lazy val setCheckpoint: Unit = promise.setValue(streamOffset)
  override def toString = s"Event($data,$arrivalDate,$streamId,$streamOffset)"
}

object StreamEvent {
  def empty: StreamEvent = new StreamEvent(
    ByteBuffer.wrap(Array.emptyByteArray), DateTime.now(), "", ""
  )
}
