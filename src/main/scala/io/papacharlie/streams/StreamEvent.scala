package io.papacharlie.streams

import java.nio.ByteBuffer
import org.joda.time.DateTime

class StreamEvent(
  val data: ByteBuffer,
  val arrivalDate: DateTime,
  val streamId: String,
  val streamOffset: String
) {
  override def toString = s"Event($data,$arrivalDate,$streamId,$streamOffset)"
}

object StreamEvent {
  val empty: StreamEvent =
    new StreamEvent(ByteBuffer.wrap(Array.emptyByteArray), DateTime.now(), "", "")
}
