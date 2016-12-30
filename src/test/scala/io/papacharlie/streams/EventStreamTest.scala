package io.papacharlie.streams

import java.nio.ByteBuffer
import org.joda.time.DateTime
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import scala.util.Random

class EventStreamTest extends Specification {
  private trait EventStreamScope extends Scope {
    private val random = new Random
    def event: StreamEvent = event(random.nextInt.toByte)
    def event(i: Byte): StreamEvent =
      new StreamEvent(ByteBuffer.wrap(Array(i)), DateTime.now(), "", "")
    val emptyEvent: StreamEvent = new StreamEvent(
      ByteBuffer.wrap(Array.emptyByteArray), DateTime.now(), "", ""
    )
  }

  "EventStream" >> {
    "streams forever" in new EventStreamScope {
      ok
    }
  }
}
