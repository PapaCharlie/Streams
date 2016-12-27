package io.papacharlie.streams

import com.twitter.concurrent.AsyncStream
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import org.joda.time.DateTime
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import scala.util.Random

class EventStreamSpec extends Specification {
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
      private val count = new AtomicLong()
      private def mkStream(): AsyncStream[Long] =
        AsyncStream.mk(count.incrementAndGet(), mkStream())
      val stream = mkStream()
      for {
        i <- stream
        if i % 10000 == 0
      } println(i)
    }
  }
}
