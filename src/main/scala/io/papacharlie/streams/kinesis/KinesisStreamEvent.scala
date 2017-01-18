package io.papacharlie.streams.kinesis

import com.amazonaws.services.kinesis.model.Record
import io.papacharlie.streams.StreamEvent
import org.joda.time.DateTime

class KinesisStreamEvent(record: Record) extends StreamEvent(
  record.getData,
  new DateTime(record.getApproximateArrivalTimestamp.getTime),
  record.getPartitionKey,
  record.getSequenceNumber
)