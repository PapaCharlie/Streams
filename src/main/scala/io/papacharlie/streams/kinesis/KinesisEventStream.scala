package io.papacharlie.streams.kinesis

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import com.amazonaws.services.kinesis.model._
import com.twitter.concurrent.AsyncStream
import com.twitter.util.{Future, Promise}
import io.papacharlie.streams.{EventStream, StreamEvent, StreamOffsetCommitter}
import scala.collection.JavaConverters._

/**
 * An [[io.papacharlie.streams.EventStream]] for Kinesis. It will fetch [[getRecordsRequestLimit]]
 * per call to Kinesis, so you may find it advantageous to set it to some multiple of
 * [[committer.maxEventsBetweenCommits]] so that it approximates prefetching, without actually
 * processing/committing any events.
 *
 * @param client                 The underlying Kinesis client from which to fetch events from the
 *                               stream
 * @param getRecordsRequestLimit Number of events to fetch from Kinesis per fetch call
 * @param initialShardIterator   Shard iterator for stream
 * @param eventConsumer          Function with which to consume events
 * @param committer              An instance of [[io.papacharlie.streams.StreamOffsetCommitter]]
 *                               that describes the commit policy
 */
class KinesisEventStream(
  client: AmazonKinesisAsyncClient,
  getRecordsRequestLimit: Int,
  initialShardIterator: String,
  val eventConsumer: StreamEvent => Future[Unit],
  val committer: StreamOffsetCommitter,
  val fatalExceptions: Boolean
) extends EventStream {
  protected def mkStream(): AsyncStream[StreamEvent] = {
    def streams(iterator: Option[String]): AsyncStream[AsyncStream[StreamEvent]] = {
      iterator match {
        case Some(i) =>
          val futureResults = getRecords(i)
          AsyncStream.Cons(
            futureResults map { results =>
              AsyncStream.fromSeq(results.getRecords.asScala.map(new KinesisStreamEvent(_)))
            },
            () => AsyncStream.fromFuture(
              futureResults map (results => streams(Option(results.getNextShardIterator)))
            ).flatten
          )
        case None => AsyncStream.empty
      }
    }
    streams(Option(initialShardIterator)).flatten
  }

  private def getRecords(shardIterator: String): Future[GetRecordsResult] = {
    val recordsPromise = new Promise[GetRecordsResult]()
    client.getRecordsAsync(
      new GetRecordsRequest().withLimit(getRecordsRequestLimit).withShardIterator(shardIterator),
      new PromiseAsyncHandler(recordsPromise)
    )
    recordsPromise
  }
}

object KinesisEventStream {
  def shardIteratorRequest(
    shardId: String,
    shardIteratorType: Option[ShardIteratorType] = None,
    streamName: Option[String] = None,
    startingSequenceNumber: Option[String] = None
  ): GetShardIteratorRequest = {
    val request = new GetShardIteratorRequest()
    shardIteratorType.foreach(request.setShardIteratorType)
    streamName.foreach(request.setStreamName)
    startingSequenceNumber.foreach(request.setStartingSequenceNumber)
    request
  }

  def getShardIterator(
    client: AmazonKinesisAsyncClient,
    iteratorRequest: GetShardIteratorRequest
  ): Future[String] = {
    val iteratorPromise = new Promise[GetShardIteratorResult]()
    client.getShardIteratorAsync(
      iteratorRequest,
      new PromiseAsyncHandler(iteratorPromise)
    )
    iteratorPromise map (_.getShardIterator)
  }
}
