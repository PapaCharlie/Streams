package io.papacharlie.streams

import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.model.{GetRecordsRequest, GetRecordsResult}
import com.twitter.concurrent.AsyncStream
import com.twitter.util.{Future, Promise}
import scala.collection.JavaConverters._

/**
 * A class that's responsible for configuring and running an instance of Kinesis's [[Worker]] class.
 */
class KinesisEventStream(
  client: AmazonKinesisAsyncClient,
  getRecordsRequestLimit: Int,
  initialShardIterator: String,
  val consumeEvent: StreamEvent => Future[Unit],
  val committer: StreamOffsetCommitter
) extends EventStream {
  val concurrencyLevel = getRecordsRequestLimit

  protected def mkStream(): AsyncStream[StreamEvent] = {
    def streams(iterator: Option[String]): AsyncStream[AsyncStream[StreamEvent]] = {
      iterator match {
        case Some(i) =>
          val futureResults = getRecords(i)
          AsyncStream.Cons(
            futureResults map { results =>
              AsyncStream.fromSeq(results.getRecords.asScala.map(new StreamEvent(_)))
            },
            () => AsyncStream.fromFuture(
              futureResults.map(results => streams(Option(results.getNextShardIterator)))
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


