package io.papacharlie.streams

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.ifttt.diary.Settings
import com.twitter.conversions.time._
import java.nio.ByteBuffer

/**
 * Wrapper for setting up and using a synchronous Kinesis client. We shouldn't use this for most
 * things, but we use it during setup and for handling dead events.
 */
class SyncKinesisClient(settings: Settings) {
  import settings.kinesis
  private val awsCredentials = new BasicAWSCredentials(
    kinesis.accessKeyId,
    kinesis.accessKeySecret
  )
  private val kinesisClient = new AmazonKinesisClient(awsCredentials)
  kinesisClient.setEndpoint(kinesis.kinesisEndpoint)

  /**
   * Ensure that a Kinesis stream with the given name exists.
   *
   * @param streamName The stream name (stream may or may not exist)
   */
  def ensureStreamExists(streamName: String): Unit = {
    var streamStatus = try {
      kinesisClient.describeStream(streamName).getStreamDescription.getStreamStatus
    } catch {
      case err: ResourceNotFoundException =>
        log.ifWarning(s"SyncKinesisClient: Creating stream $streamName")
        kinesisClient.createStream(streamName, 4)
        "CREATING"
    }

    while (streamStatus != "ACTIVE") {
      log.ifWarning(s"SyncKinesisClient: Waiting for stream $streamName to be ready")
      Thread.sleep(3.seconds.inMilliseconds)
      streamStatus = kinesisClient.describeStream(streamName).getStreamDescription.getStreamStatus
    }
  }

  /**
   * Write a byte array to a Kinesis stream.
   *
   * @param streamName   The stream name (stream must exist)
   * @param record       The data
   * @param partitionKey A key used for sharding (should be random-ish)
   */
  def putRecord(streamName: String, record: Array[Byte], partitionKey: String): Unit = {
    kinesisClient.putRecord(streamName, ByteBuffer.wrap(record), partitionKey)
  }
}
