package io.papacharlie.streams

import com.amazonaws.auth.{AWSCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.ifttt.diary.Settings
import com.ifttt.diary.storage.EventStorageClient
import com.ifttt.diary.util.MetricsScope
import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

/**
 * A class that's responsible for configuring and running an instance of Kinesis's [[Worker]] class.
 */
class KinesisWorkerManager(
  settings: Settings,
  storageClient: EventStorageClient,
  deadLetterQueue: DeadLetterQueue
) extends IRecordProcessorFactory {
  import settings.kinesis
  private val awsCredentialsProvider = new AWSCredentialsProvider {
    def refresh() = {}
    def getCredentials = new BasicAWSCredentials(kinesis.accessKeyId, kinesis.accessKeySecret)
  }

  private val _consumers = new ConcurrentLinkedQueue[EventConsumer]

  private val metrics = new MetricsScope("kinesis_worker")
  private val consumersGauge = metrics.gauge("consumers") {
    _consumers.size()
  }

  val worker = buildWorker()

  /** Start the worker. This method blocks until the worker shuts down. */
  def run(): Unit = worker.run()

  /** Shut down the worker. */
  def shutdown(): Unit = worker.shutdown()

  /** Nicer representation of the consumers queue. */
  def consumers = _consumers.iterator.asScala

  /** Factory method called by Kinesis whenever it wants to start processing a shard. */
  def createProcessor() = {
    val eventBroker = new EventBroker
    val eventConsumer = new EventConsumer(settings, eventBroker, storageClient, deadLetterQueue)
    _consumers.add(eventConsumer)
    eventConsumer.run()
    eventBroker
  }

  /**
   * Instantiate an appropriately-configured Kinesis worker, which will use the [[createProcessor]]
   * method to spin up consumers as needed.
   */
  private def buildWorker(): Worker = {
    val workerId = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID()

    val kinesisConfig = new KinesisClientLibConfiguration(
      kinesis.applicationName,
      kinesis.streamName,
      awsCredentialsProvider,
      workerId
    )
      .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
      .withKinesisEndpoint(kinesis.kinesisEndpoint)

    val dynamoClient = new AmazonDynamoDBClient(awsCredentialsProvider)
    dynamoClient.setEndpoint(kinesis.dynamoEndpoint)

    val builder = new Worker.Builder()
      .recordProcessorFactory(this)
      .config(kinesisConfig)
      .dynamoDBClient(dynamoClient)

    if (kinesis.disableCloudWatch) {
      builder.metricsFactory(new NullMetricsFactory).build()
    } else {
      builder.build()
    }
  }
}
