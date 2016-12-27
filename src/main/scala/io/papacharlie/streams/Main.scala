//package io.papacharlie.streams
//
//import com.twitter.conversions.time._
//import com.twitter.util._
//import java.security.Security
//
///**
// * The entrypoint for Diary's Kinesis consumer, which is responsible for consuming events enqueued
// * by IFE and storing them for retrieval by the [[com.ifttt.diary.server]] package.
// */
//object Main extends CustomizedTwitterServer {
//  val settings = Settings.loadSettings
//  import settings.kinesis
//  val kinesisClient = new SyncKinesisClient(settings)
//
//  // Start reporting metrics to CloudWatch or wherever.
//  MetricsReporters.start
//
//  // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
//  // Borrowed from the Kinesis sample consumer.
//  Security.setProperty("networkaddress.cache.ttl", "60")
//
//  // Ensure the necessary Kinesis streams exist. This should probably only ever be relevant in
//  // development.
//  kinesisClient.ensureStreamExists(kinesis.streamName)
//  kinesisClient.ensureStreamExists(kinesis.deadLetterStreamName)
//
//  // Set up the dead-letter queue thread, which is responsible for backing up events that fail too
//  // many times.
//  val deadLetterQueue = new DeadLetterQueue(kinesisClient, kinesis.deadLetterStreamName)
//  val deadLetterQueueThread = new Thread(deadLetterQueue)
//  deadLetterQueueThread.start()
//
//  // Set up the Kinesis worker, which will take control of the main thread momentarily.
//  val worker = new KinesisWorkerManager(settings, new EventStorageClient(settings), deadLetterQueue)
//
//  // Set up a shutdown hook to clean things up.
//  val mainThread = Thread.currentThread()
//  Runtime.getRuntime.addShutdownHook(
//    new Thread {
//      override def run(): Unit = {
//        log.ifWarning("Received shutdown notification; stopping worker and consumers")
//        worker.shutdown()
//        mainThread.join()
//      }
//    }
//  )
//
//  def main(): Unit = {
//    // Run the Kinesis worker. This blocks until the worker is shut down.
//    worker.run()
//
//
//    // Once the worker loop is over, wait for all consumers to finish if possible.
//    val startTime = Time.now
//    while (!worker.consumers.forall(_.isCompletelyShutDown.get) && startTime < 10.seconds.ago) {
//      Thread.sleep(1000)
//    }
//
//    // Then shut down the dead letter queue.
//    deadLetterQueue.inbox ! DeadLetterQueue.ShutDown
//    deadLetterQueueThread.join()
//  }
//}
