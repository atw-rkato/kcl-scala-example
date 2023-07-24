package com.example

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{
  AWSCredentialsProvider,
  AWSStaticCredentialsProvider,
  BasicAWSCredentials
}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.dynamodbv2.streamsadapter.{
  AmazonDynamoDBStreamsAdapterClient,
  StreamsWorkerFactory
}
import com.amazonaws.services.dynamodbv2.{
  AmazonDynamoDBClientBuilder,
  AmazonDynamoDBStreamsClientBuilder
}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  InitialPositionInStream,
  KinesisClientLibConfiguration,
  SimpleRecordsFetcherFactory,
  Worker
}

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.util.UUID
import java.util.concurrent.{ExecutionException, TimeUnit, TimeoutException}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

object Main {
  def main(args: Array[String]): Unit = {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val credentialsProvider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials("dummyKey", "dummySecret")
    )
    val runner = KclSample.init(credentialsProvider)
    Await.result(runner.run(), Duration.Inf)
  }
}

object KclSample {
  private final val APP_NAME = "kcl2-consumer-scala-example"
  private final val DYNAMODB_ENDPOINT = "http://localhost:49000"
  private final val CLOUD_WATCH_ENDPOINT = "http://localhost:4566"
  private final val REGION = "ap-northeast-1"

  def init(credentialsProvider: AWSCredentialsProvider): KclSample = {
    val dynamoDBClient = AmazonDynamoDBClientBuilder
      .standard()
      .withEndpointConfiguration(
        new AwsClientBuilder.EndpointConfiguration(
          DYNAMODB_ENDPOINT,
          REGION
        )
      )
      .withCredentials(credentialsProvider)
      .build()
    val dynamoDBHelper = new DynamoDBHelper(dynamoDBClient)

    val streamName = dynamoDBHelper.setUpTable()

    val cloudWatchClient = AmazonCloudWatchClientBuilder
      .standard()
      .withEndpointConfiguration(
        new AwsClientBuilder.EndpointConfiguration(
          CLOUD_WATCH_ENDPOINT,
          REGION
        )
      )
      .withCredentials(credentialsProvider)
      .build()

    val adapterClient = new AmazonDynamoDBStreamsAdapterClient(
      AmazonDynamoDBStreamsClientBuilder
        .standard()
        .withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(
            DYNAMODB_ENDPOINT,
            REGION
          )
        )
        .withCredentials(credentialsProvider)
        .build()
    )

    val workerConfig = new KinesisClientLibConfiguration(
      APP_NAME,
      streamName,
      null,
      DYNAMODB_ENDPOINT,
      InitialPositionInStream.TRIM_HORIZON,
      credentialsProvider,
      credentialsProvider,
      credentialsProvider,
      KinesisClientLibConfiguration.DEFAULT_FAILOVER_TIME_MILLIS,
      UUID.randomUUID().toString,
      KinesisClientLibConfiguration.DEFAULT_MAX_RECORDS,
      KinesisClientLibConfiguration.DEFAULT_IDLETIME_BETWEEN_READS_MILLIS,
      KinesisClientLibConfiguration.DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST,
      KinesisClientLibConfiguration.DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS,
      KinesisClientLibConfiguration.DEFAULT_SHARD_SYNC_INTERVAL_MILLIS,
      KinesisClientLibConfiguration.DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION,
      new ClientConfiguration,
      new ClientConfiguration,
      new ClientConfiguration,
      KinesisClientLibConfiguration.DEFAULT_TASK_BACKOFF_TIME_MILLIS,
      KinesisClientLibConfiguration.DEFAULT_METRICS_BUFFER_TIME_MILLIS,
      KinesisClientLibConfiguration.DEFAULT_METRICS_MAX_QUEUE_SIZE,
      KinesisClientLibConfiguration.DEFAULT_VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECKPOINTING,
      "ap-northeast-1",
      KinesisClientLibConfiguration.DEFAULT_SHUTDOWN_GRACE_MILLIS,
      KinesisClientLibConfiguration.DEFAULT_DDB_BILLING_MODE,
      new SimpleRecordsFetcherFactory,
      1.minutes.toMillis,
      5.minutes.toMillis,
      30.minutes.toMillis
    )

    val worker = StreamsWorkerFactory.createDynamoDbStreamsWorker(
      () => new SampleRecordProcessor,
      workerConfig,
      adapterClient,
      dynamoDBClient,
      cloudWatchClient
    )

    new KclSample(worker)
  }
}
class KclSample private (private val worker: Worker) {

  def run()(implicit ec: ExecutionContext): Future[Unit] = {
    val schedulerThread = new Thread(worker)
    schedulerThread.setDaemon(true)
    schedulerThread.start()

    println("Press enter to shutdown")
    val reader = new BufferedReader(new InputStreamReader(System.in))
    try reader.readLine
    catch {
      case ioex: IOException =>
        println(
          s"Caught exception while waiting for confirm. Shutting down. ${ioex}"
        )
    }

    val gracefulShutdownFuture = worker.startGracefulShutdown
    println("Waiting up to 20 seconds for shutdown to complete.")
    Future(gracefulShutdownFuture.get(20, TimeUnit.SECONDS)).transform {
      case Success(_) =>
        println("Completed, shutting down now.")
        Success(())
      case Failure(e) =>
        e match {
          case _: InterruptedException =>
            println(
              "Interrupted while waiting for graceful shutdown. Continuing."
            )
          case e: ExecutionException =>
            println(s"Exception while executing graceful shutdown. ${e}")
          case _: TimeoutException =>
            println(
              "Timeout while waiting for shutdown.  Worker may not have exited."
            )
        }
        Failure(e)
    }
  }
}
