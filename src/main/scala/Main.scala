import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{
  AWSCredentialsProvider,
  AWSStaticCredentialsProvider,
  BasicAWSCredentials
}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  KinesisClientLibConfiguration,
  SimpleRecordsFetcherFactory,
  Worker
}
import com.typesafe.scalalogging.LazyLogging

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.time.Duration
import java.util.UUID
import java.util.concurrent.{ExecutionException, TimeUnit, TimeoutException}

object Main {
  def main(args: Array[String]): Unit = {
    val credentialsProvider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials("dummyKey", "dummySecret")
    )
    val runner = KclSample.init(credentialsProvider)
    runner.run()
  }
}

object KclSample extends LazyLogging {
  private val TABLE_NAME = "sample-table"
  private val APP_NAME = "kcl2-consumer-scala-example"
  private val DYNAMODB_ENDPOINT = "http://localhost:49000"
  private val REGION = "ap-northeast-1"

  def init(credentialsProvider: AWSCredentialsProvider): KclSample = {
    val streamName: String = getTargetStreamArn(credentialsProvider)

    val config = new KinesisClientLibConfiguration(
      APP_NAME,
      streamName,
      null,
      DYNAMODB_ENDPOINT,
      KinesisClientLibConfiguration.DEFAULT_INITIAL_POSITION_IN_STREAM,
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
      Duration.ofMinutes(1).toMillis,
      Duration.ofMinutes(5).toMillis,
      Duration.ofMinutes(30).toMillis
    )

    val worker = new Worker.Builder()
      .recordProcessorFactory(() => new SampleRecordProcessor)
      .config(config)
      .build()

    new KclSample(worker)
  }

  private def getTargetStreamArn(
      credentialsProvider: AWSCredentialsProvider
  ) = {
    val dynamoClient = AmazonDynamoDBClientBuilder
      .standard()
      .withEndpointConfiguration(
        new AwsClientBuilder.EndpointConfiguration(
          DYNAMODB_ENDPOINT,
          REGION
        )
      )
      .withCredentials(credentialsProvider)
      .build()

    val tableDescription = dynamoClient
      .describeTable(
        new DescribeTableRequest(TABLE_NAME)
      )
      .getTable
    logger.info(tableDescription.toString)
    val streamName = tableDescription.getLatestStreamArn
    streamName
  }
}
class KclSample private (private val worker: Worker) extends LazyLogging {

  def run(): Unit = {

    /** Kickoff the Scheduler. Record processing of the stream of dummy data
      * will continue indefinitely until an exit is triggered.
      */
    val schedulerThread = new Thread(worker)
    schedulerThread.setDaemon(true)
    schedulerThread.start()

    /** Allows termination of app by pressing Enter.
      */
    println("Press enter to shutdown")
    val reader = new BufferedReader(new InputStreamReader(System.in))
    try reader.readLine
    catch {
      case ioex: IOException =>
        logger.error(
          "Caught exception while waiting for confirm. Shutting down.",
          ioex
        )
    }

    /** Stops consuming data. Finishes processing the current batch of data
      * already received from Kinesis before shutting down.
      */
    val gracefulShutdownFuture =
      worker.startGracefulShutdown
    logger.info("Waiting up to 20 seconds for shutdown to complete.")
    try gracefulShutdownFuture.get(20, TimeUnit.SECONDS)
    catch {
      case _: InterruptedException =>
        logger.info(
          "Interrupted while waiting for graceful shutdown. Continuing."
        )
      case e: ExecutionException =>
        logger
          .error("Exception while executing graceful shutdown.", e)
      case _: TimeoutException =>
        logger.error(
          "Timeout while waiting for shutdown.  Scheduler may not have exited."
        )
    }
    logger.info("Completed, shutting down now.")
  }
}
