import com.typesafe.scalalogging.LazyLogging
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  StaticCredentialsProvider
}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.{ConfigsBuilder, KinesisClientUtil}
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.retrieval.polling.PollingConfig

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.URI
import java.util.UUID
import java.util.concurrent.{ExecutionException, TimeUnit, TimeoutException}

object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val runner = KclSample.init()
    runner.run()
  }
}

object KclSample extends LazyLogging {
  private val REGION = Region.AP_NORTHEAST_1
  private val TABLE_NAME = "sample-table"
  private val APP_NAME = "kcl2-consumer-scala-example"

  def init(): KclSample = {
    val kinesisClient = KinesisClientUtil.createKinesisAsyncClient(
      KinesisAsyncClient.builder
        .region(REGION)
        .credentialsProvider(
          StaticCredentialsProvider
            .create(AwsBasicCredentials.create("dummyKey", "dummySecret"))
        )
    )

    val dynamoClient = DynamoDbAsyncClient
      .builder()
      .endpointOverride(URI.create("http://localhost:49000"))
      .region(REGION)
      .credentialsProvider(
        StaticCredentialsProvider
          .create(AwsBasicCredentials.create("dummyKey", "dummySecret"))
      )
      .build()

    val tableDescription = dynamoClient
      .describeTable(
        DescribeTableRequest.builder().tableName(TABLE_NAME).build()
      )
      .get()
      .table()
    logger.info(tableDescription.toString)
    val streamName = tableDescription.latestStreamArn()

    val cloudWatchClient =
      CloudWatchAsyncClient.builder
        .region(REGION)
        .credentialsProvider(
          StaticCredentialsProvider
            .create(AwsBasicCredentials.create("dummyKey", "dummySecret"))
        )
        .endpointOverride(URI.create("http://localhost:4566"))
        .build()
    val configsBuilder = new ConfigsBuilder(
      streamName,
      APP_NAME,
      kinesisClient,
      dynamoClient,
      cloudWatchClient,
      UUID.randomUUID.toString,
      () => new SampleRecordProcessor
    )

    val scheduler = new Scheduler(
      configsBuilder.checkpointConfig,
      configsBuilder.coordinatorConfig,
      configsBuilder.leaseManagementConfig,
      configsBuilder.lifecycleConfig,
      configsBuilder.metricsConfig,
      configsBuilder.processorConfig,
      configsBuilder.retrievalConfig.retrievalSpecificConfig(
        new PollingConfig(streamName, kinesisClient)
      )
    )

    new KclSample(scheduler)
  }
}
class KclSample private (private val scheduler: Scheduler) extends LazyLogging {

  def run(): Unit = {

    /**
      * Kickoff the Scheduler. Record processing of the stream of dummy data will continue indefinitely
      * until an exit is triggered.
      */
    val schedulerThread = new Thread(scheduler)
    schedulerThread.setDaemon(true)
    schedulerThread.start()

    /**
      * Allows termination of app by pressing Enter.
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

    /**
      * Stops consuming data. Finishes processing the current batch of data already received from Kinesis
      * before shutting down.
      */
    val gracefulShutdownFuture =
      scheduler.startGracefulShutdown
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
