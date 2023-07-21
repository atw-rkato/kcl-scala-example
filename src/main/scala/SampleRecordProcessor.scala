import com.typesafe.scalalogging.LazyLogging
import org.slf4j.MDC
import software.amazon.kinesis.exceptions.{
  InvalidStateException,
  ShutdownException
}
import software.amazon.kinesis.lifecycle.events.{
  InitializationInput,
  LeaseLostInput,
  ProcessRecordsInput,
  ShardEndedInput,
  ShutdownRequestedInput
}
import software.amazon.kinesis.processor.ShardRecordProcessor

/**
  * The implementation of the ShardRecordProcessor interface is where the heart of the record processing logic lives.
  * In this example all we do to 'process' is log info about the records.
  */
object SampleRecordProcessor {
  private val SHARD_ID_MDC_KEY = "ShardId"
}

class SampleRecordProcessor extends ShardRecordProcessor with LazyLogging {
  private var shardId: Option[String] = None

  /**
    * Invoked by the KCL before data records are delivered to the ShardRecordProcessor instance (via
    * processRecords). In this example we do nothing except some logging.
    *
    * @param initializationInput Provides information related to initialization.
    */
  def initialize(initializationInput: InitializationInput): Unit = {
    shardId = Some(initializationInput.shardId)
    MDC.put(SampleRecordProcessor.SHARD_ID_MDC_KEY, shardId.get)
    try logger.info(
      "Initializing @ Sequence: {}",
      initializationInput.extendedSequenceNumber
    )
    finally MDC.remove(SampleRecordProcessor.SHARD_ID_MDC_KEY)
  }

  /**
    * Handles record processing logic. The Amazon Kinesis Client Library will invoke this method to deliver
    * data records to the application. In this example we simply log our records.
    *
    * @param processRecordsInput Provides the records to be processed as well as information and capabilities
    *                            related to them (e.g. checkpointing).
    */
  def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {

    MDC.put(SampleRecordProcessor.SHARD_ID_MDC_KEY, shardId.get)
    try {
      logger
        .info("Processing {} record(s)", processRecordsInput.records.size)
      processRecordsInput.records.forEach(
        r =>
          logger.info(
            "Processing record pk: {} -- Seq: {}",
            r.partitionKey,
            r.sequenceNumber
        )
      )
    } catch {
      case _: Throwable =>
        logger.error("Caught throwable while processing records. Aborting.")
        Runtime.getRuntime.halt(1)
    } finally MDC.remove(SampleRecordProcessor.SHARD_ID_MDC_KEY)
  }

  /** Called when the lease tied to this record processor has been lost. Once the lease has been lost,
    * the record processor can no longer checkpoint.
    *
    * @param leaseLostInput Provides access to functions and data related to the loss of the lease.
    */
  def leaseLost(leaseLostInput: LeaseLostInput): Unit = {
    MDC.put(SampleRecordProcessor.SHARD_ID_MDC_KEY, shardId.get)
    try logger.info("Lost lease, so terminating.")
    finally MDC.remove(SampleRecordProcessor.SHARD_ID_MDC_KEY)
  }

  /**
    * Called when all data on this shard has been processed. Checkpointing must occur in the method for record
    * processing to be considered complete; an exception will be thrown otherwise.
    *
    * @param shardEndedInput Provides access to a checkpointer method for completing processing of the shard.
    */
  def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
    MDC.put(SampleRecordProcessor.SHARD_ID_MDC_KEY, shardId.get)
    try {
      logger.info("Reached shard end checkpointing.")
      shardEndedInput.checkpointer.checkpoint()
    } catch {
      case e @ (_: ShutdownException | _: InvalidStateException) =>
        logger
          .error("Exception while checkpointing at shard end. Giving up.", e)
    } finally MDC.remove(SampleRecordProcessor.SHARD_ID_MDC_KEY)
  }

  /**
    * Invoked when Scheduler has been requested to shut down (i.e. we decide to stop running the app by pressing
    * Enter). Checkpoints and logs the data a final time.
    *
    * @param shutdownRequestedInput Provides access to a checkpointer, allowing a record processor to checkpoint
    *                               before the shutdown is completed.
    */
  def shutdownRequested(
    shutdownRequestedInput: ShutdownRequestedInput
  ): Unit = {
    MDC.put(SampleRecordProcessor.SHARD_ID_MDC_KEY, shardId.get)
    try {
      logger.info("Scheduler is shutting down, checkpointing.")
      shutdownRequestedInput.checkpointer.checkpoint()
    } catch {
      case e @ (_: ShutdownException | _: InvalidStateException) =>
        logger.error(
          "Exception while checkpointing at requested shutdown. Giving up.",
          e
        )
    } finally MDC.remove(SampleRecordProcessor.SHARD_ID_MDC_KEY)
  }
}
