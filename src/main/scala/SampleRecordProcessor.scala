import com.amazonaws.services.kinesis.clientlibrary.exceptions.{
  InvalidStateException,
  ShutdownException
}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{
  IRecordProcessor,
  IRecordProcessorFactory
}
import com.amazonaws.services.kinesis.clientlibrary.types.{
  InitializationInput,
  ProcessRecordsInput,
  ShutdownInput
}
import com.typesafe.scalalogging.LazyLogging
import org.slf4j.MDC
object SampleRecordProcessor {
  private val SHARD_ID_MDC_KEY = "ShardId"
}

class SampleRecordProcessor extends IRecordProcessor with LazyLogging {
  private var shardId: Option[String] = None

  override def initialize(initializationInput: InitializationInput): Unit = {
    shardId = Some(initializationInput.getShardId)
    MDC.put(SampleRecordProcessor.SHARD_ID_MDC_KEY, shardId.get)
    try
      logger.info(
        "Initializing @ Sequence: {}",
        initializationInput.getExtendedSequenceNumber
      )
    finally MDC.remove(SampleRecordProcessor.SHARD_ID_MDC_KEY)
  }

  override def processRecords(
      processRecordsInput: ProcessRecordsInput
  ): Unit = {
    MDC.put(SampleRecordProcessor.SHARD_ID_MDC_KEY, shardId.get)
    try {
      logger
        .info("Processing {} record(s)", processRecordsInput.getRecords.size)
      processRecordsInput.getRecords.forEach(r =>
        logger.info(
          "Processing record pk: {} -- Seq: {}",
          r.getPartitionKey,
          r.getSequenceNumber
        )
      )
    } catch {
      case _: Throwable =>
        logger.error("Caught throwable while processing records. Aborting.")
        Runtime.getRuntime.halt(1)
    } finally MDC.remove(SampleRecordProcessor.SHARD_ID_MDC_KEY)
  }

  override def shutdown(shutdownInput: ShutdownInput): Unit = {
    MDC.put(SampleRecordProcessor.SHARD_ID_MDC_KEY, shardId.get)
    try {
      logger.info("Scheduler is shutting down, checkpointing.")
      shutdownInput.getCheckpointer.checkpoint()
    } catch {
      case e @ (_: ShutdownException | _: InvalidStateException) =>
        logger.error(
          "Exception while checkpointing at requested shutdown. Giving up.",
          e
        )
    } finally MDC.remove(SampleRecordProcessor.SHARD_ID_MDC_KEY)
  }

}
