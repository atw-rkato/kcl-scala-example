package com.example

import com.amazonaws.services.kinesis.clientlibrary.exceptions.{InvalidStateException, ShutdownException}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import io.circe.{Json, parser}

import java.nio.charset.StandardCharsets
class SampleRecordProcessor extends IRecordProcessor {

  override def initialize(initializationInput: InitializationInput): Unit = {
    println(s"shardId: ${initializationInput.getShardId}")
  }

  override def processRecords(
      processRecordsInput: ProcessRecordsInput
  ): Unit = {
    val records = processRecordsInput.getRecords
    println(s"Processing ${records.size} record(s)")
    records.forEach(r => {
      val data = new String(r.getData.array(), StandardCharsets.UTF_8)
      val json = parser.parse(data).getOrElse(Json.Null)
      println(json)
    })
  }

  override def shutdown(shutdownInput: ShutdownInput): Unit = {
    try {
      println("Worker is shutting down, checkpointing.")
      shutdownInput.getCheckpointer.checkpoint()
    } catch {
      case e @ (_: ShutdownException | _: InvalidStateException) =>
        println(
          s"Exception while checkpointing at requested shutdown. Giving up. ${e}"
        )
    }
  }
}
