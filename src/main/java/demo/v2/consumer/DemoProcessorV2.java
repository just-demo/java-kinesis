package demo.v2.consumer;

import static demo.util.JsonUtils.fromJson;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.CharBuffer;

import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class DemoProcessorV2 implements ShardRecordProcessor {

  public DemoProcessorV2() {
    System.out.println("Processor created");
  }

  @Override
  public void initialize(InitializationInput init) {
    System.out.println("Consumer ShardId: " + init.shardId());
    System.out.println("Consumer ExtendedSequenceNumber: " + init.extendedSequenceNumber());
    System.out.println("Consumer PendingCheckpointSequenceNumber: " + init.pendingCheckpointSequenceNumber());
  }

  @Override
  public void processRecords(ProcessRecordsInput processRecordsInput) {
    System.out.println("Received count: " + processRecordsInput.records().size());
    processRecordsInput.records().stream()
        .map(KinesisClientRecord::data)
        .map(UTF_8::decode)
        .map(CharBuffer::toString)
        .map(record -> fromJson(record, Object.class))
        .forEach(record -> System.out.println("Processed: " + record));
    checkpoint(processRecordsInput.checkpointer());
  }

  @Override
  public void leaseLost(LeaseLostInput leaseLostInput) {
    System.out.println("Lost lease: " + leaseLostInput);
    // Unable to checkpoint here
  }

  @Override
  public void shardEnded(ShardEndedInput shardEndedInput) {
    System.out.println("Shard processed");
    checkpoint(shardEndedInput.checkpointer());
  }

  @Override
  public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
    System.out.println("Shutdown requested");
    checkpoint(shutdownRequestedInput.checkpointer());
  }

  private void checkpoint(RecordProcessorCheckpointer checkpointer) {
    try {
      checkpointer.checkpoint();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
