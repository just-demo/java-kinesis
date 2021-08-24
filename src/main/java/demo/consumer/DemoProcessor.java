package demo.consumer;

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason.TERMINATE;
import static demo.util.JsonUtils.fromJson;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.CharBuffer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

public class DemoProcessor implements IRecordProcessor {

  @Override
  public void initialize(InitializationInput init) {
    System.out.println("Consumer ShardId: " + init.getShardId());
    System.out.println("Consumer ExtendedSequenceNumber: " + init.getExtendedSequenceNumber());
    System.out.println("Consumer PendingCheckpointSequenceNumber: " + init.getPendingCheckpointSequenceNumber());
  }

  @Override
  public void processRecords(ProcessRecordsInput processRecordsInput) {
    processRecordsInput.getRecords().stream()
        .map(Record::getData)
        .map(UTF_8::decode)
        .map(CharBuffer::toString)
        .map(record -> fromJson(record, Object.class))
        .forEach(record -> System.out.println("Processed: " + record));
    checkpoint(processRecordsInput.getCheckpointer());
    System.out.println("Checkpointed after batch");
  }

  @Override
  public void shutdown(ShutdownInput shutdownInput) {
    System.out.println("Shutting down...");
    if (shutdownInput.getShutdownReason() == TERMINATE) {
      checkpoint(shutdownInput.getCheckpointer());
      System.out.println("Checkpointed on shutdown");
    }
  }

  private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
    try {
      checkpointer.checkpoint();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
