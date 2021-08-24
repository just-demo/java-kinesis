package demo.consumer2;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST;
import static demo.util.Constants.LOCALSTACK_REGION;
import static demo.util.Constants.LOCALSTACK_STREAM;
import static demo.util.Constants.LOCALSTACK_URL;
import static demo.util.JsonUtils.fromJson;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

import java.nio.CharBuffer;
import java.util.List;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.Record;

public class DemoConsumer2 {

  private final AmazonKinesis kinesis;
  private String shardIterator;

  public DemoConsumer2() {
    kinesis = AmazonKinesisClientBuilder.standard()
        .withEndpointConfiguration(new EndpointConfiguration(LOCALSTACK_URL, LOCALSTACK_REGION))
        .build();
    shardIterator = getShardIterator();
  }

  public List<Object> receive() {
    GetRecordsResult records = kinesis.getRecords(new GetRecordsRequest()
        .withShardIterator(shardIterator));
    shardIterator = records.getNextShardIterator();
    return records.getRecords().stream()
        .map(Record::getData)
        .map(UTF_8::decode)
        .map(CharBuffer::toString)
        .map(record -> fromJson(record, Object.class))
        .collect(toList());
  }

  private String getShardIterator() {
    String shardId = kinesis.listShards(new ListShardsRequest().withStreamName(LOCALSTACK_STREAM))
        .getShards()
        .get(0)
        .getShardId();
    return kinesis.getShardIterator(new GetShardIteratorRequest()
        .withStreamName(LOCALSTACK_STREAM)
        .withShardId(shardId)
        .withShardIteratorType(LATEST)
    ).getShardIterator();
  }

  public static void main(String[] args) throws Exception {
    DemoConsumer2 consumer = new DemoConsumer2();
    for (int i = 0; i < 1000; i++) {
      System.out.println("Receiving..." + i);
      List<Object> records = consumer.receive();
      System.out.println("Received count: " + records.size());
      records.forEach(record -> System.out.println("Received: " + record));
      Thread.sleep(3000);
    }
    System.out.println("Done!");
  }
}
