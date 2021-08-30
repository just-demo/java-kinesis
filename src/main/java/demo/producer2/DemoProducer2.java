package demo.producer2;

import static demo.util.Constants.LOCALSTACK_REGION;
import static demo.util.Constants.LOCALSTACK_STREAM;
import static demo.util.Constants.LOCALSTACK_URL;
import static demo.util.JsonUtils.toJson;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;
import static java.util.UUID.randomUUID;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

public class DemoProducer2 {

  private final AmazonKinesis kinesis = AmazonKinesisClientBuilder.standard()
      .withEndpointConfiguration(new EndpointConfiguration(LOCALSTACK_URL, LOCALSTACK_REGION))
      .build();

  public void send(Object record) {
    kinesis.putRecord(new PutRecordRequest()
        .withStreamName(LOCALSTACK_STREAM)
        .withPartitionKey(randomUUID().toString())
        .withData(UTF_8.encode(toJson(record))));

    System.out.println("Sent: " + record);
  }

  public static void main(String[] args) throws Exception {
    DemoProducer2 producer = new DemoProducer2();
    for (int i = 0; i < 1000; i++) {
      System.out.println("Sending..." + i);
      producer.send(singletonMap("value", i));
      Thread.sleep(3000);
    }
    System.out.println("Done!");
  }
}
