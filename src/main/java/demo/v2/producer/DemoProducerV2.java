package demo.v2.producer;


import static demo.util.Constants.LOCALSTACK_REGION;
import static demo.util.Constants.LOCALSTACK_STREAM;
import static demo.util.Constants.LOCALSTACK_URL;
import static demo.util.JsonUtils.toJson;
import static java.util.Collections.singletonMap;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static software.amazon.awssdk.core.SdkBytes.fromUtf8String;
import static software.amazon.kinesis.common.KinesisClientUtil.createKinesisAsyncClient;

import java.net.URI;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

public class DemoProducerV2 {

  private final KinesisAsyncClient kinesis = createKinesisAsyncClient(KinesisAsyncClient.builder()
      .endpointOverride(URI.create(LOCALSTACK_URL))
      .region(Region.of(LOCALSTACK_REGION)));

  public void send(Object record) throws Exception {
    kinesis.putRecord(PutRecordRequest.builder()
        .partitionKey(randomAlphabetic(10))
        .streamName(LOCALSTACK_STREAM)
        .data(fromUtf8String(toJson(record)))
        .build()).get();
    System.out.println("Sent: " + record);
  }

  public static void main(String[] args) throws Exception {
    DemoProducerV2 producer = new DemoProducerV2();
    for (int i = 0; i < 1000; i++) {
      System.out.println("Sending..." + i);
      producer.send(singletonMap("value", i));
      Thread.sleep(3000);
    }
    System.out.println("Done!");
  }
}