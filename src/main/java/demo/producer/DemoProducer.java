package demo.producer;

import static demo.util.Constants.LOCALSTACK_HOST;
import static demo.util.Constants.LOCALSTACK_PORT;
import static demo.util.Constants.LOCALSTACK_REGION;
import static demo.util.Constants.LOCALSTACK_STREAM;
import static demo.util.JsonUtils.toJson;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;
import static java.util.UUID.randomUUID;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

public class DemoProducer {

  private final KinesisProducer kinesisProducer;

  public DemoProducer() {
    kinesisProducer = new KinesisProducer(new KinesisProducerConfiguration()
        .setKinesisEndpoint(LOCALSTACK_HOST)
        .setKinesisPort(LOCALSTACK_PORT)
        .setRegion(LOCALSTACK_REGION)
        .setVerifyCertificate(false));
  }

  public void send(Object record) {
    kinesisProducer.addUserRecord(LOCALSTACK_STREAM, randomUUID().toString(), UTF_8.encode(toJson(record)));
    System.out.println("Sent: " + record);
  }

  public static void main(String[] args) throws Exception {
    DemoProducer producer = new DemoProducer();
    for (int i = 0; i < 1000; i++) {
      System.out.println("Sending..." + i);
      producer.send(singletonMap("value", i));
      Thread.sleep(3000);
    }
    System.out.println("Done!");
  }
}
