package demo.consumer;

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream.LATEST;
import static demo.util.Constants.LOCALSTACK_REGION;
import static demo.util.Constants.LOCALSTACK_STREAM;
import static demo.util.Constants.LOCALSTACK_URL;

import java.util.UUID;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;

public class DemoConsumer {

  private static final String APPLICATION = "demo-consumer";

  public DemoConsumer() {
    Worker worker = new Worker.Builder()
        .recordProcessorFactory(DemoProcessor::new)
        .config(createConfiguration())
        .metricsFactory(new NullMetricsFactory())
        .build();
    Thread thread = new Thread(worker, APPLICATION);
    thread.start();
    System.out.println("Consumer started!");
  }

  private KinesisClientLibConfiguration createConfiguration() {
    return new KinesisClientLibConfiguration(
        APPLICATION,
        LOCALSTACK_STREAM,
        DefaultAWSCredentialsProviderChain.getInstance(),
        APPLICATION + ":" + LOCALSTACK_STREAM + ":" + UUID.randomUUID())
        .withRegionName(LOCALSTACK_REGION)
        .withInitialPositionInStream(LATEST)
        .withKinesisEndpoint(LOCALSTACK_URL)
        .withDynamoDBEndpoint(LOCALSTACK_URL);
  }

  public static void main(String[] args) {
    new DemoConsumer();
    System.out.println("Done!");
  }
}
