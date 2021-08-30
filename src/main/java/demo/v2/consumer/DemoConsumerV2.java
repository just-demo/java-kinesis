package demo.v2.consumer;


import static demo.util.Constants.LOCALSTACK_REGION;
import static demo.util.Constants.LOCALSTACK_STREAM;
import static demo.util.Constants.LOCALSTACK_URL;
import static java.util.concurrent.TimeUnit.SECONDS;
import static software.amazon.kinesis.common.KinesisClientUtil.createKinesisAsyncClient;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.Future;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

public class DemoConsumerV2 {

  private static final String APPLICATION = "demo-consumer-v2";

  private final Scheduler scheduler;

  public DemoConsumerV2() {
    KinesisAsyncClient kinesisClient = createKinesisAsyncClient(KinesisAsyncClient.builder()
        .endpointOverride(URI.create(LOCALSTACK_URL))
        .region(Region.of(LOCALSTACK_REGION)));

    DynamoDbAsyncClient dynamodbClient = DynamoDbAsyncClient.builder()
        .endpointOverride(URI.create(LOCALSTACK_URL))
        .region(Region.of(LOCALSTACK_REGION))
        .build();

    CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder()
        .endpointOverride(URI.create(LOCALSTACK_URL))
        .region(Region.of(LOCALSTACK_REGION))
        .build();

    ConfigsBuilder configsBuilder = new ConfigsBuilder(
        LOCALSTACK_STREAM,
        APPLICATION,
        kinesisClient,
        dynamodbClient,
        cloudWatchClient,
        APPLICATION + ":" + LOCALSTACK_STREAM + ":" + UUID.randomUUID(),
        DemoProcessorV2::new);

    // TODO: this does not seem to work with localstack
    scheduler = new Scheduler(
        configsBuilder.checkpointConfig(),
        configsBuilder.coordinatorConfig(),
        configsBuilder.leaseManagementConfig(),
        configsBuilder.lifecycleConfig(),
        configsBuilder.metricsConfig(),
        configsBuilder.processorConfig(),
        configsBuilder.retrievalConfig()
//            .initialPositionInStreamExtended(newInitialPositionAtTimestamp(new Date()))
//            .initialPositionInStreamExtended(newInitialPosition(TRIM_HORIZON))
            .retrievalSpecificConfig(new PollingConfig(LOCALSTACK_STREAM, kinesisClient))
    );

    Thread schedulerThread = new Thread(scheduler);
    schedulerThread.setDaemon(true);
    schedulerThread.start();
  }

  public void gracefulShutdown() throws Exception {
    Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
    gracefulShutdownFuture.get(30, SECONDS);
  }

  public static void main(String[] args) {
    new DemoConsumerV2();
    System.out.println("Done!");
  }

}