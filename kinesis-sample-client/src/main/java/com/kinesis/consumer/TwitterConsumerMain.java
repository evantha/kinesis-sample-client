package com.kinesis.consumer;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.handlers.HandlerContextKey;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class TwitterConsumerMain {
    public static final String KINESIS_TOPIC = "test_gme_2";
    public static final String AWS_REGION = "us-east-1";

    public static void main(String[] args) {

        System.out.println(HandlerContextKey.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(
                "tweets-processor-gme", KINESIS_TOPIC,
                new DefaultAWSCredentialsProviderChain(), "worker-1");

        System.out.println(config.getRegionName());

        config.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        // every 200 milliseconds, consumer fetches new data
        config.withIdleMillisBetweenCalls(200);
        config.withRegionName(AWS_REGION);

        IRecordProcessorFactory factory = new TwitterProcessorFactory();
        Worker worker = new Worker.Builder().config(config).recordProcessorFactory(factory).build();
        worker.run();
    }
}
