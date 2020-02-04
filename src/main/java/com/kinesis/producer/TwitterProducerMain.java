package com.kinesis.producer;

import com.amazonaws.services.kinesis.producer.*;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang.CharEncoding;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;


public class TwitterProducerMain {

    public static final String KINESIS_TOPIC = "test_gme_2";
    public static final String AWS_REGION = "us-east-1";

    public static void main(String[] args) {
        TwitterStream twitterStream = createTwitterStream();
        twitterStream.addListener(createListener());
        twitterStream.sample();
    }

    private static TwitterStream createTwitterStream() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(false)
                .setOAuthConsumerKey("PMFEXRi3QJjfLl6fGu54TfGqs")
                .setOAuthConsumerSecret("IGbPN4QNhdfFaLPL8gRLEYPJfwyiu74sajA7hsfaobHyKdyjg2")
                .setOAuthAccessToken("2348218487-AlBJ13MAadXMyiew5qRb7cIe7vKmtF0DzszAHFa")
                .setOAuthAccessTokenSecret("JDf00njj5d8C6pKsJ7xXoPYZ7iVBYPqGGqrZOyJjhBlzi");

        TwitterStream ts = new TwitterStreamFactory(cb.build()).getInstance();
        return ts;

    }

    private static RawStreamListener createListener() {
        KinesisProducer kinesisProducer = createKinesisProducer();
        return new TweetsStatusListener(kinesisProducer);
    }

    private static KinesisProducer createKinesisProducer() {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                .setRequestTimeout(60000)
                .setRecordMaxBufferedTime(1500)
                .setRegion(AWS_REGION);
        return new KinesisProducer(config);
    }

    private static class TweetsStatusListener implements RawStreamListener {

        private KinesisProducer kinesisProducer;
        private int count;

        public TweetsStatusListener(KinesisProducer kinesisProducer) {
            this.kinesisProducer = kinesisProducer;
        }

        public void onMessage(String rawString) {
            if (count % 5 != 0) return;
            try {
                Status status = TwitterObjectFactory.createStatus(rawString);
                if (status.getUser() != null) {
                    byte[] tweetBytes = rawString.getBytes(CharEncoding.UTF_8);
                    String partitionKey = status.getLang();
                    ListenableFuture<UserRecordResult> lf =
                            this.kinesisProducer.addUserRecord(KINESIS_TOPIC, partitionKey, ByteBuffer.wrap(tweetBytes));

                    Futures.addCallback(lf, new FutureCallback<UserRecordResult>() {
                        public void onSuccess(UserRecordResult result) {
                            System.out.println(String.format("success - %s", result.getSequenceNumber()));
                        }

                        public void onFailure(Throwable throwable) {
                            if (throwable instanceof UserRecordFailedException) {
                                UserRecordFailedException ex = (UserRecordFailedException) throwable;
                                UserRecordResult result = ex.getResult();

                                Attempt last = Iterables.getLast(result.getAttempts());
                                System.err.println(String.format("Put failed - %s", last.getErrorMessage()));
                            }
                        }
                    });
                }
            } catch (TwitterException e) {
                e.printStackTrace();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        public void onException(Exception ex) {
            ex.printStackTrace();
        }
    }
}
