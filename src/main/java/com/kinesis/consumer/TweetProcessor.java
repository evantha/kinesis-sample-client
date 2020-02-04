package com.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.commons.lang.CharEncoding;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class TweetProcessor implements IRecordProcessor {
    public void initialize(InitializationInput initializationInput) {

    }

    public void processRecords(ProcessRecordsInput processRecordsInput) {
        for (Record record : processRecordsInput.getRecords()) {
            System.out.println(getStatus(record));
            checkpoint(processRecordsInput.getCheckpointer());
        }

    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        try {
            checkpointer.checkpoint();
        } catch (InvalidStateException e) {
            // Dynamo table does not exist
            e.printStackTrace();
        } catch (ShutdownException e) {
            // Two processors are processing the same record
            e.printStackTrace();
        }
    }

    private Status getStatus(Record record) {
        ByteBuffer data = record.getData();
        Status status = null;
        try {
            String tweetJson = new String(data.array(), CharEncoding.UTF_8);
            status = parseTweet(tweetJson);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return status;
    }

    private Status parseTweet(String tweetJson) {
        try {
            return TwitterObjectFactory.createStatus(tweetJson);
        } catch (TwitterException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void shutdown(ShutdownInput shutdownInput) {
        ShutdownReason shutdownReason = shutdownInput.getShutdownReason();

        switch (shutdownReason) {
            case TERMINATE:
            case REQUESTED:
                checkpoint(shutdownInput.getCheckpointer());
                break;
            case ZOMBIE:
                System.out.println("There is another processor consuming data. So no checkpointing");
                break;
        }
    }
}
