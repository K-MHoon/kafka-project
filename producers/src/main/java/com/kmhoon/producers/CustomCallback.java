package com.kmhoon.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {

    public static  final Logger logger = LoggerFactory.getLogger(CustomCallback.class.getName());
    private int seq;

    public CustomCallback(int seq) {
        this.seq = seq;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        if(exception == null) {
            logger.info("\n ##### record Metadata received ##### \n" +
                    "seq:" + seq + "\n" +
                    "partition:" + recordMetadata.partition() + "\n" +
                    "offset:" + recordMetadata.offset() + "\n" +
                    "timestamp:" + recordMetadata.timestamp());
        } else {
            logger.error("exception error from broker " + exception.getMessage());
        }
    }
}
