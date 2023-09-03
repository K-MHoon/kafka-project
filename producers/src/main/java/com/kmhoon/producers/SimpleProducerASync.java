package com.kmhoon.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerASync {

    public static  final Logger logger = LoggerFactory.getLogger(SimpleProducerASync.class.getName());

    public static void main(String[] args) {
        // Properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // ProducerRecord
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("simple-topic", "hello world Async");

        //KafkaProducer send Message
        kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
            if(exception == null) {
                logger.info("\n ##### record Metadata received ##### \n" +
                        "partition:" + recordMetadata.partition() + "\n" +
                        "offset:" + recordMetadata.offset() + "\n" +
                        "timestamp:" + recordMetadata.timestamp());
            } else {
                logger.error("exception error from broker " + exception.getMessage());
            }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
