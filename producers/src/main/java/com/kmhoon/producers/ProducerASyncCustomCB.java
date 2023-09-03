package com.kmhoon.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerASyncCustomCB {

    public static  final Logger logger = LoggerFactory.getLogger(ProducerASyncCustomCB.class.getName());

    public static void main(String[] args) {
        // Properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        for (int i = 0; i < 20; i++) {
            // ProducerRecord
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("multipart-topic", i, "hello world " + i);
            logger.info("i:" + i);
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
        }


        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
