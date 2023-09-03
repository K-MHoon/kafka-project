package com.kmhoon.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleProducerSync {

    public static  final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class.getName());

    public static void main(String[] args) {
        // Properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // ProducerRecord
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("simple-topic", "hello world sync");

        //KafkaProducer send Message
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("\n ##### record Metadata received ##### \n" +
                    "partition:" + recordMetadata.partition() + "\n" +
                    "offset:" + recordMetadata.offset() + "\n" +
                    "timestamp:" + recordMetadata.timestamp());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }
}
