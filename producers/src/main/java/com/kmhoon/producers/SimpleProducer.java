package com.kmhoon.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {

    public static void main(String[] args) {
        // Properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // ProducerRecord
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("simple-topic", "hello world 2");

        //KafkaProducer send Message
        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
