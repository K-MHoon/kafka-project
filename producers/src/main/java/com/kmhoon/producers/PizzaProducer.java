package com.kmhoon.producers;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducer {

    public static  final Logger logger = LoggerFactory.getLogger(PizzaProducer.class.getName());

    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName,
                                        int iterCount, // 몇 번 반복할 것인지
                                        int interIntervalMillis,
                                        int intervalMillis,
                                        int intervalCount,
                                        boolean sync) {

        PizzaMessage pizzaMessage = new PizzaMessage();
        long seed = 2023;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        int iterSeq = 0;
        while(iterSeq++ != iterCount) {
            HashMap<String, String> produceMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, produceMessage.get("key"), produceMessage.get("message"));
            sendMessage(kafkaProducer, producerRecord, produceMessage, sync);
            if((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    logger.info("###### IntervalCount:" + intervalCount + " intervalMillis:" + interIntervalMillis + " ######");
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            if(interIntervalMillis > 0) {
                try {
                    logger.info("interIntervalMillis:" + interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   HashMap<String, String> produceMessage, boolean sync) {
        if(!sync) {
            kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
                if(exception == null) {
                    logger.info("async message:" + produceMessage.get("key")
                            + " partition:" + recordMetadata.partition()
                            + " offset:" + recordMetadata.offset());
                } else {
                    logger.error("exception error from broker " + exception.getMessage());
                }
            });
        } else {
            try {
                RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
                logger.info("sync message:" + produceMessage.get("key")
                        + " partition:" + recordMetadata.partition()
                        + " offset:" + recordMetadata.offset());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        // Properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        sendPizzaMessage(kafkaProducer, "pizza-topic", -1, 100,1000, 100, true);
        kafkaProducer.close();
    }
}
