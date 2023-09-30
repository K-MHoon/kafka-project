package com.kmhoon.kafka.producer;

import com.kmhoon.kafka.model.OrderModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class OrderSerdeProducer {

    public static  final Logger logger = LoggerFactory.getLogger(OrderSerdeProducer.class.getName());

    public static void main(String[] args) {
        String topicName = "order-serde-topic";

        // Properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class.getName());

        // KafkaProducer
        KafkaProducer<String, OrderModel> kafkaProducer = new KafkaProducer<>(props);
        String filePath = "C:\\Users\\MHK\\IdeaProjects\\kafka-project\\practice\\src\\main\\resources\\sample.txt";

        // Kafka 객체 생성 -> Record 객체 생성 -> send() 전송
        sendFileMessages(kafkaProducer, topicName, filePath);

        kafkaProducer.close();
    }

    private static void sendFileMessages(KafkaProducer<String, OrderModel> kafkaProducer, String topicName, String filePath) {
        String line = "";
        final String delimiter = ",";

        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            while((line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);

                String key = tokens[0];
                OrderModel value = new OrderModel(tokens[1], tokens[2], tokens[3],
                        tokens[4], tokens[5], tokens[6], LocalDateTime.parse(tokens[7].trim(), formatter));

                sendMessage(kafkaProducer, topicName, key, value);
            }
        } catch (IOException e) {
            logger.info(e.getMessage());
        }
    }

    private static void sendMessage(KafkaProducer<String, OrderModel> kafkaProducer, String topicName, String key, OrderModel value) {
            ProducerRecord<String, OrderModel> producerRecord = new ProducerRecord<>(topicName, key, value);
            logger.info("key:{}, value:{}", key, value);

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
}
