package com.kmhoon.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kmhoon.kafka.model.OrderModel;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderSerializer implements Serializer<OrderModel> {

    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    public static  final Logger logger = LoggerFactory.getLogger(OrderSerializer.class.getName());
    @Override
    public byte[] serialize(String topic, OrderModel order) {
        byte[] serializedOrder = null;

        try {
            serializedOrder = objectMapper.writeValueAsBytes(order);
        } catch (JsonProcessingException e) {
            logger.error("Json processing exception:" + e.getMessage());
        }

        return serializedOrder;
    }
}
