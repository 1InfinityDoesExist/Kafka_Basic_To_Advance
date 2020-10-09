package com.kafka.in.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AWSProducer {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    AWSProducer() {
        log.info(":::::::::AWSProducer started");
    }

    public ListenableFuture<SendResult<String, Object>> sendMessage(String topic, Object message) {
        log.info(":::::Inside AWSProducer Class, sendMessage method:::::");
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, message);
        return future;
    }

}
