package com.kafka.in.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AWSProducer {

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    AWSProducer() {
        log.info(":::::::::AWSProducer started");
    }

    public ListenableFuture<SendResult<Integer, String>> sendMessage(String topic, String message) {
        log.info(":::::Inside AWSProducer Class, sendMessage method:::::");
        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(topic, message);
        return future;
    }

}
