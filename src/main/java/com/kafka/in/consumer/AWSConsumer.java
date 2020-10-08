package com.kafka.in.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AWSConsumer {

    AWSConsumer() {
        log.info(":::::AWSConsumer has been started:::::");
    }

    @KafkaListener(topics = "bulk")
    public void consumes(ConsumerRecord<String, Object> message) {
        log.info(":::AWSConsumer Class ,consumes method::::");
    }
}
