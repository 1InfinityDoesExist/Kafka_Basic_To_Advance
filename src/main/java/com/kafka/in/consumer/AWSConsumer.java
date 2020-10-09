package com.kafka.in.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AWSConsumer {

    AWSConsumer() {
        log.info(":::::AWSConsumer has been started:::::");
    }

    @KafkaListener(topics = "simpleText", containerFactory = "kafkaListenerContainerFactory")
    public void consumeText(ConsumerRecord<String, Object> message) {
        log.info(":::AWSConsumer Class ,consumeText method::::");
        log.info("::::::messageValue {}", message.value());
        log.info("::::::messageKey {}", message.key());
    }

    @KafkaListener(topics = "object", containerFactory = "kafkaListenerContainerFactory")
    public void consumesObject(ConsumerRecord<String, Object> message) {
        log.info(":::::AWSConsumer Classs, consumesObject method::::");
        log.info("::::::messageValue {}", message.value());

    }
}
