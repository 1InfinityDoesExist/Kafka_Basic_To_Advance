package com.kafka.in.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class Producer {

    private static final String TOPICS = "users";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String msg) {
        log.info("::::::msg {}", msg);
        this.kafkaTemplate.send(TOPICS, msg);
    }
}
