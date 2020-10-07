package com.kafka.in.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class Consumer {

    @KafkaListener(topics = {"users"}, groupId = "group_id")
    public void consumes(String msg) {
        log.info(String.format(":::::::::Consumed Message %s", msg));
    }

}
