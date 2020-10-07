package com.kafka.in.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.kafka.in.producer.Producer;

@RestController
@RequestMapping(value = "/kafka")
public class CommonController {

    @Autowired
    private Producer producer;

    @PostMapping(value = "/publish")
    public void sendMessageToKafkaTopic(
                    @RequestParam(value = "message", required = true) String message) {
        this.producer.sendMessage(message);
    }
}
