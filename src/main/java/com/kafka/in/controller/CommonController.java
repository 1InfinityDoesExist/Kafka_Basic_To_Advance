package com.kafka.in.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.kafka.in.producer.AWSProducer;

@RestController
@RequestMapping(value = "/kafka")
public class CommonController {

    @Autowired
    private AWSProducer awsProducer;

    @GetMapping(value = "/sendString/{topic}")
    public ResponseEntity<?> sendString(
                    @PathVariable(value = "topic", required = true) String topic,
                    @RequestParam(value = "message", required = true) String message) {
        awsProducer.sendMessage(topic, message);
        return ResponseEntity.status(HttpStatus.OK)
                        .body(new ModelMap().addAttribute("msg", "Message Sent"));
    }

}
