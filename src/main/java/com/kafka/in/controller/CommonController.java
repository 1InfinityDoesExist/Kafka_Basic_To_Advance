package com.kafka.in.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.kafka.in.consumer.OldConsumer;
import com.kafka.in.model.User;
import com.kafka.in.producer.AWSProducer;
import com.kafka.in.producer.OldProducer;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping(value = "/kafka")
@Slf4j
public class CommonController {

    @Autowired
    private AWSProducer awsProducer;

    @Autowired(required = true)
    private OldConsumer oldConsumer;

    @Autowired(required = true)
    private OldProducer oldProducer;

    @GetMapping(value = "/sendString/{topic}")
    public ResponseEntity<?> sendString(
                    @PathVariable(value = "topic", required = true) String topic,
                    @RequestParam(value = "message", required = true) String message) {
        awsProducer.sendMessage(topic, message);
        return ResponseEntity.status(HttpStatus.OK)
                        .body(new ModelMap().addAttribute("msg", "Message Sent"));
    }


    @PostMapping(value = "/object/{topic}")
    public ResponseEntity<?> sendObject(
                    @PathVariable(value = "topic", required = true) String topic,
                    @RequestBody User user) {
        awsProducer.sendMessage(topic, user.toString());
        return ResponseEntity.status(HttpStatus.OK)
                        .body(new ModelMap().addAttribute("msg", "Message Sent"));
    }

    @GetMapping(path = "/get/{id}")
    public ResponseEntity<?> testingOldConsumer(
                    @PathVariable(value = "id", required = true) String id) {
        switch (id) {
            case "1":
                oldProducer.sendMessage();
                oldConsumer.consumeMessage("bulk");
                break;

            case "3":
                oldProducer.sendMulipleMessageWithCallBack();
                oldConsumer.consumeMessage("sulk");
                break;
            default:
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                        .body(new ModelMap().addAttribute("msg", "Successfully Executed"));
    }
}
