package com.kafka.in.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import com.kafka.in.producer.OldProducer;
import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafka
@Slf4j
public class OldConsumer {
    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;
    @Value("${spring.kafka.consumer.key-deserializer}")
    private String keyDeserializer;
    @Value("${spring.kafka.consumer.value-deserializer}")
    private String valueDeserializer;
    @Value("${spring.kafka.consumer.enable-auto-commit}")
    private String enableAutoCommit;
    @Value("${spring.kafka.consumer.auto-commit-interval}")
    private String autoCommitInterval;
    @Value("${spring.kafka.consumer.session.timeout}")
    private String sessionTimeout;

    public Properties consumerConfig() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        return properties;
    }

    public void consumeMessage(String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig());
        consumer.subscribe(Collections.singleton(topic));
        log.info(":::::::consumeMessage method :::::");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                log.info(":: key {}, value {}, partitioin {}, topic {} , offSet {}", record.key(),
                                record.value(), record.partition(), record.topic(),
                                record.offset());
            }
            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                log.error("commit failed", e);
            }
        }
    }


    @Bean
    public OldProducer oldProducer() {
        return new OldProducer();
    }
}
