package com.kafka.in.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import com.kafka.in.consumer.OldConsumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OldProducer {
    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String bootstarpServers;
    @Value("${spring.kafka.producer.key-serializer}")
    private String keySerializer;
    @Value("${spring.kafka.producer.value-serializer}")
    private String valueSerialzier;
    @Value("${spring.kafka.producer.acks}")
    private String acks;
    @Value("${spring.kafka.producer.retries}")
    private String retries;
    @Value("${spring.kafka.producer.batch-size}")
    private String batchSize;
    @Value("${spring.kafka.producer.buffer-memory}")
    private String bufferMemory;


    public Properties producerConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstarpServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerialzier);
        properties.put(ProducerConfig.ACKS_CONFIG, acks);
        properties.put(ProducerConfig.RETRIES_CONFIG, retries);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        return properties;
    }

    public void sendMessage() {
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerConfig());
        ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>("bulk", "This is just a bulk message");
        kafkaProducer.send(record);
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    public void sendMulipleMessageWithCallBack() {
        KafkaProducer<String, String> kafkaProducer =
                        new KafkaProducer<String, String>(producerConfig());
        ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>("sulk", "Key_1 Ka Value");
        kafkaProducer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    log.info(" offSet {}, topic {}, partition {}", metadata.offset(),
                                    metadata.topic(), metadata.partition());
                } else {
                    log.info("Printing the exception {}", exception.getMessage());
                }
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();
    }


    @Bean
    public OldConsumer oldConsumer() {
        return new OldConsumer();
    }

}
