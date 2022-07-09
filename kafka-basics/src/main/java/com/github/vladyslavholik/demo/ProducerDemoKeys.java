package com.github.vladyslavholik.demo;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);



        for (int i = 0; i < 10; i++) {
            String key = "id_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", key, "Hi from java" + i);
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    LOG.info("New metadata:\n Topic: " + recordMetadata.topic() +
                            ", Partition: " + recordMetadata.partition() + ", Offset: " + recordMetadata.offset() +
                            ", Timestamp: " + recordMetadata.timestamp() + "");
                } else {
                    e.printStackTrace();
                }
            }).get();
        }

        producer.flush();
        producer.close();
    }
}
