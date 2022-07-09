package com.github.vladyslavholik.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        TopicPartition partition = new TopicPartition("first_topic", 0);
        consumer.assign(List.of(partition));

        Long offsetToRead = 15L;
        consumer.seek(partition, offsetToRead);

        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;
        boolean keepOnReading = true;

        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar += 1;
                LOG.info("Value: " + record.value() + " Key: " + record.key());
                LOG.info("Partition: " + record.partition() + " Offset: " + record.offset());

                if (numberOfMessagesReadSoFar == numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
    }
}
