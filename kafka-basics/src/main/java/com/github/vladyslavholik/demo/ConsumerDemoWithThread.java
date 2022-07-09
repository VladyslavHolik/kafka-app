package com.github.vladyslavholik.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        String groupId = "consumer_group_3";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        CountDownLatch latch = new CountDownLatch(1);
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(latch, properties, "first_topic");
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        latch.await();
    }

    public static class ConsumerRunnable implements Runnable {
        Logger LOG = LoggerFactory.getLogger(ConsumerRunnable.class);
        CountDownLatch countDownLatch;
        KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(CountDownLatch countDownLatch, Properties properties, String topic) {
            this.countDownLatch = countDownLatch;
            this.consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));

                    for (ConsumerRecord<String, String> record : records) {
                        LOG.info("Value: " + record.value() + " Key: " + record.key());
                        LOG.info("Partition: " + record.partition() + " Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                LOG.info("Wake up");
            } finally {
                consumer.close();
                countDownLatch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
