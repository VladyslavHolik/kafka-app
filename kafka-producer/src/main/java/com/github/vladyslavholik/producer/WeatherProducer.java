package com.github.vladyslavholik.producer;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class WeatherProducer {
    private final Logger LOG = LoggerFactory.getLogger(WeatherProducer.class);

    public static void main(String[] args) {
        String boostrapServers = "127.0.0.1:9092";
        String keySerializer = StringSerializer.class.getName();
        String valueSerializer = StringSerializer.class.getName();
        String latitude = "50.450001";
        String longitude = "30.523333";
        String topic = "weather_topic";

        WeatherProducer weatherProducer = new WeatherProducer();
        weatherProducer.fetchWeather(topic, boostrapServers, keySerializer, valueSerializer, latitude, longitude);
    }

    public void fetchWeather(String topic, String boostrapServers, String keySerializer, String valueSerializer, String latitude,
                             String longitude) {
        CountDownLatch latch = new CountDownLatch(1);

        WeatherProducerRunnable weatherProducerRunnable = WeatherProducerRunnable.builder()
                .boostrapServers(boostrapServers)
                .topic(topic)
                .keySerializer(keySerializer)
                .valueSerializer(valueSerializer)
                .latitude(latitude)
                .longitude(longitude)
                .latch(latch)
                .build();

        Thread weatherThread = new Thread(weatherProducerRunnable);

        LOG.info("Starting Producer Thread");
        weatherThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            weatherProducerRunnable.setShutdown(true);
            try {
                latch.await();
                LOG.info("Shutdown");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }
}
