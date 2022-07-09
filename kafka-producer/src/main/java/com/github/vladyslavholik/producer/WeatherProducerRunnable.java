package com.github.vladyslavholik.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.client.RestTemplate;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Builder
@Data
@AllArgsConstructor
public class WeatherProducerRunnable implements Runnable {
    private boolean shutdown;
    private final String boostrapServers;
    private final String keySerializer;
    private final String valueSerializer;
    private final String latitude;
    private final String longitude;
    private final String topic;
    private final CountDownLatch latch;

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

        // safe producer (idempotent)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // high throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        try {
            while (!shutdown) {
                WeatherService service = WeatherService.builder()
                        .APIKey("1f0baa768f962c90a6346c6372678dce")
                        .APIUrl("https://api.openweathermap.org/data/2.5/weather?lat=%s&lon=%s&appid=%s")
                        .objectMapper(new ObjectMapper())
                        .restTemplate(new RestTemplate())
                        .build();

                JsonNode weather = service.getCurrentWeather(latitude, longitude);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, weather.toString());
                producer.send(record).get();
                producer.flush();

                Thread.sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.close();
        latch.countDown();
    }
}
