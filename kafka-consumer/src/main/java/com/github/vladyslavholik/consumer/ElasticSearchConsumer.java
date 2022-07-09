package com.github.vladyslavholik.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) throws IOException {
        ElasticSearchConsumer consumer = new ElasticSearchConsumer();
        String hostname = "kafka-course-596169073.eu-central-1.bonsaisearch.net";
        Integer port = 443;
        String username = "rk1cin14ab";
        String password = "x1mb40wte7";

        RestHighLevelClient client = consumer.createClient(hostname, port, username, password);

        KafkaConsumer<String, String> kafkaConsumer = consumer.createConsumer();
        kafkaConsumer.subscribe(Collections.singleton("weather_topic"));

        BulkRequest bulkRequest = new BulkRequest();

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));

            for (ConsumerRecord<String, String> record : records) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode actualObj = mapper.readTree(record.value());
                String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                IndexRequest request = new IndexRequest("weather", "data")
                        .source(actualObj.get("main"), XContentType.JSON)
                        .id(id);

                bulkRequest.add(request);
            }

            if (records.count() != 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                kafkaConsumer.commitSync();
            }
        }
    }

    public RestHighLevelClient createClient(String hostname, Integer port, String username, String password) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        // Create the low-level client
        return new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, port, "https"))
                .setHttpClientConfigCallback((httpClientBuilder) -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)));
    }

    public KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();

        String groupId = "consumer_group_3";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        return new KafkaConsumer<>(properties);
    }
}
