package com.github.vladyslavholik.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.springframework.web.client.RestTemplate;

@AllArgsConstructor
@Builder
public class WeatherService {
    private final String APIKey;
    private final String APIUrl;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public JsonNode getCurrentWeather(String latitude, String longitude) throws JsonProcessingException {
        String currentWeather = restTemplate.getForObject(String.format(APIUrl, latitude, longitude, APIKey), String.class);
        return objectMapper.readTree(currentWeather);
    }
}
