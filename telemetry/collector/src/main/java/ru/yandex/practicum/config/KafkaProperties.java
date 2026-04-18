package ru.yandex.practicum.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaProperties {
    private String bootstrapServers = "localhost:9092";  // значение по умолчанию

    private Topics topics = new Topics();

    @Data
    public static class Topics {
        private String sensors = "telemetry.sensors.v1";
        private String hubs = "telemetry.hubs.v1";
    }
}