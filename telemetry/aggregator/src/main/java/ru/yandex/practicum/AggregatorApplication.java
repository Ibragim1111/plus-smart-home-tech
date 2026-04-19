package ru.yandex.practicum;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;


import org.springframework.kafka.annotation.EnableKafka;

@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableKafka  // ← Включаем KafkaListener
public class AggregatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(AggregatorApplication.class, args);
        log.info("Aggregator сервис запущен. Ожидание сообщений...");
    }
}