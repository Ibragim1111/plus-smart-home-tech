package ru.yandex.practicum.configuration;

import jakarta.annotation.PreDestroy;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.GeneralAvroSerializer;

import java.util.Properties;

@Configuration
public class KafkaConfiguration {
    @Value("${kafka.bootstrap.server}")
    private String bootstrapServer;

    private KafkaProducer<String, SpecificRecordBase> producer;

    @Bean
    KafkaProducer<String, SpecificRecordBase> kafkaProducer() {
        if (producer == null) {
            Properties config = new Properties();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GeneralAvroSerializer.class.getName());
            producer = new KafkaProducer<>(config);
        }
        return producer;
    }

    @PreDestroy
    public void closeProducer() {
        if (producer != null) {
            producer.close();
        }
    }
}
