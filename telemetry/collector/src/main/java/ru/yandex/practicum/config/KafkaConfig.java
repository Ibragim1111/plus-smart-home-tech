package ru.yandex.practicum.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.serialization.GeneralAvroSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    // ⭐ Создаем сериализатор для SensorEventAvro с указанием типа
    @Bean
    public GeneralAvroSerializer<SensorEventAvro> sensorAvroSerializer() {
        return new GeneralAvroSerializer<>();  // ← Алмазный оператор с типом
    }

    // ⭐ Создаем сериализатор для HubEventAvro с указанием типа
    @Bean
    public GeneralAvroSerializer<HubEventAvro> hubAvroSerializer() {
        return new GeneralAvroSerializer<>();  // ← Алмазный оператор с типом
    }

    // ⭐ ProducerFactory для датчиков
    @Bean
    public ProducerFactory<String, SensorEventAvro> sensorEventProducerFactory(
            GeneralAvroSerializer<SensorEventAvro> sensorAvroSerializer) {  // ← Указан тип!

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(
                config,
                new StringSerializer(),
                sensorAvroSerializer  // ← Тип совпадает
        );
    }

    @Bean
    public KafkaTemplate<String, SensorEventAvro> sensorEventKafkaTemplate(
            ProducerFactory<String, SensorEventAvro> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    // ⭐ ProducerFactory для хаба
    @Bean
    public ProducerFactory<String, HubEventAvro> hubEventProducerFactory(
            GeneralAvroSerializer<HubEventAvro> hubAvroSerializer) {  // ← Указан тип!

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(
                config,
                new StringSerializer(),
                hubAvroSerializer  // ← Тип совпадает
        );
    }

    @Bean
    public KafkaTemplate<String, HubEventAvro> hubEventKafkaTemplate(
            ProducerFactory<String, HubEventAvro> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}