package ru.yandex.practicum.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.mapper.SensorEventMapper;
import ru.yandex.practicum.model.sensor.SensorEvent;

@Component
@RequiredArgsConstructor
@Slf4j
public class SensorEventProducer {

    private final KafkaTemplate<String, SensorEventAvro> kafkaTemplate;
    private final SensorEventMapper mapper;

    @Value("${spring.kafka.topics.sensors}")
    private String sensorsTopic;

    public void send(SensorEvent event) {
        SensorEventAvro avroEvent = mapper.toAvro(event);
        log.info("Sending sensor event to topic {}: {}", sensorsTopic, avroEvent);
        kafkaTemplate.send(sensorsTopic, event.getHubId(), avroEvent)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.debug("Sensor event sent successfully: {}", avroEvent);
                    } else {
                        log.error("Failed to send sensor event: {}", avroEvent, ex);
                    }
                });
    }
}