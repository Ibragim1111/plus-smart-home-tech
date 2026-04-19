package ru.yandex.practicum.config.collector;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import ru.yandex.practicum.config.KafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventProducer {

    private final KafkaProducer<String, Object> producer;
    private final KafkaProperties kafkaProperties;

    public void sendSensorEvent(SensorEventAvro event, String hubId, long timestamp) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(
                kafkaProperties.getTopics().getSensors(),
                null,
                timestamp,
                hubId,
                event
        );

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send sensor event", exception);
            } else {
                log.debug("Sensor event sent to partition {} with offset {}",
                        metadata.partition(), metadata.offset());
            }
        });
    }

    public void sendHubEvent(HubEventAvro event, String hubId, long timestamp) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(
                kafkaProperties.getTopics().getHubs(),
                null,
                timestamp,
                hubId,
                event
        );

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send hub event", exception);
            } else {
                log.debug("Hub event sent to partition {} with offset {}",
                        metadata.partition(), metadata.offset());
            }
        });
    }
}