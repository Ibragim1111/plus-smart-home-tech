package ru.yandex.practicum.telemetry.aggregator.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.aggregator.client.ClientConfiguration;
import ru.yandex.practicum.telemetry.aggregator.snapshot.SnapshotService;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {
    private final ClientConfiguration client;
    private final SnapshotService snapshotService;

    public void start() {
        try {
            client.getConsumer().subscribe(List.of("telemetry.sensors.v1"));

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records =
                        client.getConsumer().poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, SensorEventAvro> record : records) {

                    Optional<SensorsSnapshotAvro> optionalSensorsSnapshotAvro =
                            snapshotService.updateSnapshot(record.value());
                    optionalSensorsSnapshotAvro.ifPresent(sensorsSnapshotAvro ->
                            client.getProducer().send(new ProducerRecord<>("telemetry.snapshots.v1",
                                    sensorsSnapshotAvro.getHubId(), sensorsSnapshotAvro)));
                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                client.getProducer().flush();
                client.getConsumer().commitSync();
            } finally {
                log.info("Закрываем консьюмер и продюсер");
                client.stop();
            }
        }
    }
}
