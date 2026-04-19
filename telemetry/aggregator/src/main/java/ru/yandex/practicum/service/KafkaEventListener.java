package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.config.KafkaProperties;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventListener {

    private final AggregationService aggregationService;
    private final KafkaTemplate<String, SensorsSnapshotAvro> kafkaTemplate;
    private final KafkaProperties kafkaProperties;

    @KafkaListener(
            topics = "#{@kafkaProperties.topics.sensors}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(
            SensorEventAvro event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        log.debug("Получено событие из партиции {}, offset={}: hubId={}, sensorId={}",
                partition, offset, event.getHubId(), event.getId());

        try {
            // Обновляем снапшот
            Optional<SensorsSnapshotAvro> updatedSnapshot = aggregationService.updateState(event);

            // Если снапшот изменился - отправляем
            if (updatedSnapshot.isPresent()) {
                SensorsSnapshotAvro snapshot = updatedSnapshot.get();
                String topic = kafkaProperties.getTopics().getSnapshots();

                kafkaTemplate.send(topic, snapshot.getHubId(), snapshot)
                        .whenComplete((result, ex) -> {
                            if (ex == null) {
                                log.info("Снапшот для хаба {} отправлен в партицию {}, offset={}",
                                        snapshot.getHubId(),
                                        result.getRecordMetadata().partition(),
                                        result.getRecordMetadata().offset());
                            } else {
                                log.error("Ошибка отправки снапшота", ex);
                            }
                        });
            }

            // Фиксируем оффсет (сообщение обработано успешно)
            acknowledgment.acknowledge();
            log.debug("Оффсет {} зафиксирован", offset);

        } catch (Exception e) {
            log.error("Ошибка обработки события", e);
            // Не фиксируем оффсет - сообщение будет обработано снова
        }
    }
}