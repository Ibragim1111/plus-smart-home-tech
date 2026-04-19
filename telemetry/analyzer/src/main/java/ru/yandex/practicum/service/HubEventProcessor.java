package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;

import ru.yandex.practicum.model.entity.Sensor;
import ru.yandex.practicum.repository.ScenarioRepository;
import ru.yandex.practicum.repository.SensorRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ScenarioService scenarioService;

    @KafkaListener(
            topics = "#{@kafkaProperties.topics.hubs}",
            containerFactory = "hubEventListenerContainerFactory"
    )
    public void processHubEvent(
            HubEventAvro event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        log.debug("Получено событие хаба из партиции {}, offset={}", partition, offset);

        try {
            Object payload = event.getPayload();

            if (payload instanceof DeviceAddedEventAvro deviceAdded) {
                // Сохраняем новый датчик
                Sensor sensor = Sensor.builder()
                        .id(deviceAdded.getId())
                        .hubId(event.getHubId())
                        .deviceType(deviceAdded.getType().name())
                        .registeredAt(System.currentTimeMillis())
                        .build();
                sensorRepository.save(sensor);
                log.info("Добавлен датчик: {}", deviceAdded.getId());

            } else if (payload instanceof DeviceRemovedEventAvro deviceRemoved) {
                // Удаляем датчик
                sensorRepository.deleteById(deviceRemoved.getId());
                log.info("Удален датчик: {}", deviceRemoved.getId());

            } else if (payload instanceof ScenarioAddedEventAvro scenarioAdded) {
                // Сохраняем сценарий
                log.debug("Mapping condition: value={}",
                        scenarioAdded.getConditions().getFirst().getValue());

                scenarioService.saveScenario(event.getHubId(), scenarioAdded);
                log.info("Добавлен сценарий: {}", scenarioAdded.getConditions());

            } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemoved) {
                // Удаляем сценарий
                scenarioRepository.findByHubIdAndName(event.getHubId(), scenarioRemoved.getName())
                        .ifPresent(scenarioRepository::delete);
                log.info("Удален сценарий: {}", scenarioRemoved.getName());
            }

            acknowledgment.acknowledge();
            log.debug("Событие хаба обработано, оффсет {} зафиксирован", offset);

        } catch (Exception e) {
            log.error("Ошибка обработки события хаба", e);
            // Не фиксируем оффсет - будет повторная обработка
        }
    }
}