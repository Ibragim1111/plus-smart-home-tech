package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.model.entity.Scenario;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioService scenarioService;

    @KafkaListener(
            topics = "#{@kafkaProperties.topics.snapshots}",
            containerFactory = "snapshotListenerContainerFactory"
    )
    public void processSnapshot(
            SensorsSnapshotAvro snapshot,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        log.debug("Получен снапшот из партиции {}, offset={}: hubId={}",
                partition, offset, snapshot.getHubId());

        try {
            // Загружаем сценарии для этого хаба
            List<Scenario> scenarios = scenarioRepository.findByHubId(snapshot.getHubId());

            // Проверяем каждый сценарий
            for (Scenario scenario : scenarios) {
                if (scenarioService.checkConditions(scenario, snapshot)) {
                    // Если условия выполнены - выполняем действия
                    scenarioService.executeActions(scenario, snapshot);
                }
            }

            acknowledgment.acknowledge();
            log.debug("Снапшот обработан, оффсет {} зафиксирован", offset);

        } catch (Exception e) {
            log.error("Ошибка обработки снапшота", e);
            // Не фиксируем оффсет - будет повторная обработка
        }
    }
}