package ru.yandex.practicum.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class AggregationService {

    private final Map<String, SensorsSnapshotAvro> snapshots = new ConcurrentHashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();
        String sensorId = event.getId();

        log.debug("Обработка события: hubId={}, sensorId={}, timestamp={}", hubId, sensorId, event.getTimestampMs());

        // 1. Получаем или создаем снапшот для хаба
        SensorsSnapshotAvro snapshot = snapshots.get(hubId);
        if (snapshot == null) {
            log.info("Создаем новый снапшот для хаба: {}", hubId);
            snapshot = SensorsSnapshotAvro.newBuilder()
                    .setHubId(hubId)
                    .setTimestampMs(event.getTimestampMs())
                    .setSensorsState(new HashMap<>())
                    .build();
        }

        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();
        SensorStateAvro oldState = sensorsState.get(sensorId);

        // 2. Если данные не изменились - ничего не делаем
        if (oldState != null && oldState.getTimestampMs().isAfter(event.getTimestampMs())) {
            log.debug("Данные датчика {} не изменились или устарели", sensorId);
            return Optional.empty();
        }


        // 3. Создаем новое состояние датчика
        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestampMs(event.getTimestampMs())
                .setData(event.getPayload())
                .build();

        // 4. Проверяем, изменились ли сами данные
        if (oldState != null && oldState.getData().equals(event.getPayload())) {
            log.debug("Данные датчика {} не изменились (но timestamp новее)", sensorId);
            sensorsState.put(sensorId, newState);
            snapshot.setTimestampMs(event.getTimestampMs());
            snapshots.put(hubId, snapshot);
            return Optional.of(snapshot);
        }

        // 5. Обновляем снапшот
        log.info("Обновляем датчик {} в хабе {}", sensorId, hubId);
        sensorsState.put(sensorId, newState);
        snapshot.setTimestampMs(event.getTimestampMs());
        snapshots.put(hubId, snapshot);

        return Optional.of(snapshot);
    }
}