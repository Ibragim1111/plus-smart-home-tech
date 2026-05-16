package ru.yandex.practicum.telemetry.aggregator.snapshot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
public class SnapshotServiceImpl implements SnapshotService {
    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateSnapshot(SensorEventAvro event) {
        log.debug("Processing event: hubId={}, sensorId={}, timestamp={}",
                event.getHubId(), event.getId(), event.getTimestamp());

        String hubId = event.getHubId();
        SensorsSnapshotAvro existingSnapshot = snapshots.get(hubId);

        if (existingSnapshot == null) {
            return createNewSnapshot(event);
        }
        return updateExistingSnapshot(existingSnapshot, event);
    }

    private Optional<SensorsSnapshotAvro> createNewSnapshot(SensorEventAvro event) {
        Map<String, SensorStateAvro> sensorStates = new HashMap<>();
        sensorStates.put(event.getId(), toStateAvro(event));

        SensorsSnapshotAvro snapshot = SensorsSnapshotAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setSensorsState(sensorStates)
                .build();

        snapshots.put(event.getHubId(), snapshot);
        log.info("Created new snapshot for hub: {} with sensor: {}",
                event.getHubId(), event.getId());

        return Optional.of(snapshot);
    }

    private Optional<SensorsSnapshotAvro> updateExistingSnapshot(SensorsSnapshotAvro snapshot, SensorEventAvro event) {
        String sensorId = event.getId();
        Map<String, SensorStateAvro> currentStates = snapshot.getSensorsState();
        SensorStateAvro currentState = currentStates.get(sensorId);

        if (shouldSkipUpdate(currentState, event)) {
            return Optional.empty();
        }

        Map<String, SensorStateAvro> updatedStates = new HashMap<>(currentStates);
        updatedStates.put(sensorId, toStateAvro(event));

        Instant maxTimestamp = findMaxTimestamp(updatedStates, event.getTimestamp());

        SensorsSnapshotAvro newSnapshot = SensorsSnapshotAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(maxTimestamp)
                .setSensorsState(updatedStates)
                .build();

        snapshots.put(event.getHubId(), newSnapshot);
        log.debug("Updated snapshot for hub: {}, sensors count: {}",
                event.getHubId(), updatedStates.size());

        return Optional.of(newSnapshot);
    }

    private boolean shouldSkipUpdate(SensorStateAvro currentState, SensorEventAvro event) {
        if (currentState == null) {
            log.debug("New sensor {} for hub {}, will add to snapshot",
                    event.getId(), event.getHubId());
            return false;
        }

        Instant eventTimestamp = event.getTimestamp();
        Instant currentTimestamp = currentState.getTimestamp();

        if (eventTimestamp.isBefore(currentTimestamp)) {
            log.debug("Skipping outdated event: sensor={}, eventTimestamp={}, currentTimestamp={}",
                    event.getId(), eventTimestamp, currentTimestamp);
            return true;
        }

        if (eventTimestamp.equals(currentTimestamp) &&
                currentState.getData().equals(event.getPayload())) {
            log.debug("Skipping duplicate event: sensor={}, timestamp={}",
                    event.getId(), eventTimestamp);
            return true;
        }

        log.debug("Will update sensor: {}, reason: {}", event.getId(),
                eventTimestamp.isAfter(currentTimestamp) ? "newer timestamp" : "data changed");
        return false;
    }

    private Instant findMaxTimestamp(Map<String, SensorStateAvro> states, Instant defaultValue) {
        return states.values().stream()
                .map(SensorStateAvro::getTimestamp)
                .max(Instant::compareTo)
                .orElse(defaultValue);
    }

    private SensorStateAvro toStateAvro(SensorEventAvro event) {
        return SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();
    }
}