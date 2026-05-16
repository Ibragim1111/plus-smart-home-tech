package ru.yandex.practicum.telemetry.aggregator.snapshot;

import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Optional;

public interface SnapshotService {
    Optional<SensorsSnapshotAvro> updateSnapshot(SensorEventAvro event);
}
