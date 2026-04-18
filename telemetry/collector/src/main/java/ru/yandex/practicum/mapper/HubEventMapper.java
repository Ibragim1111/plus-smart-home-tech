package ru.yandex.practicum.mapper;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.hub.*;

import java.util.stream.Collectors;

@Slf4j
@Component
public class HubEventMapper {

    public HubEventAvro toAvro(HubEvent event) {
        log.info("=== MAPPER: Начало маппинга ===");
        log.info("MAPPER: Получен event типа: {}", event.getType());
        log.info("MAPPER: Класс event: {}", event.getClass().getSimpleName());

        HubEventAvro.Builder builder = HubEventAvro.newBuilder();
        builder.setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli());

        switch (event.getType()) {
            case DEVICE_ADDED:
                log.info("MAPPER: Обработка DEVICE_ADDED");
                DeviceAddedEvent deviceAdded = (DeviceAddedEvent) event;
                log.info("MAPPER: Device ID = {}", deviceAdded.getId());
                log.info("MAPPER: Device Type = {}", deviceAdded.getDeviceType());

                DeviceAddedAvro deviceAddedPayload = DeviceAddedAvro.newBuilder()
                        .setId(deviceAdded.getId())
                        .setDeviceType(DeviceTypeAvro.valueOf(deviceAdded.getDeviceType().name()))
                        .build();
                builder.setPayload(deviceAddedPayload);
                log.info("MAPPER: Создан DeviceAddedAvro с id={}", deviceAddedPayload.getId());
                break;

            case DEVICE_REMOVED:
                log.info("MAPPER: Обработка DEVICE_REMOVED");
                DeviceRemovedEvent deviceRemoved = (DeviceRemovedEvent) event;
                DeviceRemovedAvro deviceRemovedPayload = DeviceRemovedAvro.newBuilder()
                        .setId(deviceRemoved.getId())
                        .build();
                builder.setPayload(deviceRemovedPayload);
                break;

            case SCENARIO_ADDED:
                log.info("MAPPER: Обработка SCENARIO_ADDED");
                ScenarioAddedEvent scenarioAdded = (ScenarioAddedEvent) event;
                ScenarioAddedAvro scenarioAddedPayload = ScenarioAddedAvro.newBuilder()
                        .setName(scenarioAdded.getName())
                        .setConditions(scenarioAdded.getConditions().stream()
                                .map(this::mapCondition)
                                .collect(Collectors.toList()))
                        .setActions(scenarioAdded.getActions().stream()
                                .map(this::mapAction)
                                .collect(Collectors.toList()))
                        .build();
                builder.setPayload(scenarioAddedPayload);
                break;

            case SCENARIO_REMOVED:
                log.info("MAPPER: Обработка SCENARIO_REMOVED");
                ScenarioRemovedEvent scenarioRemoved = (ScenarioRemovedEvent) event;
                ScenarioRemovedAvro scenarioRemovedPayload = ScenarioRemovedAvro.newBuilder()
                        .setName(scenarioRemoved.getName())
                        .build();
                builder.setPayload(scenarioRemovedPayload);
                break;

            default:
                log.error("MAPPER: НЕИЗВЕСТНЫЙ ТИП: {}", event.getType());
                throw new IllegalArgumentException("Unknown event type: " + event.getType());
        }

        log.info("MAPPER: Итоговый payload класс: {}", builder.getPayload().getClass().getSimpleName());
        log.info("=== MAPPER: Конец маппинга ===");

        return builder.build();
    }

    private ScenarioConditionAvro mapCondition(ScenarioCondition condition) {
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                .setValue(condition.getValue())
                .build();
    }

    private DeviceActionAvro mapAction(DeviceAction action) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()));

        if (action.getValue() != null) {
            builder.setValue(action.getValue());
        }

        return builder.build();
    }
}