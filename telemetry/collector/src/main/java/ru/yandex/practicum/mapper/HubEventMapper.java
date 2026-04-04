package ru.yandex.practicum.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.hub.*;

import java.util.stream.Collectors;

@Component
public class HubEventMapper {

    public HubEventAvro toAvro(HubEvent event) {
        HubEventAvro.Builder builder = HubEventAvro.newBuilder();
        builder.setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli());

        if (event instanceof DeviceAddedEvent deviceAdded) {
            DeviceAddedAvro payload = DeviceAddedAvro.newBuilder()
                    .setId(deviceAdded.getId())
                    .setDeviceType(DeviceTypeAvro.valueOf(deviceAdded.getDeviceType().name()))
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof DeviceRemovedEvent deviceRemoved) {
            DeviceRemovedAvro payload = DeviceRemovedAvro.newBuilder()
                    .setId(deviceRemoved.getId())
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof ScenarioAddedEvent scenarioAdded) {
            ScenarioAddedAvro payload = ScenarioAddedAvro.newBuilder()
                    .setName(scenarioAdded.getName())
                    .setConditions(scenarioAdded.getConditions().stream()
                            .map(this::mapCondition)
                            .collect(Collectors.toList()))
                    .setActions(scenarioAdded.getActions().stream()
                            .map(this::mapAction)
                            .collect(Collectors.toList()))
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof ScenarioRemovedEvent scenarioRemoved) {
            ScenarioRemovedAvro payload = ScenarioRemovedAvro.newBuilder()
                    .setName(scenarioRemoved.getName())
                    .build();
            builder.setPayload(payload);
        }

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
