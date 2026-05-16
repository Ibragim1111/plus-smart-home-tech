package ru.yandex.practicum.telemetry.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import telemetry.service.event.*;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class HubEventMapper {

    public HubEventAvro toAvro(HubEventProto proto) {
        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(proto.getHubId())
                .setTimestamp(Instant.ofEpochSecond(
                        proto.getTimestamp().getSeconds(),
                        proto.getTimestamp().getNanos()
                ));

        switch (proto.getPayloadCase()) {
            case DEVICE_ADDED:
                DeviceAddedEventProto deviceAdded = proto.getDeviceAdded();
                DeviceAddedEventAvro deviceAddedPayload = DeviceAddedEventAvro.newBuilder()
                        .setId(deviceAdded.getId())
                        .setType(mapDeviceType(deviceAdded.getType()))
                        .build();
                builder.setPayload(deviceAddedPayload);
                break;

            case DEVICE_REMOVED:
                DeviceRemovedEventProto deviceRemoved = proto.getDeviceRemoved();
                DeviceRemovedEventAvro deviceRemovedPayload = DeviceRemovedEventAvro.newBuilder()
                        .setId(deviceRemoved.getId())
                        .build();
                builder.setPayload(deviceRemovedPayload);
                break;

            case SCENARIO_ADDED:
                ScenarioAddedEventProto scenarioAdded = proto.getScenarioAdded();
                ScenarioAddedEventAvro scenarioAddedPayload = ScenarioAddedEventAvro.newBuilder()
                        .setName(scenarioAdded.getName())
                        .setConditions(mapConditions(scenarioAdded.getConditionList()))
                        .setActions(mapActions(scenarioAdded.getActionList()))
                        .build();
                builder.setPayload(scenarioAddedPayload);
                break;

            case SCENARIO_REMOVED:
                ScenarioRemovedEventProto scenarioRemoved = proto.getScenarioRemoved();
                ScenarioRemovedEventAvro scenarioRemovedPayload = ScenarioRemovedEventAvro.newBuilder()
                        .setName(scenarioRemoved.getName())
                        .build();
                builder.setPayload(scenarioRemovedPayload);
                break;

            case PAYLOAD_NOT_SET:
            default:
                break;
        }

        return builder.build();
    }

    private DeviceTypeAvro mapDeviceType(DeviceTypeProto type) {
        if (type == null) {
            return null;
        }

        return switch (type) {
            case MOTION_SENSOR -> DeviceTypeAvro.MOTION_SENSOR;
            case TEMPERATURE_SENSOR -> DeviceTypeAvro.TEMPERATURE_SENSOR;
            case LIGHT_SENSOR -> DeviceTypeAvro.LIGHT_SENSOR;
            case CLIMATE_SENSOR -> DeviceTypeAvro.CLIMATE_SENSOR;
            case SWITCH_SENSOR -> DeviceTypeAvro.SWITCH_SENSOR;
            case UNRECOGNIZED -> throw new IllegalArgumentException("Unrecognized device type: " + type);
        };
    }

    private List<ScenarioConditionAvro> mapConditions(List<ScenarioConditionProto> conditions) {
        if (conditions == null) {
            return null;
        }

        return conditions.stream()
                .map(condition -> {
                    ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                            .setSensorId(condition.getSensorId())
                            .setOperation(mapOperation(condition.getOperation()))
                            .setType(mapConditionType(condition.getType()));
                    switch (condition.getValueCase()) {
                        case BOOL_VALUE:
                            builder.setValue(condition.getBoolValue());
                            break;
                        case INT_VALUE:
                            builder.setValue(condition.getIntValue());
                            break;
                        case VALUE_NOT_SET:
                        default:
                            builder.setValue(null);
                            break;
                    }

                    return builder.build();
                })
                .collect(Collectors.toList());
    }

    private List<DeviceActionAvro> mapActions(List<DeviceActionProto> actions) {
        if (actions == null) {
            return null;
        }

        return actions.stream()
                .map(action -> DeviceActionAvro.newBuilder()
                        .setSensorId(action.getSensorId())
                        .setType(mapActionType(action.getType()))
                        .setValue(action.getValue())
                        .build())
                .collect(Collectors.toList());
    }

    private ConditionOperationAvro mapOperation(ConditionOperationProto operation) {
        if (operation == null) {
            return null;
        }

        return switch (operation) {
            case EQUALS -> ConditionOperationAvro.EQUALS;
            case GREATER_THAN -> ConditionOperationAvro.GREATER_THAN;
            case LOWER_THAN -> ConditionOperationAvro.LOWER_THAN;
            case UNRECOGNIZED -> throw new IllegalArgumentException("Unrecognized operation type: " + operation);
        };
    }

    private ConditionTypeAvro mapConditionType(ConditionTypeProto condition) {
        if (condition == null) {
            return null;
        }

        return switch (condition) {
            case MOTION -> ConditionTypeAvro.MOTION;
            case LUMINOSITY -> ConditionTypeAvro.LUMINOSITY;
            case SWITCH -> ConditionTypeAvro.SWITCH;
            case TEMPERATURE -> ConditionTypeAvro.TEMPERATURE;
            case CO2LEVEL -> ConditionTypeAvro.CO2LEVEL;
            case HUMIDITY -> ConditionTypeAvro.HUMIDITY;
            case UNRECOGNIZED -> throw new IllegalArgumentException("Unrecognized condition type: " + condition);
        };
    }

    private ActionTypeAvro mapActionType(ActionTypeProto action) {
        if (action == null) {
            return null;
        }

        return switch (action) {
            case ACTIVATE -> ActionTypeAvro.ACTIVATE;
            case DEACTIVATE -> ActionTypeAvro.DEACTIVATE;
            case INVERSE -> ActionTypeAvro.INVERSE;
            case SET_VALUE -> ActionTypeAvro.SET_VALUE;
            case UNRECOGNIZED -> throw new IllegalArgumentException("Unrecognized action type: " + action);
        };
    }
}