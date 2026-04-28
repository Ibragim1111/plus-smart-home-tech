package ru.yandex.practicum.service.handler.hub;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Component
public class ScenarioAddedEventHandler extends BaseHubEventHandler<ScenarioAddedAvro> {
    public ScenarioAddedEventHandler(KafkaProducer<String, SpecificRecordBase> producer) {
        super(producer);
    }

    @Override
    protected ScenarioAddedAvro mapToAvro(HubEventProto event) {
        ScenarioAddedEventProto scenarioAddedEvent = event.getScenarioAdded();
        return ScenarioAddedAvro.newBuilder()
                .setName(scenarioAddedEvent.getName())
                .setConditions(scenarioAddedEvent.getConditionList().stream()
                        .map(this::mapToConditionAvro)
                        .toList())
                .setActions(scenarioAddedEvent.getActionList().stream()
                        .map(this::mapToActionAvro)
                        .toList())
                .build();
    }

    private ScenarioConditionAvro mapToConditionAvro(ScenarioConditionProto scenarioCondition) {
        ConditionTypeAvro conditionTypeAvro = switch (scenarioCondition.getType()) {
            case MOTION -> ConditionTypeAvro.MOTION;
            case LUMINOSITY -> ConditionTypeAvro.LUMINOSITY;
            case SWITCH -> ConditionTypeAvro.SWITCH;
            case TEMPERATURE -> ConditionTypeAvro.TEMPERATURE;
            case CO2LEVEL -> ConditionTypeAvro.CO2LEVEL;
            case HUMIDITY -> ConditionTypeAvro.HUMIDITY;
            default -> throw new IllegalArgumentException(
                    "Неизвестный тип условия: " + scenarioCondition.getType());
        };
        ConditionOperationAvro conditionOperationAvro = switch (scenarioCondition.getOperation()) {
            case EQUALS -> ConditionOperationAvro.EQUALS;
            case LOWER_THAN -> ConditionOperationAvro.LOWER_THAN;
            case GREATER_THAN -> ConditionOperationAvro.GREATER_THAN;
            default -> throw new IllegalArgumentException(
                    "Неизвестный тип операции: " + scenarioCondition.getOperation());
        };
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(scenarioCondition.getSensorId())
                .setType(conditionTypeAvro)
                .setOperation(conditionOperationAvro)
                .build();
    }

    private DeviceActionAvro mapToActionAvro(DeviceActionProto deviceAction) {
        ActionTypeAvro actionTypeAvro = switch (deviceAction.getType()) {
            case INVERSE -> ActionTypeAvro.INVERSE;
            case ACTIVATE -> ActionTypeAvro.ACTIVATE;
            case DEACTIVATE -> ActionTypeAvro.DEACTIVATE;
            case SET_VALUE -> ActionTypeAvro.SET_VALUE;
            default -> throw new IllegalArgumentException(
                    "Неизвестное действие: " + deviceAction.getType());
        };
        return DeviceActionAvro.newBuilder()
                .setSensorId(deviceAction.getSensorId())
                .setType(actionTypeAvro)
                .setValue(deviceAction.getValue())
                .build();
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }
}
