package ru.yandex.practicum.handler.snapshot;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.DeviceAction;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.ScenarioCondition;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.time.Instant;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class SnapshotHandler {
    private final ScenarioRepository scenarioRepository;
    private final GrpcActionSender grpcActionSender;

    public void handle(SensorsSnapshotAvro snapshot) {
        List<Scenario> hubScenarios = scenarioRepository.findByHubId(snapshot.getHubId());
        hubScenarios.forEach(scenario -> processScenario(snapshot, scenario));
    }

    private void processScenario(SensorsSnapshotAvro snapshot, Scenario scenario) {
        scenario.getConditions().forEach(condition -> {
            SensorStateAvro sensorState = snapshot.getSensorsState().get(condition.getDeviceId());

            if (sensorState != null && isConditionDone(sensorState, condition)) {
                executeActionsForScenario(scenario);
            }
        });
    }

    private void executeActionsForScenario(Scenario scenario) {
        List<DeviceAction> actions = scenario.getActions();
        for (DeviceAction action : actions) {
            sendActionToGrpc(action, scenario, action.getDeviceId());
        }
    }

    private void sendActionToGrpc(DeviceAction action, Scenario scenario, String sensorId) {
        Instant ts = Instant.now();

        grpcActionSender.sendData(DeviceActionRequest.newBuilder()
                .setHubId(scenario.getHubId())
                .setScenarioName(scenario.getName())
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(ts.getEpochSecond())
                        .setNanos(ts.getNano())
                        .build())
                .setAction(DeviceActionProto.newBuilder()
                        .setType(ActionTypeProto.valueOf(action.getType().toString()))
                        .setSensorId(sensorId)
                        .setValue(action.getValue())
                        .build())
                .build());
    }

    private boolean isConditionDone(SensorStateAvro sensorStateAvro, ScenarioCondition condition) {
        Object sensorValue = extractSensorValue(sensorStateAvro, condition.getType());
        ConditionOperationAvro operation = condition.getOperation();
        Integer conditionValue = condition.getValue();

        return sensorValue instanceof Integer intValue
                ? evaluateIntegerCondition(intValue, conditionValue, operation)
                : sensorValue instanceof Boolean boolValue && evaluateBooleanCondition(boolValue, conditionValue, operation);
    }

    private Object extractSensorValue(SensorStateAvro sensorStateAvro, ConditionTypeAvro type) {
        return switch (type) {
            case CO2LEVEL -> ((ClimateSensorAvro) sensorStateAvro.getData()).getCo2Level();
            case HUMIDITY -> ((ClimateSensorAvro) sensorStateAvro.getData()).getHumidity();
            case MOTION -> ((MotionSensorAvro) sensorStateAvro.getData()).getMotion();
            case LUMINOSITY -> ((LightSensorAvro) sensorStateAvro.getData()).getLuminosity();
            case SWITCH -> ((SwitchSensorAvro) sensorStateAvro.getData()).getState();
            case TEMPERATURE -> ((TemperatureSensorAvro) sensorStateAvro.getData()).getTemperatureC();
        };
    }

    private boolean evaluateIntegerCondition(int sensorValue, int conditionValue, ConditionOperationAvro operation) {
        return switch (operation) {
            case GREATER_THAN -> sensorValue > conditionValue;
            case LOWER_THAN -> sensorValue < conditionValue;
            case EQUALS -> sensorValue == conditionValue;
        };
    }

    private boolean evaluateBooleanCondition(boolean sensorValue, int conditionValue, ConditionOperationAvro operation) {
        int boolAsInt = sensorValue ? 1 : 0;
        return switch (operation) {
            case GREATER_THAN -> boolAsInt > conditionValue;
            case LOWER_THAN -> boolAsInt < conditionValue;
            case EQUALS -> boolAsInt == conditionValue;
        };
    }
}
