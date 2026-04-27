package ru.yandex.practicum.producer;
import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.config.collector.EventProducer;
import ru.yandex.practicum.exception.InvalidScenarioConditionValueException;
import ru.yandex.practicum.grpc.telemetry.collector.ConditionTypeProto;
import ru.yandex.practicum.grpc.telemetry.collector.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.collector.ScenarioConditionProto;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionProto;
import ru.yandex.practicum.kafka.telemetry.event.*;


import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProducer {

    private final EventProducer eventProducer;

    public void send(HubEventProto event) {
        HubEventAvro avroEvent = convertToAvro(event);
        long timestamp = convertTimestamp(event.getTimestamp());
        eventProducer.sendHubEvent(avroEvent, event.getHubId(), timestamp);
    }

    private HubEventAvro convertToAvro(HubEventProto proto) {
        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(proto.getHubId())
                .setTimestampMs(convertTimestampInstant(proto.getTimestamp()));

        switch (proto.getPayloadCase()) {
            case DEVICE_ADDED:
                log.info("=== DEVICE_ADDED DEBUG ===");
                log.info("Device ID: {}", proto.getDeviceAdded().getId());
                log.info("Device Type: {}", proto.getDeviceAdded().getType());

                DeviceAddedEventAvro deviceAdded = DeviceAddedEventAvro.newBuilder()
                        .setId(proto.getDeviceAdded().getId())
                        .setType(DeviceTypeAvro.valueOf(
                                proto.getDeviceAdded().getType().name()))
                        .build();
                builder.setPayload(deviceAdded);
                log.info("DeviceAddedAvro created: id={}, type={}",
                        deviceAdded.getId(), deviceAdded.getType());
                break;

            case DEVICE_REMOVED:
                DeviceRemovedEventAvro deviceRemoved = DeviceRemovedEventAvro.newBuilder()
                        .setId(proto.getDeviceRemoved().getId())
                        .build();
                builder.setPayload(deviceRemoved);
                break;

            case SCENARIO_ADDED:
                log.info("=== SCENARIO_ADDED DEBUG ===");
                log.info("Name: {}", proto.getScenarioAdded().getName());

                log.info("Conditions count: {}", proto.getScenarioAdded().getConditionsList().size());
                for (int i = 0; i < proto.getScenarioAdded().getConditionsList().size(); i++) {
                    var cond = proto.getScenarioAdded().getConditionsList().get(i);
                    log.info("Condition {}: sensorId={}, type={}, operation={}, value={}",
                            i, cond.getSensorId(), cond.getType(), cond.getOperation(),
                            cond.hasValue() ? cond.getValue() : "null");
                }
                // Проверяем actions
                log.info("Actions count: {}", proto.getScenarioAdded().getActionsList().size());
                for (int i = 0; i < proto.getScenarioAdded().getActionsList().size(); i++) {
                    var act = proto.getScenarioAdded().getActionsList().get(i);
                    log.info("Action {}: sensorId={}, type={}, value={}",
                            i, act.getSensorId(), act.getType(),
                            act.hasValue() ? act.getValue() : "null");
                }

                List<ScenarioConditionAvro> conditions = proto.getScenarioAdded().getConditionsList().stream()
                        .map(this::mapCondition)
                        .collect(Collectors.toList());

                List<DeviceActionAvro> actions = proto.getScenarioAdded().getActionsList().stream()
                        .map(this::mapAction)
                        .collect(Collectors.toList());

                log.info("Mapped conditions: {}, mapped actions: {}", conditions.size(), actions.size());

                ScenarioAddedEventAvro scenarioAdded = ScenarioAddedEventAvro.newBuilder()
                        .setName(proto.getScenarioAdded().getName())
                        .setConditions(conditions)
                        .setActions(actions)
                        .build();
                builder.setPayload(scenarioAdded);
                log.info("ScenarioAddedAvro created SUCCESS!");
                break;

            case SCENARIO_REMOVED:
                ScenarioRemovedEventAvro scenarioRemoved = ScenarioRemovedEventAvro.newBuilder()
                        .setName(proto.getScenarioRemoved().getName())
                        .build();
                builder.setPayload(scenarioRemoved);
                break;

            default:
                throw new IllegalArgumentException("Unknown hub event type: " + proto.getPayloadCase());
        }

        return builder.build();
    }

    private ScenarioConditionAvro mapCondition(ScenarioConditionProto condition) {
        // Логируем полученное значение для отладки
        log.debug("Mapping condition: sensorId={}, type={}, operation={}, valueCase={}",
                condition.getSensorId(),
                condition.getType(),
                condition.getOperation(),
                condition.getValue());

        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()));


        if (condition.hasValue()) {
            builder.setValue(mapConditionValue(condition));
        }else {
            builder.setValue(null); // ЯВНО устанавливаем null
        }

        return builder.build();
    }


    private DeviceActionAvro mapAction(DeviceActionProto action) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()));

        if (action.hasValue()) {
            builder.setValue(action.getValue());
        }

        return builder.build();
    }
    private long convertTimestamp(Timestamp timestamp) {
        return timestamp.getSeconds() * 1000 + timestamp.getNanos() / 1000000;
    }
    private Instant convertTimestampInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(
                timestamp.getSeconds(),
                timestamp.getNanos()
        );
    }
    private Object mapConditionValue(ScenarioConditionProto condition) {


        if (condition.getType() == ConditionTypeProto.MOTION|| condition.getType() == ConditionTypeProto.SWITCH) {
            // Hub Router sends scenario values as integers in JSON, but the Avro contract for
            // boolean-like conditions is boolean. We translate 0/1 explicitly to preserve intent.
            if (condition.getValue() != 0 && condition.getValue() != 1) {
                throw new InvalidScenarioConditionValueException("Boolean-like condition value must be 0 or 1");
            }
            return condition.getValue() == 1;
        }
        return condition.getValue();
    }
}