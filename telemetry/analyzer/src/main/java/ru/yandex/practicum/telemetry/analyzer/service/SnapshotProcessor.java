package ru.yandex.practicum.telemetry.analyzer.service;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import deserializer.SnapshotEventDeserializer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.client.KafkaClientConfigurationImpl;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.ActionType;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.producer.HubRouterProducer;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import telemetry.service.event.ActionTypeProto;
import telemetry.service.event.DeviceActionProto;
import telemetry.service.event.DeviceActionRequestProto;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {
    private final List<String> TOPICS = List.of("telemetry.snapshots.v1");
    private final String GROUPID = "snapshot-analyzer-group";

    private final KafkaClientConfigurationImpl<SensorsSnapshotAvro> client;
    private Consumer<String, SensorsSnapshotAvro> consumer;
    private final ScenarioRepository scenarioRepository;
    private final HubRouterProducer producer;

    @PostConstruct
    public void init() {
        this.consumer = client.initConsumer(GROUPID, SnapshotEventDeserializer.class);
    }

    public void start() {
        try {
            consumer.subscribe(TOPICS);

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records =
                        consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    log.debug("SNAPSHOT data in {}:\n {}", record.key(),
                            record.value());

                    List<Scenario> scenarios = scenarioRepository.findByHubId(record.key());
                    if (scenarios.isEmpty()) {
                        continue;
                    }

                    for (Scenario scenario : scenarios) {
                        log.debug("List scenarios:\n {}", scenario);


                        Map<String, Condition> conditions = scenario.getConditions();
                        Map<String, Action> actions = scenario.getActions();

                        Boolean allConditionsOk = record.value().getSensorsState().keySet().stream()
                                .filter(conditions::containsKey)
                                .allMatch(key -> {
                                    Condition condition = conditions.get(key);
                                    SensorStateAvro sensor = record.value().getSensorsState().get(key);
                                    return conditionType(condition, sensor);
                                });
                        log.debug("Condition check: {}", allConditionsOk);

                        if (allConditionsOk) {
                            for (String actionSensor : actions.keySet()) {

                                DeviceActionProto.Builder builder = DeviceActionProto.newBuilder()
                                        .setSensorId(actionSensor)
                                        .setType(mapToProto(actions.get(actionSensor).getType()));

                                Optional.ofNullable(actions.get(actionSensor).getValue())
                                        .ifPresent(builder::setValue);

                                DeviceActionProto action = builder.build();

                                Timestamp timestamp = Timestamps.fromMillis(System.currentTimeMillis());

                                DeviceActionRequestProto request = DeviceActionRequestProto.newBuilder()
                                        .setHubId(scenario.getHubId())
                                        .setScenarioName(scenario.getName())
                                        .setAction(action)
                                        .setTimestamp(timestamp)
                                        .build();
                                log.debug("Sending protoAction:\n {}", request);
                                producer.send(request);
                            }
                        }
                    }

                }
            }
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка во время получения или обработки снимка", e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                log.info("Закрываем консьюмер и продюсер");
                consumer.close();
            }
        }
    }

    private boolean conditionType(Condition condition, SensorStateAvro sensorAvro) {
        Integer sensorValue =
                switch (sensorAvro.getData()) {
                    case ClimateSensorAvro climateSensor -> switch (condition.getType()) {
                        case TEMPERATURE -> climateSensor.getTemperatureC();
                        case HUMIDITY -> climateSensor.getHumidity();
                        case CO2LEVEL -> climateSensor.getCo2Level();
                        default -> null;
                    };
                    case LightSensorAvro lightSensor -> lightSensor.getLuminosity();
                    case MotionSensorAvro motionSensor -> motionSensor.getMotion() ? 1 : 0;
                    case SwitchSensorAvro switchSensor -> switchSensor.getState() ? 1 : 0;
                    case TemperatureSensorAvro temperatureSensor -> switch (condition.getType()) {
                        case TEMPERATURE -> temperatureSensor.getTemperatureC();
                        default -> null;
                    };
                    default -> null;
                };
        return conditionCheck(condition, sensorValue);
    }

    private boolean conditionCheck(Condition condition, Integer sensorValue) {
        int operationValue = condition.getValue();
        switch (condition.getOperation()) {
            case LOWER_THAN -> {
                return sensorValue < operationValue;
            }
            case GREATER_THAN -> {
                return sensorValue > operationValue;
            }
            case EQUALS -> {
                return sensorValue == operationValue;
            }
            default -> {
                return false;
            }
        }
    }

    private ActionTypeProto mapToProto(ActionType type) {
        switch (type) {
            case ACTIVATE -> {
                return ActionTypeProto.ACTIVATE;
            }
            case DEACTIVATE -> {
                return ActionTypeProto.DEACTIVATE;
            }
            case SET_VALUE -> {
                return ActionTypeProto.SET_VALUE;
            }
            case INVERSE -> {
                return ActionTypeProto.INVERSE;
            }
            default -> {
                return null;
            }
        }
    }
}
