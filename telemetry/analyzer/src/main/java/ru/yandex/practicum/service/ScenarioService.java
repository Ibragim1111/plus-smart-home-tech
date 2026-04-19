package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.entity.Action;
import ru.yandex.practicum.model.entity.Condition;
import ru.yandex.practicum.model.entity.Scenario;
import ru.yandex.practicum.repository.ConditionRepository;
import ru.yandex.practicum.repository.ActionRepository;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.time.Instant;
import java.util.Optional;


@Slf4j
@Service
@RequiredArgsConstructor
public class ScenarioService {

    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final HubRouterClient hubRouterClient;


    public void saveScenario(String hubId, ScenarioAddedEventAvro scenarioAdded) {
        Optional<Scenario> existing = scenarioRepository
                .findByHubIdAndName(hubId, scenarioAdded.getName());
        if (existing.isPresent()) {
            log.warn("Scenario already exists for hub {} with name {}",
                    hubId, scenarioAdded.getName());
            // или выбросить исключение
        }
        // Создаем сценарий
        Scenario scenario = Scenario.builder()
                .hubId(hubId)
                .name(scenarioAdded.getName())
                .createdAt(System.currentTimeMillis())
                .build();

        // Сразу добавляем условия и действия в список
        for (ScenarioConditionAvro condition : scenarioAdded.getConditions()) {
            Condition cond = Condition.builder()
                    .sensorId(condition.getSensorId())
                    .type(condition.getType().name())
                    .operation(condition.getOperation().name())
                    .value(extractIntValue(condition.getValue()))
                    .scenario(scenario)  // ← связываем
                    .build();
            scenario.getConditions().add(cond);
        }

        for (DeviceActionAvro action : scenarioAdded.getActions()) {
            Action act = Action.builder()
                    .sensorId(action.getSensorId())
                    .type(action.getType().name())
                    .value(action.getValue())
                    .scenario(scenario)  // ← связываем
                    .build();
            scenario.getActions().add(act);
        }

        // ОДИН save - каскадно сохранит всё!
        scenarioRepository.save(scenario);
    }

    public boolean checkConditions(Scenario scenario, SensorsSnapshotAvro snapshot) {
        // Получаем состояние датчиков из снапшота
        var sensorsState = snapshot.getSensorsState();

        // Проверяем все условия сценария
        for (Condition condition : scenario.getConditions()) {
            SensorStateAvro sensorState = sensorsState.get(condition.getSensorId());
            if (sensorState == null) {
                log.debug("Датчик {} не найден в снапшоте", condition.getSensorId());
                return false;
            }

            if (!checkCondition(condition, sensorState)) {
                log.debug("Условие {} не выполнено", condition.getId());
                return false;
            }
        }

        log.info("Все условия сценария {} выполнены", scenario.getName());
        return true;
    }

    private boolean checkCondition(Condition condition, SensorStateAvro sensorState) {
        Object data = sensorState.getData();
        int actualValue = extractValue(data);
        int expectedValue = condition.getValue();

        return switch (condition.getOperation()) {
            case "EQUALS" -> actualValue == expectedValue;
            case "GREATER_THAN" -> actualValue > expectedValue;
            case "LOWER_THAN" -> actualValue < expectedValue;
            default -> false;
        };
    }

    private int extractValue(Object data) {
        if (data instanceof TemperatureSensorAvro temp) {
            return temp.getTemperatureC();
        } else if (data instanceof LightSensorAvro light) {
            return light.getLuminosity();
        } else if (data instanceof MotionSensorAvro motion) {
            return motion.getMotion() ? 1 : 0;
        } else if (data instanceof SwitchSensorAvro switchSensor) {
            return switchSensor.getState() ? 1 : 0;
        } else if (data instanceof ClimateSensorAvro climate) {
            return climate.getTemperatureC();
        }
        return 0;
    }

    public void executeActions(Scenario scenario, SensorsSnapshotAvro snapshot) {
        for (Action action : scenario.getActions()) {
            log.info("Выполняем действие: устройство={}, тип={}, значение={}",
                    action.getSensorId(), action.getType(), action.getValue());

            // Отправляем команду через gRPC клиент
            hubRouterClient.sendCommand(
                    snapshot.getHubId(),
                    scenario.getName(),
                    action.getSensorId(),
                    action.getType(),
                    action.getValue(),
                    Instant.now()
            );
        }
    }
    private Integer extractIntValue(Object value) {
        if (value == null) {
            log.warn("Value is null for condition, using default 0");
            return 0;  // ← ИЗМЕНЕНИЕ: возвращаем 0 вместо null
        }

        switch (value) {
            case Integer integer -> {
                return integer;
            }
            case Boolean b -> {
                return b ? 1 : 0;
            }
            default -> {
                log.warn("Неизвестный тип значения: {}, используем 0", value.getClass());
                return 0;  // ← ИЗМЕНЕНИЕ: возвращаем 0 вместо null
            }
        }
    }
}