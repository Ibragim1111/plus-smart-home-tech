package ru.yandex.practicum.telemetry.analyzer.handler.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.model.*;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedHandler implements HubEventHandler {
    private final ScenarioRepository scenarioRepository;

    @Override
    public String getEvent() {
        return ScenarioAddedEventAvro.class.getSimpleName();
    }

    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        log.debug("Start adding new scenario");
        Scenario scenario = mapToScenario(event);
        if (scenarioRepository.findByHubIdAndName(scenario.getHubId(), scenario.getName()).isEmpty()) {
            scenarioRepository.save(scenario);
        }
    }

    private Scenario mapToScenario(HubEventAvro event) {
        ScenarioAddedEventAvro addedEventAvro = (ScenarioAddedEventAvro) event.getPayload();

        Map<String, Condition> conditionsMap = new HashMap<>();
        addedEventAvro.getConditions().forEach(condition -> {
            Condition conditionEntity = Condition.builder()
                    .type(mapToConditionType(condition.getType()))
                    .operation(mapToOperationType(condition.getOperation()))
                    .value(mapToIntValue(condition.getValue()))
                    .build();
            conditionsMap.put(condition.getSensorId(), conditionEntity);
        });

        Map<String, Action> actionsMap = new HashMap<>();
        addedEventAvro.getActions().forEach(action -> {
            Action actionEntity = Action.builder()
                    .type(mapToAction(action.getType()))
                    .value(mapToIntValue(action.getValue()))
                    .build();
            actionsMap.put(action.getSensorId(), actionEntity);
        });

        return Scenario.builder()
                .hubId(event.getHubId())
                .name(addedEventAvro.getName())
                .conditions(conditionsMap)
                .actions(actionsMap)
                .build();
    }

    private ConditionType mapToConditionType(ConditionTypeAvro type) {
        return switch (type) {
            case MOTION -> ConditionType.MOTION;
            case LUMINOSITY -> ConditionType.LUMINOSITY;
            case SWITCH -> ConditionType.SWITCH;
            case TEMPERATURE -> ConditionType.TEMPERATURE;
            case CO2LEVEL -> ConditionType.CO2LEVEL;
            case HUMIDITY -> ConditionType.HUMIDITY;
        };
    }

    private OperationType mapToOperationType(ConditionOperationAvro type) {
        return switch (type) {
            case EQUALS -> OperationType.EQUALS;
            case GREATER_THAN -> OperationType.GREATER_THAN;
            case LOWER_THAN -> OperationType.LOWER_THAN;
        };
    }

    private ActionType mapToAction(ActionTypeAvro type) {
        return switch (type) {
            case ACTIVATE -> ActionType.ACTIVATE;
            case DEACTIVATE -> ActionType.DEACTIVATE;
            case INVERSE -> ActionType.INVERSE;
            case SET_VALUE -> ActionType.SET_VALUE;
        };
    }

    private int mapToIntValue(Object value) {
        return switch (value) {
            case Boolean bool -> bool ? 1 : 0;
            case Integer i -> i;
            case null, default -> 0;
        };
    }
}