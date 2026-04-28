package ru.yandex.practicum.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.ScenarioCondition;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class ScenarioConditionMapper {
    public ScenarioCondition toCondition(ScenarioConditionAvro avro, Scenario scenario) {
        if (avro == null) {
            return null;
        }
        ScenarioCondition scenarioCondition = new ScenarioCondition();
        scenarioCondition.setScenario(scenario);
        scenarioCondition.setType(avro.getType());
        scenarioCondition.setOperation(avro.getOperation());
        scenarioCondition.setValue(avro.getValue());
        scenarioCondition.setDeviceId(avro.getSensorId());
        return scenarioCondition;
    }

    public List<ScenarioCondition> toConditions(List<ScenarioConditionAvro> avros, Scenario scenario) {
        if (avros == null || avros.isEmpty()) {
            return List.of();
        }
        return avros.stream()
                .map(avro -> toCondition(avro, scenario))
                .collect(Collectors.toList());
    }
}
