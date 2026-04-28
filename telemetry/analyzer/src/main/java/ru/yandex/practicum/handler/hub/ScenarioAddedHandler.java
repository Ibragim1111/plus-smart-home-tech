package ru.yandex.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedAvro;
import ru.yandex.practicum.mapper.DeviceActionMapper;
import ru.yandex.practicum.mapper.ScenarioConditionMapper;
import ru.yandex.practicum.model.DeviceAction;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.model.ScenarioCondition;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.util.List;
import java.util.Optional;

@Component
@RequiredArgsConstructor
@Slf4j
public class ScenarioAddedHandler implements HubEventHandler {
    private final ScenarioRepository scenarioRepository;
    private final DeviceActionMapper deviceActionMapper;
    private final ScenarioConditionMapper scenarioConditionMapper;

    @Override
    public void handle(Object payload, String hubId) {
        ScenarioAddedAvro scenarioAddedAvro = (ScenarioAddedAvro) payload;

        String scenarioName = scenarioAddedAvro.getName();
        Optional<Scenario> optionalScenario = scenarioRepository.findByHubIdAndName(hubId, scenarioName);

        Scenario scenario = new Scenario();
        optionalScenario.ifPresent(value -> scenario.setId(value.getId()));
        scenario.setHubId(hubId);
        scenario.setName(scenarioName);

        List<DeviceAction> deviceActions = deviceActionMapper.toActions(scenarioAddedAvro.getActions(), scenario);
        scenario.setActions(deviceActions);
        List<ScenarioCondition> conditions = scenarioConditionMapper.toConditions(scenarioAddedAvro.getConditions(), scenario);
        scenario.setConditions(conditions);

        scenarioRepository.save(scenario);
    }

    @Override
    public Class<? extends SpecificRecordBase> getPayloadType() {
        return ScenarioAddedAvro.class;
    }
}
