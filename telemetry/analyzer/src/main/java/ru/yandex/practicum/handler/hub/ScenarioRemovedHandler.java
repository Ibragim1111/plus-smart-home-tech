package ru.yandex.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedAvro;
import ru.yandex.practicum.model.Scenario;
import ru.yandex.practicum.repository.ScenarioRepository;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class ScenarioRemovedHandler implements HubEventHandler {
    private final ScenarioRepository scenarioRepository;

    @Override
    public void handle(Object payload, String hubId) {
        ScenarioRemovedAvro scenarioRemovedAvro = (ScenarioRemovedAvro) payload;
        String scenarioName = scenarioRemovedAvro.getName();
        Optional<Scenario> optionalScenario = scenarioRepository.findByHubIdAndName(hubId, scenarioName);
        optionalScenario.ifPresent(scenario -> scenarioRepository.deleteById(scenario.getId()));
    }

    @Override
    public Class<? extends SpecificRecordBase> getPayloadType() {
        return ScenarioRemovedAvro.class;
    }
}
