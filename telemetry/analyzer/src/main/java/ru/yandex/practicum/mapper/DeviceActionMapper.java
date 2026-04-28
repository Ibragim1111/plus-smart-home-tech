package ru.yandex.practicum.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.model.DeviceAction;
import ru.yandex.practicum.model.Scenario;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class DeviceActionMapper {
    public DeviceAction toAction(DeviceActionAvro avro, Scenario scenario) {
        if (avro == null) {
            return null;
        }
        DeviceAction deviceAction = new DeviceAction();
        deviceAction.setValue(avro.getValue());
        deviceAction.setType(avro.getType());
        deviceAction.setDeviceId(avro.getSensorId());
        deviceAction.setScenario(scenario);
        return deviceAction;
    }

    public List<DeviceAction> toActions(List<DeviceActionAvro> avros, Scenario scenario) {
        if (avros == null || avros.isEmpty()) {
            return List.of();
        }
        return avros.stream()
                .map(avro -> toAction(avro, scenario))
                .collect(Collectors.toList());
    }
}
