package ru.yandex.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedAvro;
import ru.yandex.practicum.model.Device;
import ru.yandex.practicum.repository.DeviceRepository;

import java.util.List;

@Component
@RequiredArgsConstructor
public class DeviceAddedHandler implements HubEventHandler {
    private final DeviceRepository deviceRepository;

    @Override
    public void handle(Object payload, String hubId) {
        DeviceAddedAvro deviceAddedAvro = (DeviceAddedAvro) payload;
        String sensorId = deviceAddedAvro.getId();
        if (deviceRepository.existsByIdInAndHubId(List.of(sensorId), hubId)) {
            return;
        }
        Device device = new Device();
        device.setId(sensorId);
        device.setHubId(hubId);
        deviceRepository.save(device);
    }

    @Override
    public Class<DeviceAddedAvro> getPayloadType() {
        return DeviceAddedAvro.class;
    }
}
