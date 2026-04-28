package ru.yandex.practicum.handler.hub;

import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedAvro;
import ru.yandex.practicum.model.Device;
import ru.yandex.practicum.repository.DeviceRepository;

import java.util.List;

@Component
@RequiredArgsConstructor
public class DeviceRemovedHandler implements HubEventHandler {
    private final DeviceRepository deviceRepository;

    @Override
    public void handle(Object payload, String hubId) {
        DeviceRemovedAvro deviceRemovedAvro = (DeviceRemovedAvro) payload;
        if (!deviceRepository.existsByIdInAndHubId(List.of(deviceRemovedAvro.getId()), hubId)) {
            return;
        }
        Device device = deviceRepository.findByIdAndHubId(deviceRemovedAvro.getId(), hubId).get();
        deviceRepository.deleteById(device.getId());
    }

    @Override
    public Class<? extends SpecificRecordBase> getPayloadType() {
        return DeviceRemovedAvro.class;
    }
}
