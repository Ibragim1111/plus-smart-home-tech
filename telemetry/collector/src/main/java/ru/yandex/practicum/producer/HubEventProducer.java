package ru.yandex.practicum.producer;
import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.config.EventProducer;
import ru.yandex.practicum.grpc.telemetry.collector.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Component
@RequiredArgsConstructor
@Slf4j
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
                .setTimestamp(convertTimestamp(proto.getTimestamp()));

        switch (proto.getPayloadCase()) {
            case DEVICE_ADDED:
                DeviceAddedAvro deviceAdded = DeviceAddedAvro.newBuilder()
                        .setId(proto.getDeviceAdded().getId())
                        .setDeviceType(DeviceTypeAvro.valueOf(
                                proto.getDeviceAdded().getDeviceType().name()))
                        .build();
                builder.setPayload(deviceAdded);
                break;

            case DEVICE_REMOVED:
                DeviceRemovedAvro deviceRemoved = DeviceRemovedAvro.newBuilder()
                        .setId(proto.getDeviceRemoved().getId())
                        .build();
                builder.setPayload(deviceRemoved);
                break;

            case SCENARIO_ADDED:
                ScenarioAddedAvro scenarioAdded = ScenarioAddedAvro.newBuilder()
                        .setName(proto.getScenarioAdded().getName())
                        .build();
                builder.setPayload(scenarioAdded);
                break;

            case SCENARIO_REMOVED:
                ScenarioRemovedAvro scenarioRemoved = ScenarioRemovedAvro.newBuilder()
                        .setName(proto.getScenarioRemoved().getName())
                        .build();
                builder.setPayload(scenarioRemoved);
                break;

            default:
                throw new IllegalArgumentException("Unknown hub event type: " + proto.getPayloadCase());
        }

        return builder.build();
    }

    private long convertTimestamp(Timestamp timestamp) {
        return timestamp.getSeconds() * 1000 + timestamp.getNanos() / 1000000;
    }
}