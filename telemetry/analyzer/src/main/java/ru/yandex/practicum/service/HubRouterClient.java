package ru.yandex.practicum.service;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.hubrouter.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;


import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubRouterClient {

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterStub;

    public void sendCommand(String hubId, String scenarioName, String sensorId,
                            String actionType, Integer value, Instant timestamp) {
        try {
            DeviceActionRequest request =
                    DeviceActionRequest.newBuilder()
                            .setHubId(hubId)
                            .setScenarioName(scenarioName)
                            .setAction(DeviceActionProto.newBuilder()
                                    .setSensorId(sensorId)
                                    .setType(ActionTypeProto.valueOf(actionType))
                                    .setValue(value != null ? value : 0)
                                    .build())
                            .setTimestamp(Timestamp.newBuilder()
                                    .setSeconds(timestamp.getEpochSecond())
                                    .setNanos(timestamp.getNano())
                                    .build())
                            .build();

            Empty response = hubRouterStub.handleDeviceAction(request);
            log.info("Команда отправлена в Hub Router: hubId={}, sensorId={}, action={}",
                    hubId, sensorId, actionType);

        } catch (Exception e) {
            log.error("Ошибка отправки команды в Hub Router", e);
        }
    }
}