package ru.yandex.practicum.handler.snapshot;

import com.google.protobuf.Empty;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;

@Slf4j
@Component
public class GrpcActionSender {
    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public GrpcActionSender(@GrpcClient("hub-router")
                            HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient) {
        this.hubRouterClient = hubRouterClient;
    }

    public void sendData(DeviceActionRequest deviceActionRequest) {
        Empty response = hubRouterClient.handleDeviceAction(deviceActionRequest);
        if (response.isInitialized()) {
            log.info("Получили ответ от хаба");
        } else {
            log.info("Нет ответа от хаба");
        }
    }
}