package ru.yandex.practicum.telemetry.analyzer.producer;

import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import telemetry.service.event.DeviceActionRequestProto;

@Component
public class HubRouterProducer {

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub controller;

    public void send(DeviceActionRequestProto request) {
        controller.handleDeviceAction(request);
    }
}
