package ru.yandex.practicum.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.collector.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.collector.SensorEventProto;
import ru.yandex.practicum.service.HubEventService;
import ru.yandex.practicum.service.SensorEventService;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class EventController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final SensorEventService sensorEventService;
    private final HubEventService hubEventService;

    @Override
    public void collectSensorEvent(SensorEventProto request,
                                   io.grpc.stub.StreamObserver<Empty> responseObserver) {
        try {
            log.info("Received sensor event via gRPC: id={}, hubId={}, type={}",
                    request.getId(), request.getHubId(), request.getPayloadCase());

            // Конвертируем Protobuf в нашу внутреннюю модель (если нужно)
            // или сразу отправляем в Kafka через продюсер

            sensorEventService.processEvent(request);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error processing sensor event", e);
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL.withDescription(e.getMessage()).withCause(e)));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request,
                                io.grpc.stub.StreamObserver<Empty> responseObserver) {
        try {
            log.info("Received hub event via gRPC: hubId={}, type={}",
                    request.getHubId(), request.getPayloadCase());

            hubEventService.processEvent(request);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error processing hub event", e);
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL.withDescription(e.getMessage()).withCause(e)));
        }
    }
}