package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.collector.SensorEventProto;
import ru.yandex.practicum.producer.SensorEventProducer;

@Service
@RequiredArgsConstructor

public class SensorEventService {

    private final SensorEventProducer producer;

    public void processEvent(SensorEventProto event) {
        producer.send(event);
    }
}