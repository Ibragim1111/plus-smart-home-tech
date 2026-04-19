package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.collector.HubEventProto;
import ru.yandex.practicum.producer.HubEventProducer;

@Service
@RequiredArgsConstructor

public class HubEventService {

    private final HubEventProducer producer;

    public void processEvent(HubEventProto event) {
        producer.send(event);
    }
}
