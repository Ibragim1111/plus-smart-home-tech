package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.model.sensor.SensorEvent;
import ru.yandex.practicum.producer.SensorEventProducer;

@Service
@RequiredArgsConstructor
@Slf4j
public class SensorEventService {

    private final SensorEventProducer producer;

    public void processEvent(SensorEvent event) {
        producer.send(event);
    }
}
