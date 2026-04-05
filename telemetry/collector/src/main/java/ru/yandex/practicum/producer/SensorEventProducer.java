package ru.yandex.practicum.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.config.EventProducer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.mapper.HubEventMapper;
import ru.yandex.practicum.mapper.SensorEventMapper;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.model.sensor.SensorEvent;

@Component
@RequiredArgsConstructor
@Slf4j
public class SensorEventProducer {

    private final SensorEventMapper mapper;
    private final EventProducer eventProducer;

    public void send(SensorEvent event) {
        SensorEventAvro avroEvent = mapper.toAvro(event);

        long timestamp = event.getTimestamp().toEpochMilli();

        // Отправляем
        eventProducer.sendSensorEvent(avroEvent, event.getHubId(), timestamp);


    }
}