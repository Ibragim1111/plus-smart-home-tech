package ru.yandex.practicum.telemetry.analyzer.handler;

import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

public interface HubEventHandler {

    public String getEvent();

    public void handle(HubEventAvro event);
}
