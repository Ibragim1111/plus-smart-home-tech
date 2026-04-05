package ru.yandex.practicum.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.config.EventProducer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.mapper.HubEventMapper;
import ru.yandex.practicum.model.hub.HubEvent;

@Component
@RequiredArgsConstructor
@Slf4j
public class HubEventProducer {

    private final HubEventMapper mapper;
    private final EventProducer eventProducer;

    public void send(HubEvent event) {
        HubEventAvro avroEvent = mapper.toAvro(event);

        long timestamp = event.getTimestamp().toEpochMilli();

        // Отправляем
        eventProducer.sendHubEvent(avroEvent, event.getHubId(), timestamp);


    }
}
