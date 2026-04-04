package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.model.hub.HubEvent;
import ru.yandex.practicum.producer.HubEventProducer;

@Service
@RequiredArgsConstructor
@Slf4j
public class HubEventService {

    private final HubEventProducer producer;

    public void processEvent(HubEvent event) {
        producer.send(event);
    }
}
