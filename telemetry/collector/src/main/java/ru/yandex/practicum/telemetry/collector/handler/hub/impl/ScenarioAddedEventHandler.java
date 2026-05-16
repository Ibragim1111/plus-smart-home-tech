package ru.yandex.practicum.telemetry.collector.handler.hub.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.handler.hub.HubEventHandler;
import ru.yandex.practicum.telemetry.collector.mapper.HubEventMapper;
import telemetry.service.event.HubEventProto;

@Slf4j
@Component
public class ScenarioAddedEventHandler implements HubEventHandler {
    private final HubEventMapper mapper = new HubEventMapper();

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    public void handle(HubEventProto event, Producer<String, SpecificRecordBase> producer) {
        HubEventAvro avro = mapper.toAvro(event);
        log.debug("avro request = {}", avro);
        producer.send(new ProducerRecord<>("telemetry.hubs.v1", avro.getHubId(), avro));
    }
}
