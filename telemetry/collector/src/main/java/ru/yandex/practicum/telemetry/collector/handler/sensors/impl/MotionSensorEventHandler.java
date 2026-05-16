package ru.yandex.practicum.telemetry.collector.handler.sensors.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.handler.sensors.SensorEventHandler;
import ru.yandex.practicum.telemetry.collector.mapper.SensorEventMapper;
import telemetry.service.event.SensorEventProto;

@Slf4j
@Component
public class MotionSensorEventHandler implements SensorEventHandler {
    private final SensorEventMapper mapper = new SensorEventMapper();

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event, Producer<String, SpecificRecordBase> producer) {
        SensorEventAvro avro = mapper.toAvro(event);
        log.debug("avro request = {}", event);
        producer.send(new ProducerRecord<>("telemetry.sensors.v1", avro.getHubId(), avro));
    }
}
