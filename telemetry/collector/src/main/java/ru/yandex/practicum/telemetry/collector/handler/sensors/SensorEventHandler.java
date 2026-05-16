package ru.yandex.practicum.telemetry.collector.handler.sensors;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import telemetry.service.event.SensorEventProto;

public interface SensorEventHandler {
    SensorEventProto.PayloadCase getMessageType();

    void handle(SensorEventProto event, Producer<String, SpecificRecordBase> producer);
}
