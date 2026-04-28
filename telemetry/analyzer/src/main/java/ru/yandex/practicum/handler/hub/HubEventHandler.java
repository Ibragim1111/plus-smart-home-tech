package ru.yandex.practicum.handler.hub;

import org.apache.avro.specific.SpecificRecordBase;

public interface HubEventHandler {
    void handle(Object payload, String hubId);

    Class<? extends SpecificRecordBase> getPayloadType();
}