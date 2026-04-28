package kafka.deserializer;

import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

public class HubEventsDeserializer extends BaseAvroDeserializer<HubEventAvro> {
    public HubEventsDeserializer() {
        super(HubEventAvro.getClassSchema());
    }
}