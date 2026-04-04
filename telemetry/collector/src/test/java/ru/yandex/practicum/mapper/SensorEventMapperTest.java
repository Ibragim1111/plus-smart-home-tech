package ru.yandex.practicum.mapper;

import org.junit.jupiter.api.Test;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.model.sensor.LightSensorEvent;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class SensorEventMapperTest {

    private final SensorEventMapper mapper = new SensorEventMapper();

    @Test
    void toAvro_ShouldMapLightSensorEventCorrectly() {
        // given
        LightSensorEvent event = new LightSensorEvent();
        event.setId("sensor.1");
        event.setHubId("hub-1");
        event.setTimestamp(Instant.now());
        event.setLinkQuality(85);
        event.setLuminosity(75);

        // when
        SensorEventAvro avro = mapper.toAvro(event);

        // then
        assertThat(avro.getId()).isEqualTo(event.getId());
        assertThat(avro.getHubId()).isEqualTo(event.getHubId());
        assertThat(avro.getPayload()).isInstanceOf(LightSensorAvro.class);

        LightSensorAvro payload = (LightSensorAvro) avro.getPayload();
        assertThat(payload.getLinkQuality()).isEqualTo(85);
        assertThat(payload.getLuminosity()).isEqualTo(75);
    }
}
