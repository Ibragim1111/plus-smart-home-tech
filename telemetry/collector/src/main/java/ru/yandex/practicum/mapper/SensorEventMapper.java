package ru.yandex.practicum.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.model.sensor.*;

import java.time.Instant;

@Component
public class SensorEventMapper {

    public SensorEventAvro toAvro(SensorEvent event) {
        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder();
        builder.setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(event.getTimestamp().toEpochMilli()));

        if (event instanceof LightSensorEvent lightEvent) {
            LightSensorAvro payload = LightSensorAvro.newBuilder()
                    .setLinkQuality(lightEvent.getLinkQuality())
                    .setLuminosity(lightEvent.getLuminosity())
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof TemperatureSensorEvent tempEvent) {
            TemperatureSensorAvro payload = TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(tempEvent.getTemperatureC())
                    .setTemperatureF(tempEvent.getTemperatureF())
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof ClimateSensorEvent climateEvent) {
            ClimateSensorAvro payload = ClimateSensorAvro.newBuilder()
                    .setTemperatureC(climateEvent.getTemperatureC())
                    .setHumidity(climateEvent.getHumidity())
                    .setCo2Level(climateEvent.getCo2Level())
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof MotionSensorEvent motionEvent) {
            MotionSensorAvro payload = MotionSensorAvro.newBuilder()
                    .setLinkQuality(motionEvent.getLinkQuality())
                    .setMotion(motionEvent.isMotion())
                    .setVoltage(motionEvent.getVoltage())
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof SwitchSensorEvent switchEvent) {
            SwitchSensorAvro payload = SwitchSensorAvro.newBuilder()
                    .setState(switchEvent.isState())
                    .build();
            builder.setPayload(payload);
        }

        return builder.build();
    }
}
