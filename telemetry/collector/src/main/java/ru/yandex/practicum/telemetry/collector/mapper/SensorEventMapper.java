package ru.yandex.practicum.telemetry.collector.mapper;

import ru.yandex.practicum.kafka.telemetry.event.*;
import telemetry.service.event.*;

import java.time.Instant;

public class SensorEventMapper {

    public SensorEventAvro toAvro(SensorEventProto proto) {
        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder()
                .setId(proto.getId())
                .setHubId(proto.getHubId())
                .setTimestamp(Instant.ofEpochSecond(
                        proto.getTimestamp().getSeconds(),
                        proto.getTimestamp().getNanos()
                ));

        switch (proto.getPayloadCase()) {
            case CLIMATE_SENSOR:
                ClimateSensorProto climateSensor = proto.getClimateSensor();
                ClimateSensorAvro climatePayload = ClimateSensorAvro.newBuilder()
                        .setTemperatureC(climateSensor.getTemperatureC())
                        .setHumidity(climateSensor.getHumidity())
                        .setCo2Level(climateSensor.getCo2Level())
                        .build();
                builder.setPayload(climatePayload);
                break;

            case LIGHT_SENSOR:
                LightSensorProto lightSensor = proto.getLightSensor();
                LightSensorAvro lightPayload = LightSensorAvro.newBuilder()
                        .setLinkQuality(lightSensor.getLinkQuality())
                        .setLuminosity(lightSensor.getLuminosity())
                        .build();
                builder.setPayload(lightPayload);
                break;

            case MOTION_SENSOR:
                MotionSensorProto motionSensor = proto.getMotionSensor();
                MotionSensorAvro motionPayload = MotionSensorAvro.newBuilder()
                        .setLinkQuality(motionSensor.getLinkQuality())
                        .setMotion(motionSensor.getMotion())
                        .setVoltage(motionSensor.getVoltage())
                        .build();
                builder.setPayload(motionPayload);
                break;

            case SWITCH_SENSOR:
                SwitchSensorProto switchSensor = proto.getSwitchSensor();
                SwitchSensorAvro switchPayload = SwitchSensorAvro.newBuilder()
                        .setState(switchSensor.getState())
                        .build();
                builder.setPayload(switchPayload);
                break;

            case TEMPERATURE_SENSOR:
                TemperatureSensorProto tempSensor = proto.getTemperatureSensor();
                TemperatureSensorAvro tempPayload = TemperatureSensorAvro.newBuilder()
                        .setTemperatureC(tempSensor.getTemperatureC())
                        .setTemperatureF(tempSensor.getTemperatureF())
                        .build();
                builder.setPayload(tempPayload);
                break;

            case PAYLOAD_NOT_SET:
            default:
                break;
        }

        return builder.build();
    }
}