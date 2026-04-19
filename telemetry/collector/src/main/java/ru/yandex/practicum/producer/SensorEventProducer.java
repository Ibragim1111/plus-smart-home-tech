package ru.yandex.practicum.producer;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.config.collector.EventProducer;
import ru.yandex.practicum.grpc.telemetry.collector.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;

@Component
@RequiredArgsConstructor
@Slf4j
public class SensorEventProducer {

    private final EventProducer eventProducer;

    public void send(SensorEventProto event) {
        // Конвертируем Protobuf в Avro
        SensorEventAvro avroEvent = convertToAvro(event);

        long timestamp = convertTimestamp(event.getTimestamp());

        eventProducer.sendSensorEvent(avroEvent, event.getHubId(), timestamp);
    }

    private SensorEventAvro convertToAvro(SensorEventProto proto) {
        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder()
                .setId(proto.getId())
                .setHubId(proto.getHubId())
                .setTimestamp(Instant.ofEpochSecond(
                        proto.getTimestamp().getSeconds(),
                        proto.getTimestamp().getNanos()
                ));
        switch (proto.getPayloadCase()) {
            case LIGHT_SENSOR:
                LightSensorAvro light = LightSensorAvro.newBuilder()
                        .setLinkQuality(proto.getLightSensor().getLinkQuality())
                        .setLuminosity(proto.getLightSensor().getLuminosity())
                        .build();
                builder.setPayload(light);
                break;

            case TEMPERATURE_SENSOR:
                TemperatureSensorAvro temp = TemperatureSensorAvro.newBuilder()
                        .setTemperatureC(proto.getTemperatureSensor().getTemperatureC())
                        .setTemperatureF(proto.getTemperatureSensor().getTemperatureF())
                        .build();
                builder.setPayload(temp);
                break;

            case CLIMATE_SENSOR:
                ClimateSensorAvro climate = ClimateSensorAvro.newBuilder()
                        .setTemperatureC(proto.getClimateSensor().getTemperatureC())
                        .setHumidity(proto.getClimateSensor().getHumidity())
                        .setCo2Level(proto.getClimateSensor().getCo2Level())
                        .build();
                builder.setPayload(climate);
                break;

            case MOTION_SENSOR:
                MotionSensorAvro motion = MotionSensorAvro.newBuilder()
                        .setLinkQuality(proto.getMotionSensor().getLinkQuality())
                        .setMotion(proto.getMotionSensor().getMotion())
                        .setVoltage(proto.getMotionSensor().getVoltage())
                        .build();
                builder.setPayload(motion);
                break;

            case SWITCH_SENSOR:
                SwitchSensorAvro switchSensor = SwitchSensorAvro.newBuilder()
                        .setState(proto.getSwitchSensor().getState())
                        .build();
                builder.setPayload(switchSensor);
                break;

            default:
                throw new IllegalArgumentException("Unknown sensor type: " + proto.getPayloadCase());
        }

        return builder.build();
    }

    private long convertTimestamp(Timestamp timestamp) {
        return timestamp.getSeconds() * 1000 + timestamp.getNanos() / 1000000;
    }
}