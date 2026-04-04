package ru.yandex.practicum.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.mapper.SensorEventMapper;
import ru.yandex.practicum.model.sensor.LightSensorEvent;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SensorEventProducerTest {

    @Mock
    private KafkaTemplate<String, SensorEventAvro> kafkaTemplate;

    @Mock
    private SensorEventMapper mapper;

    @InjectMocks
    private SensorEventProducer producer;

    @BeforeEach
    void setUp() {
        // Устанавливаем значение поля sensorsTopic через рефлексию
        ReflectionTestUtils.setField(producer, "sensorsTopic", "telemetry.sensors.v1");
    }

    @Test
    void send_ShouldSendToKafka() {
        // given
        LightSensorEvent event = new LightSensorEvent();
        event.setId("sensor.1");
        event.setHubId("hub-1");

        SensorEventAvro avroEvent = mock(SensorEventAvro.class);
        when(mapper.toAvro(event)).thenReturn(avroEvent);

        // Мокаем успешный ответ
        CompletableFuture<SendResult<String, SensorEventAvro>> future = CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(eq("telemetry.sensors.v1"), eq("hub-1"), eq(avroEvent)))
                .thenReturn(future);

        // when
        producer.send(event);

        // then
        verify(kafkaTemplate, times(1))
                .send(eq("telemetry.sensors.v1"), eq("hub-1"), eq(avroEvent));
        verify(mapper, times(1)).toAvro(event);
    }

    @Test
    void send_ShouldLogError_WhenKafkaFails() {
        // given
        LightSensorEvent event = new LightSensorEvent();
        event.setId("sensor.1");
        event.setHubId("hub-1");

        SensorEventAvro avroEvent = mock(SensorEventAvro.class);
        when(mapper.toAvro(event)).thenReturn(avroEvent);

        // Мокаем ошибку
        CompletableFuture<SendResult<String, SensorEventAvro>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka error"));
        when(kafkaTemplate.send(eq("telemetry.sensors.v1"), eq("hub-1"), eq(avroEvent)))
                .thenReturn(failedFuture);

        // when - не должно выбросить исключение
        producer.send(event);

        // then
        verify(kafkaTemplate, times(1))
                .send(eq("telemetry.sensors.v1"), eq("hub-1"), eq(avroEvent));
    }
}