/*package ru.yandex.practicum;


import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import ru.yandex.practicum.model.sensor.LightSensorEvent;
import ru.yandex.practicum.model.sensor.SensorEventType;

import java.time.Instant;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = {"telemetry.sensors.v1", "telemetry.hubs.v1"})
class CollectorApplicationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void testSensorEventEndpoint() throws Exception {
        LightSensorEvent event = new LightSensorEvent();
        event.setId("sensor.test.1");
        event.setHubId("hub-1");
        event.setTimestamp(Instant.now());
        event.setType(SensorEventType.LIGHT_SENSOR_EVENT);
        event.setLinkQuality(85);
        event.setLuminosity(75);

        String json = objectMapper.writeValueAsString(event);

        mockMvc.perform(post("/events/sensors")
                        .contentType("application/json")
                        .content(json))
                .andExpect(status().isOk());
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> "localhost:9092");
    }
}*/