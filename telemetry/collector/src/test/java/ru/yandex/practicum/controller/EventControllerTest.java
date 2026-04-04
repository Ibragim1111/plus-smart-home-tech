package ru.yandex.practicum.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import ru.yandex.practicum.model.sensor.LightSensorEvent;
import ru.yandex.practicum.model.sensor.SensorEventType;
import ru.yandex.practicum.service.HubEventService;
import ru.yandex.practicum.service.SensorEventService;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class EventControllerTest {

    private MockMvc mockMvc;

    @Mock
    private SensorEventService sensorEventService;

    @Mock
    private HubEventService hubEventService;

    @InjectMocks
    private EventController eventController;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(eventController).build();
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    void collectSensorEvent_ShouldReturnOk_WhenValidLightSensorEvent() throws Exception {
        // given
        LightSensorEvent event = new LightSensorEvent();
        event.setId("sensor.test.1");
        event.setHubId("hub-1");
        event.setTimestamp(Instant.now());
        event.setLinkQuality(85);
        event.setLuminosity(75);

        String json = objectMapper.writeValueAsString(event);

        // when/then
        mockMvc.perform(post("/events/sensors")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isOk());

        verify(sensorEventService, times(1)).processEvent(any(LightSensorEvent.class));
    }

    @Test
    void collectSensorEvent_ShouldReturnBadRequest_WhenInvalidEvent() throws Exception {
        // given
        String invalidJson = "{\"id\":\"\"}"; // пустой id

        // when/then
        mockMvc.perform(post("/events/sensors")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(invalidJson))
                .andExpect(status().isBadRequest());
    }
}
