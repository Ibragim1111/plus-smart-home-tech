package ru.yandex.practicum.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class ValidationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void shouldRejectEventWithoutId() throws Exception {
        String json = """
                {
                    "hubId": "hub-1",
                    "type": "LIGHT_SENSOR_EVENT",
                    "luminosity": 75
                }
                """;

        mockMvc.perform(post("/events/sensors")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldRejectEventWithoutHubId() throws Exception {
        String json = """
                {
                    "id": "sensor.1",
                    "type": "LIGHT_SENSOR_EVENT",
                    "luminosity": 75
                }
                """;

        mockMvc.perform(post("/events/sensors")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldRejectScenarioWithoutName() throws Exception {
        String json = """
                {
                    "hubId": "hub-1",
                    "type": "SCENARIO_ADDED",
                    "conditions": [],
                    "actions": []
                }
                """;

        mockMvc.perform(post("/events/hubs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldRejectDeviceAddedWithoutId() throws Exception {
        String json = """
            {
                "hubId": "hub-1",
                "type": "DEVICE_ADDED",
                "deviceType": "LIGHT_SENSOR"
            }
            """;

        mockMvc.perform(post("/events/hubs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldRejectScenarioAddedWithoutName() throws Exception {
        String json = """
            {
                "hubId": "hub-1",
                "type": "SCENARIO_ADDED",
                "conditions": [],
                "actions": []
            }
            """;

        mockMvc.perform(post("/events/hubs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldRejectScenarioAddedWithShortName() throws Exception {
        String json = """
            {
                "hubId": "hub-1",
                "type": "SCENARIO_ADDED",
                "name": "ab",
                "conditions": [],
                "actions": []
            }
            """;

        mockMvc.perform(post("/events/hubs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldRejectScenarioRemovedWithoutName() throws Exception {
        String json = """
            {
                "hubId": "hub-1",
                "type": "SCENARIO_REMOVED"
            }
            """;

        mockMvc.perform(post("/events/hubs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isBadRequest());
    }
}