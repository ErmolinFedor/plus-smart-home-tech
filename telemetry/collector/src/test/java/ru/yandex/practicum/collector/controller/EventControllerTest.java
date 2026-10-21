package ru.yandex.practicum.collector.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import ru.yandex.practicum.collector.service.EventMapperService;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(EventController.class)
class EventControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @MockBean
    private EventMapperService mapperService;

    @Test
    @DisplayName("Успешная обработка LIGHT_SENSOR_EVENT и отправка в Kafka")
    void shouldCollectLightSensorEventSuccessfully() throws Exception {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("id", "sensor.light.3");
        requestBody.put("hubId", "hub-1");
        requestBody.put("timestamp", "2026-10-18T12:00:00Z");
        requestBody.put("linkQuality", 80);
        requestBody.put("luminosity", 450);
        requestBody.put("type", "LIGHT_SENSOR_EVENT");

        byte[] mockAvroBytes = new byte[]{1, 2, 3};

        Mockito.when(mapperService.mapAndSerializeSensor(any())).thenReturn(mockAvroBytes);

        mockMvc.perform(post("/events/sensors")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(requestBody)))
                .andExpect(status().isOk());

        Mockito.verify(kafkaTemplate).send(eq("telemetry.sensors.v1"), eq("hub-1"), eq(mockAvroBytes));
    }

    @Test
    @DisplayName("Ошибка 400 Bad Request при отсутствии обязательного поля hubId")
    void shouldReturnBadRequestWhenHubIdIsMissing() throws Exception {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("id", "sensor.light.3");
        requestBody.put("linkQuality", 80);
        requestBody.put("luminosity", 450);
        requestBody.put("type", "LIGHT_SENSOR_EVENT");

        mockMvc.perform(post("/events/sensors")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(requestBody)))
                .andExpect(status().isBadRequest());

        Mockito.verify(kafkaTemplate, Mockito.never()).send(any(), any(), any());
    }

    @Test
    @DisplayName("Успешная обработка DEVICE_ADDED события хаба")
    void shouldCollectDeviceAddedHubEventSuccessfully() throws Exception {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("hubId", "hub-2");
        requestBody.put("timestamp", "2026-10-18T13:00:00Z");
        requestBody.put("id", "new-sensor-id-123");
        requestBody.put("deviceType", "MOTION_SENSOR");
        requestBody.put("type", "DEVICE_ADDED");

        byte[] mockAvroBytes = new byte[]{4, 5, 6};
        Mockito.when(mapperService.mapAndSerializeHub(any())).thenReturn(mockAvroBytes);

        mockMvc.perform(post("/events/hubs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(requestBody)))
                .andExpect(status().isOk());

        Mockito.verify(kafkaTemplate).send(eq("telemetry.hubs.v1"), eq("hub-2"), eq(mockAvroBytes));
    }
}
