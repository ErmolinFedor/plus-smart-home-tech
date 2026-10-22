package ru.yandex.practicum.collector.service;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.yandex.practicum.collector.dto.*;
import ru.yandex.practicum.collector.serialization.AvroSerializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class EventMapperServiceTest {

    @Autowired
    private EventMapperService mapperService;


    @BeforeEach
    void setUp() {
        AvroSerializer serializer = new AvroSerializer();
        mapperService = new EventMapperService(serializer);
    }

    @Test
    @DisplayName("Успешный маппинг и бинарная сериализация датчика освещенности")
    void shouldMapAndSerializeLightSensorEvent() throws IOException {
        LightSensorEvent dto = new LightSensorEvent();
        dto.setId("light-1");
        dto.setHubId("hub-1");
        dto.setTimestamp(Instant.parse("2026-10-19T12:00:00Z"));
        dto.setLinkQuality(90);
        dto.setLuminosity(500);
        dto.setType(SensorEventType.LIGHT_SENSOR_EVENT);

        byte[] bytes = mapperService.mapAndSerializeSensor(dto);

        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        SensorEventAvro deserialized = deserializeAvro(bytes, SensorEventAvro.class);

        assertEquals("light-1", deserialized.getId());
        assertEquals("hub-1", deserialized.getHubId());
        assertEquals(dto.getTimestamp(), deserialized.getTimestamp());

        assertInstanceOf(LightSensorAvro.class, deserialized.getPayload());
        LightSensorAvro payload = (LightSensorAvro) deserialized.getPayload();
        assertEquals(90, payload.getLinkQuality());
        assertEquals(500, payload.getLuminosity());
    }

    @Test
    @DisplayName("Успешный маппинг гибкого поля value (Union) в сценарии")
    void shouldMapScenarioWithFlexibleUnionValue() throws IOException {
        ScenarioCondition condition = new ScenarioCondition();
        condition.setSensorId("temp-sensor");
        condition.setType(ConditionType.TEMPERATURE);
        condition.setOperation(ConditionOperation.GREATER_THAN);
        condition.setValue(25);

        ScenarioAddedEvent dto = new ScenarioAddedEvent();
        dto.setHubId("hub-1");
        dto.setName("Hot Weather");
        dto.setConditions(List.of(condition));
        dto.setActions(List.of());
        dto.setType(HubEventType.SCENARIO_ADDED);

        byte[] bytes = mapperService.mapAndSerializeHub(dto);

        HubEventAvro hubEventContainer = deserializeAvro(bytes, HubEventAvro.class);

        assertEquals("hub-1", hubEventContainer.getHubId());
        assertNotNull(hubEventContainer.getTimestamp());

        assertInstanceOf(ScenarioAddedEventAvro.class, hubEventContainer.getPayload());
        ScenarioAddedEventAvro deserializedScenario = (ScenarioAddedEventAvro) hubEventContainer.getPayload();

        assertEquals("Hot Weather", deserializedScenario.getName());
        var avroCondition = deserializedScenario.getConditions().getFirst();

        assertEquals("temp-sensor", avroCondition.getSensorId());
        assertEquals(25, avroCondition.getValue());
    }

    private <T> T deserializeAvro(byte[] bytes, Class<T> clazz) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        SpecificDatumReader<T> reader = new SpecificDatumReader<>(clazz);
        return reader.read(null, decoder);
    }
}
