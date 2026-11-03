package ru.yandex.practicum.collector.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.dto.HubEvent;
import ru.yandex.practicum.collector.dto.HubEventType;
import ru.yandex.practicum.collector.dto.SensorEvent;
import ru.yandex.practicum.collector.dto.SensorEventType;
import ru.yandex.practicum.collector.exception.MappingStrategyNotFoundException;
import ru.yandex.practicum.collector.mapper.HubEventMapper;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.collector.mapper.impl.*;
import ru.yandex.practicum.collector.serialization.AvroSerializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@SuppressWarnings({"rawtypes", "unchecked"})
public class EventMapperService {

    private final AvroSerializer serializer;
    private final Map<SensorEventType, SensorEventMapper> sensorMappers;
    private final Map<HubEventType, HubEventMapper> hubMappers;

    public EventMapperService(AvroSerializer serializer) {
        this.serializer = serializer;

        List<SensorEventMapper> sensorMapperList = List.of(
                new LightSensorEventMapper(),
                new TemperatureSensorEventMapper(),
                new ClimateSensorEventMapper(),
                new MotionSensorEventMapper(),
                new SwitchSensorEventMapper()
        );

        List<HubEventMapper> hubMapperList = List.of(
                new DeviceAddedHubEventMapper(),
                new DeviceRemovedHubEventMapper(),
                new ScenarioAddedHubEventMapper(),
                new ScenarioRemovedHubEventMapper()
        );

        this.sensorMappers = sensorMapperList.stream()
                .collect(Collectors.toMap(SensorEventMapper::getSupportedType, Function.identity()));

        this.hubMappers = hubMapperList.stream()
                .collect(Collectors.toMap(HubEventMapper::getSupportedType, Function.identity()));
    }

    public byte[] mapAndSerializeSensor(SensorEvent dto) {
        SensorEventMapper mapper = sensorMappers.get(dto.getType());
        if (mapper == null) {
            throw MappingStrategyNotFoundException.forSensor(dto.getType());
        }
        SensorEventAvro avroRecord = mapper.map(dto);
        return serializer.serialize(avroRecord);
    }

    public byte[] mapAndSerializeHub(HubEvent dto) {
        HubEventMapper mapper = hubMappers.get(dto.getType());
        if (mapper == null) {
            throw MappingStrategyNotFoundException.forHub(dto.getType());
        }
        HubEventAvro avroRecord = mapper.map(dto);
        return serializer.serialize(avroRecord);
    }
}
