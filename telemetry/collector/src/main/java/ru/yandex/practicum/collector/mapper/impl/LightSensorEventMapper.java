package ru.yandex.practicum.collector.mapper.impl;

import ru.yandex.practicum.collector.dto.LightSensorEvent;
import ru.yandex.practicum.collector.dto.SensorEventType;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

public class LightSensorEventMapper implements SensorEventMapper<LightSensorEvent> {

    @Override
    public SensorEventType getSupportedType() {
        return SensorEventType.LIGHT_SENSOR_EVENT;
    }

    @Override
    public SensorEventAvro map(LightSensorEvent dto) {
        LightSensorAvro payload = LightSensorAvro.newBuilder()
                .setLinkQuality(dto.getLinkQuality())
                .setLuminosity(dto.getLuminosity())
                .build();

        return SensorEventAvro.newBuilder()
                .setId(dto.getId())
                .setHubId(dto.getHubId())
                .setTimestamp(dto.getTimestamp())
                .setPayload(payload)
                .build();
    }
}
