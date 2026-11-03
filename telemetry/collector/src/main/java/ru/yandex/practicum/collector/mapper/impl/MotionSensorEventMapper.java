package ru.yandex.practicum.collector.mapper.impl;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.dto.MotionSensorEvent;
import ru.yandex.practicum.collector.dto.SensorEvent;
import ru.yandex.practicum.collector.dto.SensorEventType;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Component
public class MotionSensorEventMapper implements SensorEventMapper<MotionSensorEvent> {
    @Override
    public SensorEventType getSupportedType() {
        return SensorEventType.MOTION_SENSOR_EVENT;
    }

    @Override
    public SensorEventAvro map(MotionSensorEvent dto) {
        MotionSensorAvro payload = MotionSensorAvro.newBuilder()
                .setLinkQuality(dto.getLinkQuality())
                .setMotion(dto.getMotion())
                .setVoltage(dto.getVoltage())
                .build();

        return SensorEventAvro.newBuilder()
                .setId(dto.getId())
                .setHubId(dto.getHubId())
                .setTimestamp(dto.getTimestamp())
                .setPayload(payload)
                .build();
    }
}
