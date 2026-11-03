package ru.yandex.practicum.collector.mapper.impl;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.dto.SensorEvent;
import ru.yandex.practicum.collector.dto.SensorEventType;
import ru.yandex.practicum.collector.dto.SwitchSensorEvent;
import ru.yandex.practicum.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;

@Component
public class SwitchSensorEventMapper implements SensorEventMapper<SwitchSensorEvent> {
    @Override
    public SensorEventType getSupportedType() {
        return SensorEventType.SWITCH_SENSOR_EVENT;
    }

    @Override
    public SensorEventAvro map(SwitchSensorEvent dto) {
        SwitchSensorAvro payload = SwitchSensorAvro.newBuilder()
                .setState(dto.getState())
                .build();

        return SensorEventAvro.newBuilder()
                .setId(dto.getId())
                .setHubId(dto.getHubId())
                .setTimestamp(dto.getTimestamp())
                .setPayload(payload)
                .build();
    }
}
