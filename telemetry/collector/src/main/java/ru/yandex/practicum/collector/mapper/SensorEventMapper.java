package ru.yandex.practicum.collector.mapper;

import ru.yandex.practicum.collector.dto.SensorEvent;
import ru.yandex.practicum.collector.dto.SensorEventType;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

public interface SensorEventMapper<T extends SensorEvent> {
    SensorEventType getSupportedType();

    SensorEventAvro map(T event);
}
