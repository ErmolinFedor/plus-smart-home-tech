package ru.yandex.practicum.collector.mapper;

import ru.yandex.practicum.collector.dto.HubEvent;
import ru.yandex.practicum.collector.dto.HubEventType;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

public interface HubEventMapper<T extends HubEvent> {
    HubEventType getSupportedType();

    HubEventAvro map(T event);
}

