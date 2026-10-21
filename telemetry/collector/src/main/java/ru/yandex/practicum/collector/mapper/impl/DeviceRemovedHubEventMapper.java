package ru.yandex.practicum.collector.mapper.impl;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.dto.DeviceRemovedEvent;
import ru.yandex.practicum.collector.dto.HubEvent;
import ru.yandex.practicum.collector.dto.HubEventType;
import ru.yandex.practicum.collector.mapper.HubEventMapper;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Component
public class DeviceRemovedHubEventMapper implements HubEventMapper<DeviceRemovedEvent> {
    @Override
    public HubEventType getSupportedType() {
        return HubEventType.DEVICE_REMOVED;
    }

    @Override
    public HubEventAvro map(DeviceRemovedEvent dto) {
        DeviceRemovedEventAvro payload = DeviceRemovedEventAvro.newBuilder()
                .setId(dto.getId())
                .build();

        return HubEventAvro.newBuilder()
                .setHubId(dto.getHubId())
                .setTimestamp(dto.getTimestamp())
                .setPayload(payload)
                .build();
    }
}
