package ru.yandex.practicum.collector.mapper.impl;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.dto.HubEvent;
import ru.yandex.practicum.collector.dto.HubEventType;
import ru.yandex.practicum.collector.dto.ScenarioRemovedEvent;
import ru.yandex.practicum.collector.mapper.HubEventMapper;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

@Component
public class ScenarioRemovedHubEventMapper implements HubEventMapper<ScenarioRemovedEvent> {
    @Override
    public HubEventType getSupportedType() {
        return HubEventType.SCENARIO_REMOVED;
    }

    @Override
    public HubEventAvro map(ScenarioRemovedEvent dto) {
        ScenarioRemovedEventAvro payload = ScenarioRemovedEventAvro.newBuilder()
                .setName(dto.getName())
                .build();

        return HubEventAvro.newBuilder()
                .setHubId(dto.getHubId())
                .setTimestamp(dto.getTimestamp())
                .setPayload(payload)
                .build();
    }
}
