package ru.yandex.practicum.collector.mapper.impl;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.collector.dto.HubEventType;
import ru.yandex.practicum.collector.dto.ScenarioAddedEvent;
import ru.yandex.practicum.collector.mapper.HubEventMapper;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Component
public class ScenarioAddedHubEventMapper implements HubEventMapper<ScenarioAddedEvent> {
    @Override
    public HubEventType getSupportedType() {
        return HubEventType.SCENARIO_ADDED;
    }

    @Override
    public HubEventAvro map(ScenarioAddedEvent dto) {

        var avroConditions = dto.getConditions().stream().map(c ->
                ScenarioConditionAvro.newBuilder()
                        .setSensorId(c.getSensorId())
                        .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                        .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()))
                        .setValue(c.getValue())
                        .build()
        ).toList();

        var avroActions = dto.getActions().stream().map(a ->
                DeviceActionAvro.newBuilder()
                        .setSensorId(a.getSensorId())
                        .setType(ActionTypeAvro.valueOf(a.getType().name()))
                        .setValue(a.getValue())
                        .build()
        ).toList();

        ScenarioAddedEventAvro payload = ScenarioAddedEventAvro.newBuilder()
                .setName(dto.getName())
                .setConditions(avroConditions)
                .setActions(avroActions)
                .build();

        return HubEventAvro.newBuilder()
                .setHubId(dto.getHubId())
                .setTimestamp(dto.getTimestamp())
                .setPayload(payload)
                .build();
    }
}
