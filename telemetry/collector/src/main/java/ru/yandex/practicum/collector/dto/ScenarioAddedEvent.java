package ru.yandex.practicum.collector.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;
import java.util.List;

@Getter
@Setter
@ToString
public class ScenarioAddedEvent implements HubEvent {
    @NotBlank
    private String hubId;
    private Instant timestamp = Instant.now();
    @NotBlank
    @Size(min = 3)
    private String name;
    @NotNull
    private List<ScenarioCondition> conditions;
    @NotNull
    private List<DeviceAction> actions;
    @NotNull
    private HubEventType type;
}
