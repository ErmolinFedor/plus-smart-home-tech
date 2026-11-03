package ru.yandex.practicum.collector.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

@Getter
@Setter
@ToString
public class ScenarioRemovedEvent implements HubEvent {
    @NotBlank
    private String hubId;
    private Instant timestamp = Instant.now();
    @NotBlank
    @Size(min = 3)
    private String name;
    @NotNull
    private HubEventType type;
}
