package ru.yandex.practicum.collector.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

@Getter
@Setter
@ToString
public class DeviceAddedEvent implements HubEvent {
    @NotBlank
    private String hubId;
    private Instant timestamp = Instant.now();
    @NotBlank
    private String id;
    @NotNull
    private DeviceType deviceType;
    @NotNull
    private HubEventType type;
}
