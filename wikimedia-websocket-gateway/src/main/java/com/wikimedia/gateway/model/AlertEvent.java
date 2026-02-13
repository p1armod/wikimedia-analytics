package com.wikimedia.gateway.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertEvent {

    private String alertId;
    private String wiki;
    private String severity;
    private long editCount;
    private double baseline;
    private long detectedAt;
    private String message;
}
