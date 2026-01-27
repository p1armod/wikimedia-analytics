package com.wikimedia.streams.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertEvent {

    private String alertId;       // UUID
    private String wiki;          // Which wiki had the spike
    private String severity;      // "WARNING" or "CRITICAL"
    private long editCount;       // Current edit count in the window
    private double baseline;      // Rolling average baseline
    private long detectedAt;      // Timestamp of detection
    private String message;       // Human-readable alert message
}
