package com.wikimedia.gateway.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;


@Entity
@Table(name = "bot_ratio_snapshot")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BotRatioSnapshot {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "bot_edits")
    private long botEdits;

    @Column(name = "human_edits")
    private long humanEdits;

    @Column(name = "bot_percentage")
    private double botPercentage;

    @Column(name = "recorded_at")
    private Instant recordedAt;
}
