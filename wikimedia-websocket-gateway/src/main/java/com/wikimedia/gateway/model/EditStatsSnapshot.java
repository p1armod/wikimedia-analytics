package com.wikimedia.gateway.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;


@Entity
@Table(name = "edit_stats_snapshot")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EditStatsSnapshot {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "window_start")
    private Instant windowStart;

    @Column(name = "window_end")
    private Instant windowEnd;

    @Column(name = "total_edits")
    private long totalEdits;

    @Column(name = "unique_users")
    private long uniqueUsers;

    @Column(name = "edits_per_minute")
    private double editsPerMinute;

    @Column(name = "avg_edit_size")
    private double avgEditSize;

    @Column(name = "recorded_at")
    private Instant recordedAt;
}
