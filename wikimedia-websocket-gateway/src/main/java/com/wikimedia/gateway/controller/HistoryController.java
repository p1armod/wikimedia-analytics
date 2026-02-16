package com.wikimedia.gateway.controller;

import com.wikimedia.gateway.model.EditStats;
import com.wikimedia.gateway.model.EditStatsSnapshot;
import com.wikimedia.gateway.model.BotRatioStat;
import com.wikimedia.gateway.model.BotRatioSnapshot;
import com.wikimedia.gateway.repository.EditStatsSnapshotRepository;
import com.wikimedia.gateway.repository.BotRatioSnapshotRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


@Slf4j
// Expose REST endpoints for client access
@RestController
@RequestMapping("/api")
public class HistoryController {

    private final EditStatsSnapshotRepository snapshotRepository;
    private final BotRatioSnapshotRepository botRatioSnapshotRepository;
    private final RedisTemplate<String, Object> redisTemplate;

    public HistoryController(EditStatsSnapshotRepository snapshotRepository,
                             BotRatioSnapshotRepository botRatioSnapshotRepository,
                             RedisTemplate<String, Object> redisTemplate) {
        this.snapshotRepository = snapshotRepository;
        this.botRatioSnapshotRepository = botRatioSnapshotRepository;
        this.redisTemplate = redisTemplate;
    }

    
    @GetMapping("/history/stats")
    public ResponseEntity<List<EditStats>> getHistoricalStats() {
        List<EditStatsSnapshot> snapshots = snapshotRepository.findTop20ByOrderByRecordedAtDesc();

        Collections.reverse(snapshots);

        List<EditStats> result = snapshots.stream()
                .map(s -> EditStats.builder()
                        .windowStart(s.getWindowStart() != null ? s.getWindowStart().toEpochMilli() : s.getRecordedAt().toEpochMilli())
                        .windowEnd(s.getWindowEnd() != null ? s.getWindowEnd().toEpochMilli() : s.getRecordedAt().toEpochMilli())
                        .totalEdits(s.getTotalEdits())
                        .uniqueUsers(s.getUniqueUsers())
                        .editsPerMinute(s.getEditsPerMinute())
                        .avgEditSize(s.getAvgEditSize())
                        .build())
                .collect(Collectors.toList());

        log.info("Returning {} historical stats snapshots", result.size());
        return ResponseEntity.ok(result);
    }

    
    @GetMapping("/history/bot-ratio")
    public ResponseEntity<List<BotRatioStat>> getHistoricalBotRatio() {
        List<BotRatioSnapshot> snapshots = botRatioSnapshotRepository.findTop20ByOrderByRecordedAtDesc();

        Collections.reverse(snapshots);

        List<BotRatioStat> result = snapshots.stream()
                .map(s -> BotRatioStat.builder()
                        .botEdits(s.getBotEdits())
                        .humanEdits(s.getHumanEdits())
                        .botPercentage(s.getBotPercentage())
                        .windowStart(s.getRecordedAt().toEpochMilli())
                        .build())
                .collect(Collectors.toList());

        log.info("Returning {} historical bot ratio snapshots", result.size());
        return ResponseEntity.ok(result);
    }

    
    @GetMapping("/history/top-wikis")
    public ResponseEntity<Object> getLatestTopWikis() {
        Object topWikis = redisTemplate.opsForValue().get("wikimedia:latest:top-wikis");
        if (topWikis == null) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(topWikis);
    }

    
    @GetMapping("/cache/stats")
    public ResponseEntity<Object> getCachedStats() {
        Object stats = redisTemplate.opsForValue().get("wikimedia:latest:stats");
        if (stats == null) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(stats);
    }

    
    @GetMapping("/cache/bot-ratio")
    public ResponseEntity<Object> getCachedBotRatio() {
        Object botRatio = redisTemplate.opsForValue().get("wikimedia:latest:bot-ratio");
        if (botRatio == null) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(botRatio);
    }
}
