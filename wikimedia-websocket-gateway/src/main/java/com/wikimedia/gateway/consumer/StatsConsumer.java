package com.wikimedia.gateway.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikimedia.gateway.model.EditStats;
import com.wikimedia.gateway.model.EditStatsSnapshot;
import com.wikimedia.gateway.repository.EditStatsSnapshotRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.springframework.transaction.annotation.Transactional;


@Slf4j
@Component
public class StatsConsumer {

    private final SimpMessagingTemplate messagingTemplate;
    private final RedisTemplate<String, Object> redisTemplate;
    private final EditStatsSnapshotRepository snapshotRepository;
    private final ObjectMapper objectMapper;

    private static final String REDIS_KEY = "wikimedia:latest:stats";
    private static final String STOMP_DESTINATION = "/topic/stats";

    private volatile EditStats latestStats;

    public StatsConsumer(SimpMessagingTemplate messagingTemplate,
                         RedisTemplate<String, Object> redisTemplate,
                         EditStatsSnapshotRepository snapshotRepository,
                         ObjectMapper objectMapper) {
        this.messagingTemplate = messagingTemplate;
        this.redisTemplate = redisTemplate;
        this.snapshotRepository = snapshotRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "wikimedia.stats", groupId = "wikimedia-gateway")
    public void consume(String message) {
        try {
            EditStats stats = objectMapper.readValue(message, EditStats.class);
            this.latestStats = stats;

            messagingTemplate.convertAndSend(STOMP_DESTINATION, stats);

            redisTemplate.opsForValue().set(REDIS_KEY, stats, Duration.ofMinutes(5));

            log.debug("Stats broadcast: {} edits/min, {} unique users",
                    stats.getEditsPerMinute(), stats.getUniqueUsers());

        } catch (Exception e) {
            log.error("Failed to process stats message: {}", e.getMessage());
        }
    }

    
    @Scheduled(fixedRate = 60000) // 1 minute in milliseconds
    public void persistSnapshot() {
        if (latestStats == null) {
            return;
        }

        try {
            EditStatsSnapshot snapshot = EditStatsSnapshot.builder()
                    .windowStart(Instant.ofEpochMilli(latestStats.getWindowStart()))
                    .windowEnd(Instant.ofEpochMilli(latestStats.getWindowEnd()))
                    .totalEdits(latestStats.getTotalEdits())
                    .uniqueUsers(latestStats.getUniqueUsers())
                    .editsPerMinute(latestStats.getEditsPerMinute())
                    .avgEditSize(latestStats.getAvgEditSize())
                    .recordedAt(Instant.now())
                    .build();

            snapshotRepository.save(snapshot);
            log.info("Persisted stats snapshot: {} edits at {}", snapshot.getTotalEdits(), snapshot.getRecordedAt());

        } catch (Exception e) {
            log.error("Failed to persist stats snapshot: {}", e.getMessage());
        }
    }

    
    @Transactional
    @Scheduled(fixedRate = 3600000) // 1 hour in milliseconds
    public void cleanupOldSnapshots() {
        try {
            Instant expiryDate = Instant.now().minus(24, ChronoUnit.HOURS);
            snapshotRepository.deleteByRecordedAtBefore(expiryDate);
            log.info("Cleaned up edit stats snapshots older than {}", expiryDate);
        } catch (Exception e) {
            log.error("Failed to clean up old stats snapshots: {}", e.getMessage());
        }
    }
}
