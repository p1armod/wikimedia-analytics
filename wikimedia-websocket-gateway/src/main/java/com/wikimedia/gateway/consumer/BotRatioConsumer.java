package com.wikimedia.gateway.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikimedia.gateway.model.BotRatioStat;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import com.wikimedia.gateway.model.BotRatioSnapshot;
import com.wikimedia.gateway.repository.BotRatioSnapshotRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;


@Slf4j
@Component
public class BotRatioConsumer {

    private final SimpMessagingTemplate messagingTemplate;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    private final BotRatioSnapshotRepository snapshotRepository;

    private static final String REDIS_KEY = "wikimedia:latest:bot-ratio";
    private static final String STOMP_DESTINATION = "/topic/bot-ratio";

    private volatile BotRatioStat latestBotRatio;

    public BotRatioConsumer(SimpMessagingTemplate messagingTemplate,
                            RedisTemplate<String, Object> redisTemplate,
                            ObjectMapper objectMapper,
                            BotRatioSnapshotRepository snapshotRepository) {
        this.messagingTemplate = messagingTemplate;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.snapshotRepository = snapshotRepository;
    }

    @KafkaListener(topics = "wikimedia.bot-ratio", groupId = "wikimedia-gateway")
    public void consume(String message) {
        try {
            BotRatioStat botRatio = objectMapper.readValue(message, BotRatioStat.class);
            this.latestBotRatio = botRatio;

            messagingTemplate.convertAndSend(STOMP_DESTINATION, botRatio);

            redisTemplate.opsForValue().set(REDIS_KEY, botRatio, Duration.ofMinutes(5));

            log.debug("Bot ratio broadcast: {}% bot ({} bot, {} human)",
                    botRatio.getBotPercentage(), botRatio.getBotEdits(), botRatio.getHumanEdits());

        } catch (Exception e) {
            log.error("Failed to process bot-ratio message: {}", e.getMessage());
        }
    }

    
    @Scheduled(fixedRate = 300000) // 5 minutes in milliseconds
    public void persistSnapshot() {
        if (latestBotRatio == null) {
            return;
        }

        try {
            BotRatioSnapshot snapshot = BotRatioSnapshot.builder()
                    .botEdits(latestBotRatio.getBotEdits())
                    .humanEdits(latestBotRatio.getHumanEdits())
                    .botPercentage(latestBotRatio.getBotPercentage())
                    .recordedAt(Instant.now())
                    .build();

            snapshotRepository.save(snapshot);
            log.info("Persisted bot ratio snapshot: {}% bot at {}", snapshot.getBotPercentage(), snapshot.getRecordedAt());

        } catch (Exception e) {
            log.error("Failed to persist bot ratio snapshot: {}", e.getMessage());
        }
    }

    
    @Transactional
    @Scheduled(fixedRate = 3600000) // 1 hour in milliseconds
    public void cleanupOldSnapshots() {
        try {
            Instant expiryDate = Instant.now().minus(24, ChronoUnit.HOURS);
            snapshotRepository.deleteByRecordedAtBefore(expiryDate);
            log.info("Cleaned up bot ratio snapshots older than {}", expiryDate);
        } catch (Exception e) {
            log.error("Failed to clean up old bot ratio snapshots: {}", e.getMessage());
        }
    }
}
