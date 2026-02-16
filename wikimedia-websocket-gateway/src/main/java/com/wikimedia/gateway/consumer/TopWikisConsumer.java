package com.wikimedia.gateway.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikimedia.gateway.model.WikiStat;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;


@Slf4j
@Component
public class TopWikisConsumer {

    private final SimpMessagingTemplate messagingTemplate;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    private static final String REDIS_KEY = "wikimedia:latest:top-wikis";
    private static final String STOMP_DESTINATION = "/topic/top-wikis";

    public TopWikisConsumer(SimpMessagingTemplate messagingTemplate,
                            RedisTemplate<String, Object> redisTemplate,
                            ObjectMapper objectMapper) {
        this.messagingTemplate = messagingTemplate;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "wikimedia.top-wikis", groupId = "wikimedia-gateway")
    public void consume(String message) {
        try {
            List<WikiStat> topWikis = objectMapper.readValue(message,
                    new TypeReference<List<WikiStat>>() {});

            messagingTemplate.convertAndSend(STOMP_DESTINATION, topWikis);

            redisTemplate.opsForValue().set(REDIS_KEY, topWikis, Duration.ofMinutes(5));

            log.debug("Top wikis broadcast: {} wikis, top={}", topWikis.size(),
                    topWikis.isEmpty() ? "none" : topWikis.get(0).getWiki());

        } catch (Exception e) {
            log.error("Failed to process top-wikis message: {}", e.getMessage());
        }
    }
}
