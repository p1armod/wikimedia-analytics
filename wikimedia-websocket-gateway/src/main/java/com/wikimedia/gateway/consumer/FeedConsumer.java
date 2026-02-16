package com.wikimedia.gateway.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;


@Slf4j
@Component
public class FeedConsumer {

    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper objectMapper;

    private static final String STOMP_DESTINATION = "/topic/feed";
    private static final long MIN_INTERVAL_MS = 200; // 5 events/second max

    private final AtomicLong lastSentTime = new AtomicLong(0);

    public FeedConsumer(SimpMessagingTemplate messagingTemplate,
                        ObjectMapper objectMapper) {
        this.messagingTemplate = messagingTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "wikimedia.recentchange", groupId = "wikimedia-gateway-feed")
    public void consume(String message) {
        try {

            long now = System.currentTimeMillis();
            long lastSent = lastSentTime.get();

            if (now - lastSent < MIN_INTERVAL_MS) {
                return; // Skip this event — too soon
            }

            if (lastSentTime.compareAndSet(lastSent, now)) {

                Object event = objectMapper.readValue(message, Object.class);
                messagingTemplate.convertAndSend(STOMP_DESTINATION, event);
            }

        } catch (Exception e) {
            log.debug("Failed to process feed event: {}", e.getMessage());
        }
    }
}
